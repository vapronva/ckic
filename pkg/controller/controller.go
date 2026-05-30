package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"git.horse/vapronva/ckic/pkg/aggregator"
	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/constants"
	"git.horse/vapronva/ckic/pkg/utils"
)

type Config struct {
	NodeLabel                    string
	ConfigMapName                string
	Namespace                    string
	BootstrapDefaultConfig       bool
	CommunicationMethod          caddy.CommunicationMethod
	CaddyImage                   string
	ImagePullPolicy              string
	PrePullImage                 bool
	LoadBalancerMode             caddy.LoadBalancerMode
	EnvSecretName                string
	EnvSecretKeys                []string
	DataVolumePVC                string
	ConfigVolumePVC              string
	ExternalEndpoints            utils.ExternalEndpointsMap
	UseHostNetwork               bool
	CaddyAdminOriginKey          string
	HTTPHostPort                 int
	HTTPSHostPort                int
	ExternalEnable               bool
	ExternalLabel                string
	ExternalNsMode               string
	ExternalAllowNamespaces      string
	ExternalDenyNamespaces       string
	ExternalPublishAggregated    bool
	ExternalAggregatedConfigName string
	ConfigResyncInterval         time.Duration
}

const (
	configReconcileKey = "\x00config-reconcile"
	workerCount        = 4
	reconcileTimeout   = 2 * time.Minute
	informerResync     = 10 * time.Minute
	maxPushFailures    = 5
	nsModeAll          = "all"
	nsModeAllow        = "allow"
	nsModeDeny         = "deny"
)

type pushRecord struct {
	digest  string
	podName string
}

type Controller struct {
	clientset         kubernetes.Interface
	config            Config
	deployOpts        caddy.DeployOptions
	comm              caddy.CommunicationMethod
	adminConfig       *caddy.AdminAPIConfig
	aggregator        *aggregator.Aggregator
	nodeSelector      labels.Selector
	allowedNamespaces map[string]struct{}
	deniedNamespaces  map[string]struct{}
	nodeFactory       informers.SharedInformerFactory
	nsFactory         informers.SharedInformerFactory
	extFactory        informers.SharedInformerFactory
	nodeLister        corev1listers.NodeLister
	deployLister      appsv1listers.DeploymentLister
	cacheSyncs        []cache.InformerSynced
	queue             workqueue.TypedRateLimitingInterface[string]
	pushMu            sync.Mutex
	pushState         map[string]pushRecord
	deployFn          func(context.Context, caddy.DeployOptions, string, []string) (*caddy.Instance, error)
	pushFn            func(context.Context, *caddy.Instance, string) error
}

func NewController(
	clientset kubernetes.Interface,
	config Config,
) (*Controller, error) {
	normalized, selector, err := utils.NormalizeNodeLabelSelector(config.NodeLabel)
	if err != nil {
		return nil, err
	}
	config.NodeLabel = normalized
	if config.ExternalEnable {
		switch config.ExternalNsMode {
		case nsModeAll, nsModeAllow, nsModeDeny:
		default:
			return nil, fmt.Errorf(
				"invalid external namespace mode %q (want all, allow, or deny)",
				config.ExternalNsMode,
			)
		}
	}
	c := &Controller{
		clientset: clientset,
		config:    config,
		deployOpts: caddy.DeployOptions{
			Clientset:        clientset,
			Namespace:        config.Namespace,
			CaddyImage:       config.CaddyImage,
			ImagePullPolicy:  corev1.PullPolicy(config.ImagePullPolicy),
			PrePullImage:     config.PrePullImage,
			LoadBalancerMode: config.LoadBalancerMode,
			EnvSecretName:    config.EnvSecretName,
			EnvSecretKeys:    config.EnvSecretKeys,
			DataVolumePVC:    config.DataVolumePVC,
			ConfigVolumePVC:  config.ConfigVolumePVC,
			ConfigMapName:    config.ConfigMapName,
			UseHostNetwork:   config.UseHostNetwork,
			HTTPHostPort:     config.HTTPHostPort,
			HTTPSHostPort:    config.HTTPSHostPort,
		},
		comm:              config.CommunicationMethod,
		adminConfig:       caddy.NewAdminAPIConfig(config.CaddyAdminOriginKey),
		nodeSelector:      selector,
		allowedNamespaces: parseNamespaceSet(config.ExternalAllowNamespaces),
		deniedNamespaces:  parseNamespaceSet(config.ExternalDenyNamespaces),
		pushState:         make(map[string]pushRecord),
		deployFn:          caddy.EnsureCaddy,
	}
	c.pushFn = func(ctx context.Context, instance *caddy.Instance, merged string) error {
		return instance.UpdateConfig(ctx, merged, c.comm, c.adminConfig)
	}
	c.queue = workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[string](),
	)
	c.aggregator = aggregator.New(
		clientset,
		config.Namespace,
		config.ExternalPublishAggregated,
		config.ExternalAggregatedConfigName,
		func() { c.queue.Add(configReconcileKey) },
	)
	c.setupInformers()
	return c, nil
}

func parseNamespaceSet(csv string) map[string]struct{} {
	set := make(map[string]struct{})
	for ns := range strings.SplitSeq(csv, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			set[ns] = struct{}{}
		}
	}
	return set
}

func (c *Controller) setupInformers() {
	c.nodeFactory = informers.NewSharedInformerFactory(c.clientset, informerResync)
	c.nsFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync, informers.WithNamespace(c.config.Namespace),
	)
	nodeInformer := c.nodeFactory.Core().V1().Nodes()
	cmInformer := c.nsFactory.Core().V1().ConfigMaps()
	deployInformer := c.nsFactory.Apps().V1().Deployments()
	c.nodeLister = nodeInformer.Lister()
	c.deployLister = deployInformer.Lister()
	c.addNodeHandler(nodeInformer.Informer())
	c.addBaseConfigMapHandler(cmInformer.Informer())
	c.addDeploymentHandler(deployInformer.Informer())
	c.cacheSyncs = []cache.InformerSynced{
		nodeInformer.Informer().HasSynced,
		cmInformer.Informer().HasSynced,
		deployInformer.Informer().HasSynced,
	}
	if !c.config.ExternalEnable {
		return
	}
	label := c.config.ExternalLabel
	c.extFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = label
		}),
	)
	extInformer := c.extFactory.Core().V1().ConfigMaps()
	c.addExternalConfigMapHandler(extInformer.Informer())
	c.cacheSyncs = append(c.cacheSyncs, extInformer.Informer().HasSynced)
}

func (c *Controller) addNodeHandler(informer cache.SharedIndexInformer) {
	enqueueIfManaged := func(node *corev1.Node) {
		if c.nodeSelector.Matches(labels.Set(node.Labels)) {
			c.queue.Add(node.Name)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if node, ok := obj.(*corev1.Node); ok {
				enqueueIfManaged(node)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldNode, ok1 := oldObj.(*corev1.Node)
			newNode, ok2 := newObj.(*corev1.Node)
			if !ok2 {
				return
			}
			if (ok1 && c.nodeSelector.Matches(labels.Set(oldNode.Labels))) ||
				c.nodeSelector.Matches(labels.Set(newNode.Labels)) {
				c.queue.Add(newNode.Name)
			}
		},
		DeleteFunc: func(obj any) {
			if node, ok := tombstone[*corev1.Node](obj); ok {
				c.queue.Add(node.Name)
			}
		},
	})
}

func (c *Controller) addBaseConfigMapHandler(informer cache.SharedIndexInformer) {
	handle := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || cm.Name != c.config.ConfigMapName {
			return
		}
		if data, exists := cm.Data[constants.CaddyfileKey]; exists {
			c.aggregator.UpdateBase(data)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, newObj any) { handle(newObj) },
		DeleteFunc: func(obj any) {
			if cm, ok := tombstone[*corev1.ConfigMap](obj); ok &&
				cm.Name == c.config.ConfigMapName {
				log.Warn().
					Str("configmap", cm.Name).
					Msg("Base ConfigMap deleted; keeping last known configuration")
			}
		},
	})
}

func (c *Controller) addExternalConfigMapHandler(informer cache.SharedIndexInformer) {
	upsert := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || !c.namespaceAllowed(cm.Namespace) {
			return
		}
		source := cm.Namespace + "/" + cm.Name
		if fragment, exists := cm.Data[constants.CaddyfileKey]; exists {
			c.aggregator.SetExternal(source, fragment)
		} else {
			c.aggregator.RemoveExternal(source)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    upsert,
		UpdateFunc: func(_, newObj any) { upsert(newObj) },
		DeleteFunc: func(obj any) {
			if cm, ok := tombstone[*corev1.ConfigMap](obj); ok {
				c.aggregator.RemoveExternal(cm.Namespace + "/" + cm.Name)
			}
		},
	})
}

func (c *Controller) addDeploymentHandler(informer cache.SharedIndexInformer) {
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj any) {
			dep, ok := tombstone[*appsv1.Deployment](obj)
			if !ok ||
				dep.Labels[constants.LabelCaddyManaged] != constants.LabelManagedValue {
				return
			}
			if node := dep.Labels[constants.LabelInstance]; node != "" {
				c.queue.Add(node)
			}
		},
	})
}

func (c *Controller) namespaceAllowed(namespace string) bool {
	if namespace == c.config.Namespace {
		return false
	}
	switch c.config.ExternalNsMode {
	case nsModeAll:
		return true
	case nsModeAllow:
		_, ok := c.allowedNamespaces[namespace]
		return ok
	case nsModeDeny:
		_, denied := c.deniedNamespaces[namespace]
		return !denied
	default:
		return false
	}
}

func (c *Controller) Run(ctx context.Context) error {
	logger := log.With().Str("component", "controller").Logger()
	if err := c.bootstrapBaseConfig(ctx, logger); err != nil {
		return err
	}
	stopCh := ctx.Done()
	c.nodeFactory.Start(stopCh)
	c.nsFactory.Start(stopCh)
	if c.extFactory != nil {
		c.extFactory.Start(stopCh)
	}
	logger.Info().Msg("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(stopCh, c.cacheSyncs...) {
		return errors.New("failed to sync informer caches")
	}
	logger.Info().Msg("Caches synced; starting reconcile workers")
	c.sweepLegacyPodDisruptionBudgets(ctx, logger)
	c.enqueueExistingDeployments(logger)
	c.queue.Add(configReconcileKey)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			for c.processNextItem(ctx) {
			}
		})
	}
	if c.config.ConfigResyncInterval > 0 {
		logger.Info().
			Dur("interval", c.config.ConfigResyncInterval).
			Msg("Periodic config re-push enabled")
		wg.Go(func() { c.runConfigResync(stopCh) })
	}
	<-stopCh
	logger.Info().Msg("Controller shutting down; draining workqueue")
	c.queue.ShutDown()
	wg.Wait()
	logger.Info().Msg("Controller stopped")
	return nil
}

func (c *Controller) runConfigResync(stopCh <-chan struct{}) {
	ticker := time.NewTicker(c.config.ConfigResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			c.forceConfigResync()
		}
	}
}

func (c *Controller) sweepLegacyPodDisruptionBudgets(
	ctx context.Context,
	logger zerolog.Logger,
) {
	deployments, err := c.deployLister.Deployments(c.config.Namespace).List(
		labels.SelectorFromSet(labels.Set{
			constants.LabelCaddyManaged: constants.LabelManagedValue,
		}),
	)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to list deployments for legacy PDB cleanup")
		return
	}
	for _, dep := range deployments {
		caddy.DeleteLegacyPodDisruptionBudget(
			ctx,
			c.clientset,
			&caddy.Instance{Namespace: c.config.Namespace, DeploymentName: dep.Name},
			logger,
		)
	}
}

func (c *Controller) enqueueExistingDeployments(logger zerolog.Logger) {
	deployments, err := c.deployLister.Deployments(c.config.Namespace).List(
		labels.SelectorFromSet(labels.Set{
			constants.LabelCaddyManaged: constants.LabelManagedValue,
		}),
	)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to list managed deployments for adoption")
		return
	}
	for _, dep := range deployments {
		if node := dep.Labels[constants.LabelInstance]; node != "" {
			c.queue.Add(node)
		}
	}
}

func (c *Controller) bootstrapBaseConfig(
	ctx context.Context,
	logger zerolog.Logger,
) error {
	_, err := c.clientset.CoreV1().
		ConfigMaps(c.config.Namespace).
		Get(ctx, c.config.ConfigMapName, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to read base ConfigMap: %w", err)
	}
	if !c.config.BootstrapDefaultConfig {
		logger.Warn().
			Str("configmap", c.config.ConfigMapName).
			Msg("Base ConfigMap not found and bootstrap disabled; waiting for it to be created")
		return nil
	}
	apply := corev1ac.ConfigMap(c.config.ConfigMapName, c.config.Namespace).
		WithData(map[string]string{
			constants.CaddyfileKey: ":80 {\n    respond \"Hello, world!\"\n}\n",
		})
	if _, err = c.clientset.CoreV1().ConfigMaps(c.config.Namespace).Apply(
		ctx, apply, metav1.ApplyOptions{FieldManager: "ckic", Force: true},
	); err != nil {
		return fmt.Errorf("failed to bootstrap default ConfigMap: %w", err)
	}
	logger.Info().Msg("Bootstrapped default base ConfigMap")
	return nil
}

func tombstone[T any](obj any) (T, bool) {
	if typed, ok := obj.(T); ok {
		return typed, true
	}
	if deleted, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if typed, typedOK := deleted.Obj.(T); typedOK {
			return typed, true
		}
	}
	var zero T
	return zero, false
}
