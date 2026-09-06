package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	Deploy                       caddy.DeployOptions
	NodeLabel                    string
	ConfigMapName                string
	Namespace                    string
	BootstrapDefaultConfig       bool
	ConfigResyncInterval         time.Duration
	ExternalEndpoints            utils.ExternalEndpointsMap
	ExternalEnable               bool
	ExternalLabel                string
	ExternalAllowNamespaces      []string
	ExternalDenyNamespaces       []string
	ExternalPublishAggregated    bool
	ExternalAggregatedConfigName string
}

const (
	configReconcileKey        = "\x00config-reconcile"
	workerCount               = 4
	reconcileTimeout          = 5 * time.Minute
	informerResync            = 10 * time.Minute
	podStartupRequeueInterval = 5 * time.Second
)

type pushRecord struct {
	digest      string
	containerID string
}

type Controller struct {
	clientset         kubernetes.Interface
	config            Config
	deployOpts        caddy.DeployOptions
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

func NewController(clientset kubernetes.Interface, config Config) (*Controller, error) {
	normalized, selector, err := utils.NormalizeNodeLabelSelector(config.NodeLabel)
	if err != nil {
		return nil, err
	}
	config.NodeLabel = normalized
	if config.ConfigResyncInterval < 0 {
		return nil, errors.New("config resync interval cannot be negative")
	}
	envKeys := map[string]bool{
		constants.PodNameEnvVar:  true,
		constants.NodeNameEnvVar: true,
		constants.PodIPEnvVar:    true,
	}
	for _, key := range config.Deploy.EnvSecretKeys {
		if envKeys[key] {
			return nil, fmt.Errorf("duplicate or reserved environment key %q", key)
		}
		envKeys[key] = true
	}
	if config.ExternalEnable {
		externalLabel, _, labelErr := utils.NormalizeNodeLabelSelector(config.ExternalLabel)
		if labelErr != nil {
			return nil, fmt.Errorf("invalid external label selector: %w", labelErr)
		}
		config.ExternalLabel = externalLabel
	}
	if config.ExternalPublishAggregated && config.ExternalAggregatedConfigName == "" {
		return nil, errors.New("external aggregated config name must be set when publishing the aggregated config")
	}
	if config.ExternalPublishAggregated && config.ExternalAggregatedConfigName == config.ConfigMapName {
		return nil, errors.New("external aggregated config name must differ from the base config-map name")
	}
	deployOpts := config.Deploy
	deployOpts.Clientset = clientset
	deployOpts.Namespace = config.Namespace
	deployOpts.ConfigMapName = bootConfigMapName(config)
	c := &Controller{
		clientset:         clientset,
		config:            config,
		deployOpts:        deployOpts,
		adminConfig:       caddy.NewAdminAPIConfig(config.Deploy.CaddyAdminOriginKey),
		nodeSelector:      selector,
		allowedNamespaces: namespaceSet(config.ExternalAllowNamespaces),
		deniedNamespaces:  namespaceSet(config.ExternalDenyNamespaces),
		pushState:         make(map[string]pushRecord),
		deployFn:          caddy.EnsureCaddy,
	}
	c.pushFn = func(ctx context.Context, instance *caddy.Instance, merged string) error {
		return instance.UpdateConfig(ctx, merged, c.adminConfig)
	}
	c.queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	c.aggregator = aggregator.New(
		clientset,
		config.Namespace,
		config.ExternalPublishAggregated,
		config.ExternalAggregatedConfigName,
		func() { c.queue.Add(configReconcileKey) },
	)
	if err := c.setupInformers(); err != nil {
		c.queue.ShutDown()
		return nil, err
	}
	return c, nil
}

func bootConfigMapName(config Config) string {
	if config.ExternalPublishAggregated {
		return config.ExternalAggregatedConfigName
	}
	return config.ConfigMapName
}

func namespaceSet(namespaces []string) map[string]struct{} {
	set := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		set[ns] = struct{}{}
	}
	return set
}

func (c *Controller) setupInformers() error {
	c.nodeFactory = informers.NewSharedInformerFactory(c.clientset, informerResync)
	c.nsFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync, informers.WithNamespace(c.config.Namespace),
	)
	nodeInformer := c.nodeFactory.Core().V1().Nodes()
	cmInformer := c.nsFactory.Core().V1().ConfigMaps()
	deployInformer := c.nsFactory.Apps().V1().Deployments()
	podInformer := c.nsFactory.Core().V1().Pods()
	serviceInformer := c.nsFactory.Core().V1().Services()
	c.nodeLister = nodeInformer.Lister()
	c.deployLister = deployInformer.Lister()
	c.addNodeHandler(nodeInformer.Informer())
	c.addDeploymentHandler(deployInformer.Informer())
	c.addPodHandler(podInformer.Informer())
	c.addServiceHandler(serviceInformer.Informer())
	c.cacheSyncs = []cache.InformerSynced{
		nodeInformer.Informer().HasSynced,
		cmInformer.Informer().HasSynced,
		deployInformer.Informer().HasSynced,
		podInformer.Informer().HasSynced,
		serviceInformer.Informer().HasSynced,
	}
	if err := c.addConfigMapHandler(cmInformer.Informer()); err != nil {
		return err
	}
	if !c.config.ExternalEnable {
		return nil
	}
	label := c.config.ExternalLabel
	c.extFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = label
		}),
	)
	return c.addExternalConfigMapHandler(c.extFactory.Core().V1().ConfigMaps().Informer())
}

func (c *Controller) enqueueNodeIfManaged(node *corev1.Node) {
	if c.nodeSelector.Matches(labels.Set(node.Labels)) {
		c.queue.Add(node.Name)
	}
}

func (c *Controller) addNodeHandler(informer cache.SharedIndexInformer) {
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if node, ok := obj.(*corev1.Node); ok {
				c.enqueueNodeIfManaged(node)
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
				c.enqueueNodeIfManaged(node)
			}
		},
	})
}

func (c *Controller) addConfigMapHandler(informer cache.SharedIndexInformer) error {
	handle := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok {
			return
		}
		if c.config.ExternalPublishAggregated && cm.Name == c.config.ExternalAggregatedConfigName {
			c.enqueueChangedMirror(cm)
			return
		}
		if cm.Name != c.config.ConfigMapName {
			return
		}
		data, exists := cm.Data[constants.CaddyfileKey]
		if !exists {
			log.Warn().Str("configmap", cm.Name).Msg("Base ConfigMap has no Caddyfile; ignoring update")
			return
		}
		c.aggregator.UpdateBase(data)
	}
	handler, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, newObj any) { handle(newObj) },
		DeleteFunc: func(obj any) {
			cm, ok := tombstone[*corev1.ConfigMap](obj)
			if !ok {
				return
			}
			if cm.Name == c.config.ConfigMapName {
				log.Warn().Str("configmap", cm.Name).Msg("Base ConfigMap deleted; keeping last known configuration")
			}
			if c.config.ExternalPublishAggregated && cm.Name == c.config.ExternalAggregatedConfigName {
				c.queue.Add(configReconcileKey)
			}
		},
	})
	if err != nil {
		return err
	}
	c.cacheSyncs = append(c.cacheSyncs, handler.HasSynced)
	return nil
}

func (c *Controller) enqueueChangedMirror(cm *corev1.ConfigMap) {
	merged, err := c.aggregator.CurrentMerged()
	if err != nil {
		return
	}
	if cm.Data[constants.CaddyfileKey] != merged {
		c.queue.Add(configReconcileKey)
		return
	}
	for key, value := range constants.AggregatedConfigLabels() {
		if cm.Labels[key] != value {
			c.queue.Add(configReconcileKey)
			return
		}
	}
}

func (c *Controller) addExternalConfigMapHandler(informer cache.SharedIndexInformer) error {
	upsert := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || !c.namespaceAllowed(cm.Namespace) {
			return
		}
		source := cm.Namespace + "/" + cm.Name
		fragment, exists := cm.Data[constants.CaddyfileKey]
		if !exists {
			c.aggregator.RemoveExternal(source)
			return
		}
		c.aggregator.SetExternal(source, fragment)
	}
	handler, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    upsert,
		UpdateFunc: func(_, newObj any) { upsert(newObj) },
		DeleteFunc: func(obj any) {
			if cm, ok := tombstone[*corev1.ConfigMap](obj); ok {
				c.aggregator.RemoveExternal(cm.Namespace + "/" + cm.Name)
			}
		},
	})
	if err != nil {
		return err
	}
	c.cacheSyncs = append(c.cacheSyncs, handler.HasSynced)
	return nil
}

func (c *Controller) addPodHandler(informer cache.SharedIndexInformer) {
	handle := func(obj any) {
		pod, ok := tombstone[*corev1.Pod](obj)
		if ok && pod.Spec.NodeName != "" &&
			pod.Labels[constants.LabelApp] == constants.LabelAppValue &&
			pod.Labels[constants.LabelCaddyManaged] == constants.LabelManagedValue {
			c.queue.Add(pod.Spec.NodeName)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, obj any) { handle(obj) },
		DeleteFunc: handle,
	})
}

func (c *Controller) addDeploymentHandler(informer cache.SharedIndexInformer) {
	handle := func(obj any) {
		if dep, ok := tombstone[*appsv1.Deployment](obj); ok {
			c.enqueueManagedObject(dep)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, obj any) { handle(obj) },
		DeleteFunc: handle,
	})
}

func (c *Controller) addServiceHandler(informer cache.SharedIndexInformer) {
	handle := func(obj any) {
		if service, ok := tombstone[*corev1.Service](obj); ok {
			c.enqueueManagedObject(service)
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, obj any) { handle(obj) },
		DeleteFunc: handle,
	})
}

func (c *Controller) enqueueManagedObject(obj metav1.Object) {
	if nodeName := managedNodeName(obj); nodeName != "" {
		c.queue.Add(nodeName)
	}
}

func managedNodeName(obj metav1.Object) string {
	objLabels := obj.GetLabels()
	if objLabels[constants.LabelCaddyManaged] != constants.LabelManagedValue {
		return ""
	}
	return objLabels[constants.LabelInstance]
}

func (c *Controller) namespaceAllowed(namespace string) bool {
	if namespace == c.config.Namespace {
		return false
	}
	if _, denied := c.deniedNamespaces[namespace]; denied {
		return false
	}
	if len(c.allowedNamespaces) == 0 {
		return true
	}
	_, allowed := c.allowedNamespaces[namespace]
	return allowed
}

func (c *Controller) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.queue.ShutDown()
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
		return fmt.Errorf("shutting down before informer caches synced: %w", ctx.Err())
	}
	logger.Info().Msg("Caches synced; starting reconcile workers")
	if err := c.aggregator.PublishMirror(ctx); err != nil {
		logger.Warn().Err(err).Msg("Initial mirror ConfigMap publish failed; reconciliation will retry")
	}
	c.enqueueExistingDeployments(logger)
	caddy.ReapPrePullPods(ctx, c.clientset, c.config.Namespace, logger)
	c.queue.Add(configReconcileKey)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			for c.processNextItem(ctx) {
			}
		})
	}
	if c.config.ConfigResyncInterval > 0 {
		logger.Info().Dur("interval", c.config.ConfigResyncInterval).Msg("Periodic config re-push enabled")
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

func (c *Controller) enqueueExistingDeployments(logger zerolog.Logger) {
	deployments, err := c.deployLister.Deployments(c.config.Namespace).List(labels.Everything())
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to list managed deployments for adoption")
		return
	}
	for _, dep := range deployments {
		c.enqueueManagedObject(dep)
	}
}

func (c *Controller) bootstrapBaseConfig(ctx context.Context, logger zerolog.Logger) error {
	configMaps := c.clientset.CoreV1().ConfigMaps(c.config.Namespace)
	_, err := configMaps.Get(ctx, c.config.ConfigMapName, metav1.GetOptions{})
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
	base := &corev1.ConfigMap{
		Name: c.config.ConfigMapName, Namespace: c.config.Namespace,
		Data: map[string]string{
			constants.CaddyfileKey: defaultBootstrapCaddyfile(c.config.Deploy.CaddyAdminOriginKey),
		},
	}
	_, err = configMaps.Create(ctx, base, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to bootstrap default ConfigMap: %w", err)
	}
	logger.Info().Msg("Bootstrapped default base ConfigMap")
	return nil
}

func defaultBootstrapCaddyfile(originKey string) string {
	admin := "\tadmin :2019\n"
	if originKey != "" {
		admin = fmt.Sprintf(
			"\tadmin :2019 {\n\t\torigins http://%s.caddy-admin-api.ckic.cmld.ru\n\t\tenforce_origin\n\t}\n",
			originKey,
		)
	}
	return fmt.Sprintf("{\n%s}\n\n:80 {\n\trespond \"Hello, world!\"\n}\n", admin)
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
