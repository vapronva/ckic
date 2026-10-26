package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
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
)

type Config struct {
	Deploy                  caddy.DeployOptions
	NodeLabel               string
	BaseConfigMapName       string
	BootConfigMapName       string
	Namespace               string
	ConfigResyncInterval    time.Duration
	ForceReload             bool
	ExternalEndpoints       map[string][]string
	ExternalEnable          bool
	ExternalLabel           string
	ExternalAllowNamespaces []string
	ExternalDenyNamespaces  []string
}

const (
	mirrorKey            = "mirror/publish"
	workerCount          = 4
	reconcileTimeout     = 5 * time.Minute
	informerResync       = 10 * time.Minute
	requeueInterval      = 5 * time.Second
	prePullRetryInterval = time.Minute
)

type pushRecord struct {
	digest      string
	containerID string
}

type Controller struct {
	clientset    kubernetes.Interface
	config       Config
	admin        *caddy.Admin
	aggregator   *aggregator.Aggregator
	nodeSelector labels.Selector
	nodeFactory  informers.SharedInformerFactory
	nsFactory    informers.SharedInformerFactory
	extFactory   informers.SharedInformerFactory
	nodeLister   corev1listers.NodeLister
	deployLister appsv1listers.DeploymentLister
	podLister    corev1listers.PodLister
	cacheSyncs   []cache.InformerSynced
	queue        workqueue.TypedRateLimitingInterface[string]
	rolloutMu    sync.Mutex
	pushMu       sync.Mutex
	pushState    map[string]pushRecord
	deployFn     func(context.Context, *corev1.Node) (caddy.Pod, error)
	pushFn       func(ctx context.Context, podIP, caddyfile string) error
}

func NewController(clientset kubernetes.Interface, config Config) (*Controller, error) {
	config, selector, err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	config.Deploy.Clientset = clientset
	config.Deploy.Namespace = config.Namespace
	config.Deploy.BootConfigMapName = config.BootConfigMapName
	admin := caddy.NewAdmin(config.Deploy.CaddyAdminOriginKey, config.ForceReload)
	c := &Controller{
		clientset:    clientset,
		config:       config,
		admin:        admin,
		nodeSelector: selector,
		queue:        workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		pushState:    make(map[string]pushRecord),
		pushFn:       admin.Load,
	}
	c.deployFn = c.deployNode
	c.aggregator = aggregator.New(clientset, config.Namespace, config.BootConfigMapName, c.enqueueManagedNodes)
	if err := c.setupInformers(); err != nil {
		c.queue.ShutDown()
		return nil, err
	}
	return c, nil
}

func validateConfig(config Config) (Config, labels.Selector, error) {
	selector, err := labels.Parse(config.NodeLabel)
	if err != nil {
		return config, nil, fmt.Errorf("invalid node label selector %q: %w", config.NodeLabel, err)
	}
	if config.ConfigResyncInterval < 0 {
		return config, nil, errors.New("config resync interval cannot be negative")
	}
	if err := config.Deploy.Validate(); err != nil {
		return config, nil, err
	}
	if config.ExternalEnable {
		externalSelector, err := labels.Parse(config.ExternalLabel)
		if err != nil {
			return config, nil, fmt.Errorf("invalid external label selector %q: %w", config.ExternalLabel, err)
		}
		if externalSelector.Empty() {
			return config, nil, errors.New("external label selector must not be empty")
		}
		config.ExternalLabel = externalSelector.String()
		for _, namespace := range slices.Concat(config.ExternalAllowNamespaces, config.ExternalDenyNamespaces) {
			if err := caddy.ValidateName("external namespace", namespace, content.IsDNS1123Label); err != nil {
				return config, nil, err
			}
		}
	}
	if err := errors.Join(
		caddy.ValidateName("namespace", config.Namespace, content.IsDNS1123Label),
		caddy.ValidateName("base ConfigMap name", config.BaseConfigMapName, content.IsDNS1123Subdomain),
		caddy.ValidateName("boot ConfigMap name", config.BootConfigMapName, content.IsDNS1123Subdomain),
	); err != nil {
		return config, nil, err
	}
	if config.BootConfigMapName == config.BaseConfigMapName {
		return config, nil, errors.New("external aggregated config name must differ from the base config-map name")
	}
	return config, selector, nil
}

func (c *Controller) setupInformers() error {
	c.nodeFactory = informers.NewSharedInformerFactory(c.clientset, informerResync)
	c.nsFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync, informers.WithNamespace(c.config.Namespace),
	)
	nodes := c.nodeFactory.Core().V1().Nodes()
	deployments := c.nsFactory.Apps().V1().Deployments()
	pods := c.nsFactory.Core().V1().Pods()
	c.nodeLister = nodes.Lister()
	c.deployLister = deployments.Lister()
	c.podLister = pods.Lister()
	managed := c.managedObjectHandler()
	err := errors.Join(
		c.register(nodes.Informer(), c.nodeHandler()),
		c.register(c.nsFactory.Core().V1().ConfigMaps().Informer(), c.configMapHandler()),
		c.register(deployments.Informer(), managed),
		c.register(pods.Informer(), managed),
		c.register(c.nsFactory.Core().V1().Services().Informer(), managed),
	)
	if err != nil || !c.config.ExternalEnable {
		return err
	}
	label := c.config.ExternalLabel
	c.extFactory = informers.NewSharedInformerFactoryWithOptions(
		c.clientset, informerResync,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = label
		}),
	)
	return c.register(c.extFactory.Core().V1().ConfigMaps().Informer(), c.externalConfigMapHandler())
}

func (c *Controller) register(informer cache.SharedIndexInformer, handler cache.ResourceEventHandler) error {
	registration, err := informer.AddEventHandler(handler)
	if err != nil {
		return err
	}
	c.cacheSyncs = append(c.cacheSyncs, registration.HasSynced)
	return nil
}

func (c *Controller) isManagedNode(node *corev1.Node) bool {
	return c.nodeSelector.Matches(labels.Set(node.Labels))
}

func (c *Controller) enqueueNodeIfManaged(node *corev1.Node) {
	if c.isManagedNode(node) {
		c.queue.Add(node.Name)
	}
}

func (c *Controller) nodeHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if node, ok := obj.(*corev1.Node); ok {
				c.enqueueNodeIfManaged(node)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldNode, isOldNode := oldObj.(*corev1.Node)
			newNode, isNewNode := newObj.(*corev1.Node)
			if isNewNode && (c.isManagedNode(newNode) || isOldNode && c.isManagedNode(oldNode)) {
				c.queue.Add(newNode.Name)
			}
		},
		DeleteFunc: func(obj any) {
			if node, ok := tombstone[*corev1.Node](obj); ok {
				c.enqueueNodeIfManaged(node)
			}
		},
	}
}

func (c *Controller) configMapHandler() cache.ResourceEventHandler {
	handle := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok {
			return
		}
		if cm.Name == c.config.BootConfigMapName {
			c.enqueueChangedMirror(cm)
			return
		}
		if cm.Name != c.config.BaseConfigMapName {
			return
		}
		data, exists := cm.Data[caddy.CaddyfileKey]
		if !exists {
			log.Warn().Str("configmap", cm.Name).Msg("Base ConfigMap has no Caddyfile; ignoring update")
			return
		}
		c.aggregator.UpdateBase(data)
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, newObj any) { handle(newObj) },
		DeleteFunc: func(obj any) {
			cm, ok := tombstone[*corev1.ConfigMap](obj)
			if !ok {
				return
			}
			if cm.Name == c.config.BaseConfigMapName {
				log.Warn().Str("configmap", cm.Name).Msg("Base ConfigMap deleted; keeping last known configuration")
			}
			if cm.Name == c.config.BootConfigMapName {
				c.queue.Add(mirrorKey)
			}
		},
	}
}

func (c *Controller) enqueueChangedMirror(cm *corev1.ConfigMap) {
	accepted, ok := c.aggregator.CurrentAccepted()
	if ok && cm.Data[caddy.CaddyfileKey] != accepted {
		c.queue.Add(mirrorKey)
	}
}

func (c *Controller) externalConfigMapHandler() cache.ResourceEventHandler {
	upsert := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || !c.namespaceAllowed(cm.Namespace) {
			return
		}
		source := cm.Namespace + "/" + cm.Name
		fragment, exists := cm.Data[caddy.CaddyfileKey]
		if !exists {
			c.aggregator.RemoveExternal(source)
			return
		}
		c.aggregator.SetExternal(source, fragment)
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    upsert,
		UpdateFunc: func(_, newObj any) { upsert(newObj) },
		DeleteFunc: func(obj any) {
			if cm, ok := tombstone[*corev1.ConfigMap](obj); ok {
				c.aggregator.RemoveExternal(cm.Namespace + "/" + cm.Name)
			}
		},
	}
}

func (c *Controller) managedObjectHandler() cache.ResourceEventHandler {
	enqueue := func(obj any) {
		object, ok := tombstone[metav1.Object](obj)
		if !ok {
			return
		}
		if nodeName := caddy.ManagedNodeName(object); nodeName != "" {
			c.queue.Add(nodeName)
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, newObj any) { enqueue(newObj) },
		DeleteFunc: enqueue,
	}
}

func (c *Controller) namespaceAllowed(namespace string) bool {
	allowed := c.config.ExternalAllowNamespaces
	if namespace == c.config.Namespace || slices.Contains(c.config.ExternalDenyNamespaces, namespace) {
		return false
	}
	return len(allowed) == 0 || slices.Contains(allowed, namespace)
}

func (c *Controller) Run(ctx context.Context) error {
	defer c.queue.ShutDown()
	logger := log.With().Str("component", "controller").Logger()
	if err := c.aggregator.InitializeMirror(ctx, c.admin.BootstrapCaddyfile()); err != nil {
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
	logger.Info().Msg("Controller shutting down; waiting for workers")
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

func (c *Controller) enqueueManagedNodes() {
	nodes, err := c.nodeLister.List(c.nodeSelector)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list managed nodes")
		return
	}
	for _, node := range nodes {
		c.queue.Add(node.Name)
	}
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
