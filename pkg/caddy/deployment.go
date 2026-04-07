package caddy

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	deploymentTimeout      = 3 * time.Minute
	deploymentPollDelay    = 5 * time.Second
	caddyHTTPPort          = 80
	caddyHTTPSPort         = 443
	configMapDefaultMode   = int32(0o644)
	hostPortMin            = 1
	hostPortMax            = 65535
	caddyContainerName     = "caddy"
	caddyImagePullPolicy   = corev1.PullPolicy("Always")
	caddyCapabilityNetAdm  = corev1.Capability("NET_ADMIN")
	caddyCapabilityBind    = corev1.Capability("NET_BIND_SERVICE")
	caddyCapabilityDrop    = corev1.Capability("ALL")
	downwardAPIEnvVarCount = 3
)

func managedLabels(nodeName string) map[string]string {
	return map[string]string{
		constants.LabelApp:          constants.LabelAppValue,
		constants.LabelInstance:     nodeName,
		constants.LabelCaddyManaged: constants.LabelManagedValue,
	}
}

func selectorLabels(nodeName string) map[string]string {
	return map[string]string{
		constants.LabelApp:      constants.LabelAppValue,
		constants.LabelInstance: nodeName,
	}
}

func trafficServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:       "http-tcp",
			Port:       caddyHTTPPort,
			TargetPort: intstr.FromInt(caddyHTTPPort),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "http-udp",
			Port:       caddyHTTPPort,
			TargetPort: intstr.FromInt(caddyHTTPPort),
			Protocol:   corev1.ProtocolUDP,
		},
		{
			Name:       "https-tcp",
			Port:       caddyHTTPSPort,
			TargetPort: intstr.FromInt(caddyHTTPSPort),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "https-udp",
			Port:       caddyHTTPSPort,
			TargetPort: intstr.FromInt(caddyHTTPSPort),
			Protocol:   corev1.ProtocolUDP,
		},
	}
}

type DeployOptions struct {
	Clientset          kubernetes.Interface
	Namespace          string
	CaddyImage         string
	EnableLoadBalancer bool
	EnvSecretName      string
	EnvSecretKeys      []string
	DataVolumePVC      string
	ConfigVolumePVC    string
	ConfigMapName      string
	UseHostNetwork     bool
	HTTPHostPort       int
	HTTPSHostPort      int
}

func DeployCaddy(
	ctx context.Context,
	opts DeployOptions,
	nodeName string,
	externalIPs []string,
) (*Instance, error) {
	deployCtx, cancel := context.WithTimeout(ctx, deploymentTimeout)
	defer cancel()
	logger := log.With().Str("node", nodeName).Logger()
	deploymentName := fmt.Sprintf("caddy-%s", nodeName)
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      opts.Namespace,
		DeploymentName: deploymentName,
		ServiceName:    deploymentName,
		ExternalIPs:    externalIPs,
		KubeClient:     opts.Clientset,
	}
	deploymentChanged, deploymentCreated, err := deployDeployment(
		deployCtx,
		opts,
		instance,
	)
	if err != nil {
		return nil, err
	}
	if err = deployCaddyServices(
		deployCtx,
		ctx,
		opts.Clientset,
		instance,
		opts.UseHostNetwork,
		opts.EnableLoadBalancer,
		deploymentCreated,
		logger,
	); err != nil {
		return nil, err
	}
	if err = waitForDeploymentReadyIfChanged(
		deployCtx,
		ctx,
		opts.Clientset,
		opts.Namespace,
		deploymentName,
		deploymentChanged,
		deploymentCreated,
		instance,
	); err != nil {
		return nil, err
	}
	if err = resolveAndAssignPodName(
		deployCtx,
		ctx,
		opts.Clientset,
		opts.Namespace,
		nodeName,
		instance,
		deploymentCreated,
		logger,
	); err != nil {
		return nil, err
	}
	logger.Info().Str("pod", instance.PodName).Msg("Caddy instance deployed successfully")
	return instance, nil
}

func deployCaddyServices(
	deployCtx context.Context,
	cleanupCtx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
	useHostNetwork, enableLoadBalancer, deploymentCreated bool,
	logger zerolog.Logger,
) error {
	if useHostNetwork {
		logger.Info().Msg("Skipping service creation due to hostNetwork mode")
		return nil
	}
	if _, err := deployService(deployCtx, clientset, instance); err != nil {
		rollbackCreatedDeployment(cleanupCtx, clientset, instance, deploymentCreated)
		return err
	}
	if !enableLoadBalancer {
		return nil
	}
	if _, err := deployLoadBalancerService(deployCtx, clientset, instance); err != nil {
		rollbackCreatedDeployment(cleanupCtx, clientset, instance, deploymentCreated)
		return err
	}
	return nil
}

func waitForDeploymentReadyIfChanged(
	deployCtx context.Context,
	cleanupCtx context.Context,
	clientset kubernetes.Interface,
	namespace, deploymentName string,
	deploymentChanged, deploymentCreated bool,
	instance *Instance,
) error {
	if !deploymentChanged {
		return nil
	}
	err := waitForDeploymentReady(deployCtx, clientset, namespace, deploymentName)
	if err == nil {
		return nil
	}
	rollbackCreatedDeployment(cleanupCtx, clientset, instance, deploymentCreated)
	return fmt.Errorf(
		"deployment %s/%s did not become ready: %w",
		namespace,
		deploymentName,
		err,
	)
}

func resolveAndAssignPodName(
	deployCtx context.Context,
	cleanupCtx context.Context,
	clientset kubernetes.Interface,
	namespace, nodeName string,
	instance *Instance,
	deploymentCreated bool,
	logger zerolog.Logger,
) error {
	podName, err := resolvePodName(deployCtx, clientset, namespace, nodeName)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to resolve Caddy pod name")
		rollbackCreatedDeployment(cleanupCtx, clientset, instance, deploymentCreated)
		return err
	}
	instance.PodName = podName
	return nil
}

func rollbackCreatedDeployment(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
	deploymentCreated bool,
) {
	if deploymentCreated {
		cleanupDeployment(ctx, clientset, instance)
	}
}

func resolvePodName(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, nodeName string,
) (string, error) {
	const (
		maxAttempts    = 6
		initialBackoff = 250 * time.Millisecond
		maxBackoff     = 2 * time.Second
	)
	labelSelector := fmt.Sprintf("app=caddy,instance=%s", nodeName)
	backoff := initialBackoff
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err == nil {
			if podName, ok := SelectNewestActivePodName(pods.Items); ok {
				return podName, nil
			}
			if len(pods.Items) == 0 {
				lastErr = fmt.Errorf("no pods found for node %s", nodeName)
			} else {
				lastErr = fmt.Errorf("no active pods found for node %s", nodeName)
			}
		} else {
			lastErr = fmt.Errorf("failed to list pods for node %s: %w", nodeName, err)
		}
		if attempt == maxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return "", fmt.Errorf(
				"context deadline exceeded while resolving pod for node %s: %w",
				nodeName,
				ctx.Err(),
			)
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return "", lastErr
}

func SelectNewestActivePodName(pods []corev1.Pod) (string, bool) {
	var selected *corev1.Pod
	for idx := range pods {
		pod := &pods[idx]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if selected == nil {
			selected = pod
			continue
		}
		switch {
		case pod.CreationTimestamp.After(selected.CreationTimestamp.Time):
			selected = pod
		case pod.CreationTimestamp.Equal(&selected.CreationTimestamp) &&
			pod.Name > selected.Name:
			selected = pod
		}
	}
	if selected == nil {
		return "", false
	}
	return selected.Name, true
}

func deployDeployment(
	ctx context.Context,
	opts DeployOptions,
	instance *Instance,
) (bool, bool, error) {
	logger := log.With().Str("deployment", instance.DeploymentName).Logger()
	deployment, err := buildDesiredDeployment(instance, opts, logger)
	if err != nil {
		return false, false, err
	}
	return upsertDeployment(ctx, opts.Clientset, instance, deployment, logger)
}

func buildDesiredDeployment(
	instance *Instance,
	options DeployOptions,
	logger zerolog.Logger,
) (*appsv1.Deployment, error) {
	replicas := int32(1)
	labels := managedLabels(instance.NodeName)
	caddyContainer, err := buildCaddyContainer(options, logger)
	if err != nil {
		return nil, err
	}
	volumes := buildCaddyVolumes(options, logger)
	podSpec := corev1.PodSpec{
		NodeSelector: map[string]string{
			"kubernetes.io/hostname": instance.NodeName,
		},
		Containers: []corev1.Container{caddyContainer},
		Volumes:    volumes,
	}
	if options.UseHostNetwork {
		podSpec.HostNetwork = true
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		logger.Info().Msg("Enabled hostNetwork mode")
	} else {
		podSpec.DNSPolicy = corev1.DNSClusterFirst
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.DeploymentName,
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: deploymentStrategy(options.UseHostNetwork),
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(instance.NodeName),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec,
			},
		},
	}, nil
}

func buildCaddyContainer(
	options DeployOptions,
	logger zerolog.Logger,
) (corev1.Container, error) {
	trafficPorts, err := buildTrafficPorts(
		options.UseHostNetwork,
		options.HTTPHostPort,
		options.HTTPSHostPort,
		logger,
	)
	if err != nil {
		return corev1.Container{}, err
	}
	container := corev1.Container{
		Name:  caddyContainerName,
		Image: options.CaddyImage,
		Ports: append(
			[]corev1.ContainerPort{
				{
					Name:          "admin",
					ContainerPort: constants.CaddyAdminPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
			trafficPorts...,
		),
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "caddy-config",
				MountPath: "/etc/caddy/Caddyfile",
				SubPath:   "Caddyfile",
			},
			{Name: "opt-data", MountPath: "/data"},
			{Name: "opt-config", MountPath: "/config"},
		},
		Env: buildCaddyEnvVars(
			options.EnvSecretName,
			options.EnvSecretKeys,
			logger,
		),
		ImagePullPolicy: caddyImagePullPolicy,
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{caddyCapabilityNetAdm, caddyCapabilityBind},
				Drop: []corev1.Capability{caddyCapabilityDrop},
			},
		},
	}
	return container, nil
}

func buildTrafficPorts(
	useHostNetwork bool,
	httpHostPort, httpsHostPort int,
	logger zerolog.Logger,
) ([]corev1.ContainerPort, error) {
	if !useHostNetwork {
		return []corev1.ContainerPort{
			{
				Name:          "http-tcp",
				ContainerPort: caddyHTTPPort,
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "http-udp",
				ContainerPort: caddyHTTPPort,
				Protocol:      corev1.ProtocolUDP,
			},
			{
				Name:          "https-tcp",
				ContainerPort: caddyHTTPSPort,
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "https-udp",
				ContainerPort: caddyHTTPSPort,
				Protocol:      corev1.ProtocolUDP,
			},
		}, nil
	}
	httpPort32, err := normalizeHostPort(httpHostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP host port: %w", err)
	}
	httpsPort32, err := normalizeHostPort(httpsHostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTPS host port: %w", err)
	}
	logger.Info().
		Int("httpPort", httpHostPort).
		Int("httpsPort", httpsHostPort).
		Msg("Configuring hostNetwork with host ports")
	return []corev1.ContainerPort{
		{
			Name:          "http-tcp",
			ContainerPort: caddyHTTPPort,
			Protocol:      corev1.ProtocolTCP,
			HostPort:      httpPort32,
		},
		{
			Name:          "http-udp",
			ContainerPort: caddyHTTPPort,
			Protocol:      corev1.ProtocolUDP,
			HostPort:      httpPort32,
		},
		{
			Name:          "https-tcp",
			ContainerPort: caddyHTTPSPort,
			Protocol:      corev1.ProtocolTCP,
			HostPort:      httpsPort32,
		},
		{
			Name:          "https-udp",
			ContainerPort: caddyHTTPSPort,
			Protocol:      corev1.ProtocolUDP,
			HostPort:      httpsPort32,
		},
	}, nil
}

func buildCaddyEnvVars(
	envSecretName string,
	envSecretKeys []string,
	logger zerolog.Logger,
) []corev1.EnvVar {
	envVars := make([]corev1.EnvVar, 0, len(envSecretKeys)+downwardAPIEnvVarCount)
	if envSecretName != "" && len(envSecretKeys) > 0 {
		logger.Info().
			Str("secret", envSecretName).
			Strs("keys", envSecretKeys).
			Msg("Configuring environment variables from secret")
		for _, key := range envSecretKeys {
			envVars = append(envVars, corev1.EnvVar{
				Name: key,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: envSecretName,
						},
						Key: key,
					},
				},
			})
		}
	}
	return append(envVars,
		corev1.EnvVar{
			Name: "CKIC_POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		corev1.EnvVar{
			Name: "CKIC_NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		corev1.EnvVar{
			Name: "CKIC_POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	)
}

func buildCaddyVolumes(
	options DeployOptions,
	logger zerolog.Logger,
) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: "caddy-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: options.ConfigMapName,
					},
					Items: []corev1.KeyToPath{
						{Key: "Caddyfile", Path: "Caddyfile"},
					},
				},
			},
		},
	}
	volumes = append(
		volumes,
		buildStorageVolume(
			"opt-data",
			options.DataVolumePVC,
			"/opt/cmld/caddy/data",
			logger,
		),
		buildStorageVolume(
			"opt-config",
			options.ConfigVolumePVC,
			"/opt/cmld/caddy/config",
			logger,
		),
	)
	return volumes
}

func buildStorageVolume(
	name, pvcName, hostPath string,
	logger zerolog.Logger,
) corev1.Volume {
	if pvcName != "" {
		logger.Info().Str("pvc", pvcName).Msgf("Using PVC for %s volume", name)
		return corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		}
	}
	logger.Info().Msgf("Using HostPath for %s volume", name)
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: hostPath,
				Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
			},
		},
	}
}

func upsertDeployment(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
	deployment *appsv1.Deployment,
	logger zerolog.Logger,
) (bool, bool, error) {
	existingDeployment, err := clientset.AppsV1().
		Deployments(instance.Namespace).
		Get(ctx, instance.DeploymentName, metav1.GetOptions{})
	switch {
	case err == nil:
		if !deploymentNeedsUpdate(existingDeployment, deployment) {
			logger.Debug().Msg("Caddy deployment already up-to-date")
			return false, false, nil
		}
		mergeDeploymentForUpdate(existingDeployment, deployment)
		_, err = clientset.AppsV1().
			Deployments(instance.Namespace).
			Update(ctx, existingDeployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing Caddy deployment")
			return false, false, fmt.Errorf(
				"failed to update deployment %s: %w",
				instance.DeploymentName,
				err,
			)
		}
		logger.Info().Msg("Updated existing Caddy deployment")
		deleteLegacyPodDisruptionBudget(ctx, clientset, instance, logger)
		return true, false, nil
	case apierrors.IsNotFound(err):
		_, err = clientset.AppsV1().
			Deployments(instance.Namespace).
			Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Caddy deployment")
			return false, false, fmt.Errorf(
				"failed to create deployment %s: %w",
				instance.DeploymentName,
				err,
			)
		}
		logger.Info().Msg("Created Caddy deployment")
		deleteLegacyPodDisruptionBudget(ctx, clientset, instance, logger)
		return true, true, nil
	default:
		logger.Error().Err(err).Msg("Failed to fetch existing Caddy deployment")
		return false, false, fmt.Errorf(
			"failed to get deployment %s: %w",
			instance.DeploymentName,
			err,
		)
	}
}

func deleteLegacyPodDisruptionBudget(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
	logger zerolog.Logger,
) {
	if cleanupErr := clientset.PolicyV1().
		PodDisruptionBudgets(instance.Namespace).
		Delete(ctx, instance.DeploymentName, metav1.DeleteOptions{}); cleanupErr == nil {
		logger.Info().Msg("Deleted legacy PodDisruptionBudget")
	}
}

func deploymentNeedsUpdate(existing, desired *appsv1.Deployment) bool {
	if !equality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Strategy, desired.Spec.Strategy) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return true
	}
	if !equality.Semantic.DeepEqual(
		existing.Spec.Template.Labels,
		desired.Spec.Template.Labels,
	) {
		return true
	}
	if podTemplateNeedsUpdate(existing.Spec.Template.Spec, desired.Spec.Template.Spec) {
		return true
	}
	return false
}

func mergeDeploymentForUpdate(existing, desired *appsv1.Deployment) {
	existing.Labels = desired.Labels
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Strategy = desired.Spec.Strategy
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Template.Labels = desired.Spec.Template.Labels
	existing.Spec.Template.Spec.NodeSelector = desired.Spec.Template.Spec.NodeSelector
	existing.Spec.Template.Spec.Volumes = desired.Spec.Template.Spec.Volumes
	existing.Spec.Template.Spec.Containers = desired.Spec.Template.Spec.Containers
	existing.Spec.Template.Spec.HostNetwork = desired.Spec.Template.Spec.HostNetwork
	existing.Spec.Template.Spec.DNSPolicy = desired.Spec.Template.Spec.DNSPolicy
}

func podTemplateNeedsUpdate(existing, desired corev1.PodSpec) bool {
	if !equality.Semantic.DeepEqual(existing.NodeSelector, desired.NodeSelector) {
		return true
	}
	if volumesNeedUpdate(existing.Volumes, desired.Volumes) {
		return true
	}
	if existing.HostNetwork != desired.HostNetwork {
		return true
	}
	if existing.DNSPolicy != desired.DNSPolicy {
		return true
	}
	if len(existing.Containers) != len(desired.Containers) {
		return true
	}
	for idx := range desired.Containers {
		if containerNeedsUpdate(existing.Containers[idx], desired.Containers[idx]) {
			return true
		}
	}
	return false
}

func volumesNeedUpdate(existing, desired []corev1.Volume) bool {
	if len(existing) != len(desired) {
		return true
	}
	existingByName := make(map[string]corev1.Volume, len(existing))
	for _, existingVolume := range existing {
		existingByName[existingVolume.Name] = normalizeVolumeForComparison(existingVolume)
	}
	for _, desiredVolume := range desired {
		normalizedDesiredVolume := normalizeVolumeForComparison(desiredVolume)
		normalizedExistingVolume, exists := existingByName[desiredVolume.Name]
		if !exists {
			return true
		}
		if !equality.Semantic.DeepEqual(
			normalizedExistingVolume,
			normalizedDesiredVolume,
		) {
			return true
		}
	}
	return false
}

func normalizeVolumeForComparison(volume corev1.Volume) corev1.Volume {
	normalized := volume.DeepCopy()
	if normalized.ConfigMap != nil {
		normalized.ConfigMap.DefaultMode = normalizeConfigMapDefaultMode(
			normalized.ConfigMap.DefaultMode,
		)
	}
	return *normalized
}

func normalizeConfigMapDefaultMode(defaultMode *int32) *int32 {
	if defaultMode == nil || *defaultMode == configMapDefaultMode {
		return nil
	}
	normalizedMode := *defaultMode
	return &normalizedMode
}

func containerNeedsUpdate(existing, desired corev1.Container) bool {
	if existing.Name != desired.Name {
		return true
	}
	if existing.Image != desired.Image {
		return true
	}
	if existing.ImagePullPolicy != desired.ImagePullPolicy {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Ports, desired.Ports) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.VolumeMounts, desired.VolumeMounts) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Env, desired.Env) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.SecurityContext, desired.SecurityContext) {
		return true
	}
	return false
}

func deploymentStrategy(useHostNetwork bool) appsv1.DeploymentStrategy {
	if useHostNetwork {
		return appsv1.DeploymentStrategy{
			Type: appsv1.RecreateDeploymentStrategyType,
		}
	}
	maxSurge := intstr.FromString("25%")
	maxUnavailable := intstr.FromString("25%")
	return appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxSurge:       &maxSurge,
			MaxUnavailable: &maxUnavailable,
		},
	}
}

func normalizeHostPort(port int) (int32, error) {
	if port < hostPortMin || port > hostPortMax {
		return 0, fmt.Errorf(
			"port %d is out of valid range [%d, %d]",
			port,
			hostPortMin,
			hostPortMax,
		)
	}
	return int32(port), nil
}

func deployService(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
) (bool, error) {
	logger := log.With().Str("service", instance.ServiceName).Logger()
	return upsertService(
		ctx, clientset, instance.Namespace, instance.DeploymentName,
		desiredClusterIPService(instance), logger,
	)
}

func deployLoadBalancerService(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
) (bool, error) {
	serviceName := instance.LoadBalancerServiceName()
	logger := log.With().Str("service", serviceName).Logger()
	desired := desiredLoadBalancerService(instance)
	if len(instance.ExternalIPs) > 0 {
		logger.Info().
			Strs("externalIPs", instance.ExternalIPs).
			Msg("Setting external IPs for LoadBalancer service")
		desired.Spec.ExternalIPs = instance.ExternalIPs
	}
	return upsertService(ctx, clientset, instance.Namespace, serviceName, desired, logger)
}

func upsertService(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, serviceName string,
	desired *corev1.Service,
	logger zerolog.Logger,
) (bool, error) {
	existing, err := clientset.CoreV1().
		Services(namespace).
		Get(ctx, serviceName, metav1.GetOptions{})
	switch {
	case err == nil:
		updated := existing.DeepCopy()
		mergeServiceForUpdate(updated, desired)
		if !serviceNeedsUpdate(existing, updated) {
			logger.Debug().Msg("Service already up-to-date")
			return false, nil
		}
		_, err = clientset.CoreV1().
			Services(namespace).
			Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update service")
			return false, fmt.Errorf("failed to update service %s: %w", serviceName, err)
		}
		logger.Info().Msg("Updated service")
		return true, nil
	case apierrors.IsNotFound(err):
		_, err = clientset.CoreV1().
			Services(namespace).
			Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create service")
			return false, fmt.Errorf("failed to create service %s: %w", serviceName, err)
		}
		logger.Info().Msg("Created service")
		return true, nil
	default:
		logger.Error().Err(err).Msg("Failed to fetch existing service")
		return false, fmt.Errorf("failed to get service %s: %w", serviceName, err)
	}
}

func desiredClusterIPService(
	instance *Instance,
) *corev1.Service {
	adminPort := corev1.ServicePort{
		Name:       "admin",
		Port:       constants.CaddyAdminPort,
		TargetPort: intstr.FromInt(constants.CaddyAdminPort),
		Protocol:   corev1.ProtocolTCP,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.DeploymentName,
			Namespace: instance.Namespace,
			Labels:    managedLabels(instance.NodeName),
		},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels(instance.NodeName),
			Ports:    append([]corev1.ServicePort{adminPort}, trafficServicePorts()...),
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
}

func desiredLoadBalancerService(instance *Instance) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.LoadBalancerServiceName(),
			Namespace: instance.Namespace,
			Labels:    managedLabels(instance.NodeName),
			Annotations: map[string]string{
				"io.cilium.nodeipam/match-node-labels": "kubernetes.io/hostname=" + instance.NodeName,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector:              selectorLabels(instance.NodeName),
			Ports:                 trafficServicePorts(),
			Type:                  corev1.ServiceTypeLoadBalancer,
			LoadBalancerClass:     new("io.cilium/node"),
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
		},
	}
}

func mergeServiceForUpdate(existing, desired *corev1.Service) {
	existing.Labels = desired.Labels
	if existing.Annotations == nil {
		existing.Annotations = make(map[string]string)
	}
	maps.Copy(existing.Annotations, desired.Annotations)
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Type = desired.Spec.Type
	existing.Spec.ExternalIPs = desired.Spec.ExternalIPs
	existing.Spec.ExternalTrafficPolicy = desired.Spec.ExternalTrafficPolicy
	existing.Spec.LoadBalancerClass = desired.Spec.LoadBalancerClass
	existing.Spec.Ports = mergeServicePortsKeepingNodePorts(
		existing.Spec.Ports,
		desired.Spec.Ports,
	)
}

func serviceNeedsUpdate(existing, desired *corev1.Service) bool {
	if !equality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}
	for k, v := range desired.Annotations {
		if existing.Annotations[k] != v {
			return true
		}
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return true
	}
	if existing.Spec.Type != desired.Spec.Type {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.ExternalIPs, desired.Spec.ExternalIPs) {
		return true
	}
	if existing.Spec.ExternalTrafficPolicy != desired.Spec.ExternalTrafficPolicy {
		return true
	}
	if !equality.Semantic.DeepEqual(
		existing.Spec.LoadBalancerClass,
		desired.Spec.LoadBalancerClass,
	) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
		return true
	}
	return false
}

func mergeServicePortsKeepingNodePorts(
	existingPorts []corev1.ServicePort,
	desiredPorts []corev1.ServicePort,
) []corev1.ServicePort {
	if len(desiredPorts) == 0 {
		return nil
	}
	byPortKey := make(map[string]corev1.ServicePort, len(existingPorts))
	for _, port := range existingPorts {
		byPortKey[servicePortKey(port)] = port
	}
	merged := make([]corev1.ServicePort, 0, len(desiredPorts))
	for _, port := range desiredPorts {
		if existingPort, exists := byPortKey[servicePortKey(port)]; exists &&
			existingPort.NodePort > 0 {
			port.NodePort = existingPort.NodePort
		}
		merged = append(merged, port)
	}
	return merged
}

func servicePortKey(port corev1.ServicePort) string {
	return port.Name + "/" + string(port.Protocol)
}

func waitForDeploymentReady(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, name string,
) error {
	logger := log.With().Str("deployment", name).Str("namespace", namespace).Logger()
	for {
		deployment, err := clientset.AppsV1().
			Deployments(namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get deployment %s: %w", name, err)
		}
		if deploymentRolloutComplete(deployment) {
			logger.Info().Msg("Deployment is ready")
			return nil
		}
		desiredReplicas := int32(1)
		if deployment.Spec.Replicas != nil {
			desiredReplicas = *deployment.Spec.Replicas
		}
		logger.Debug().
			Int64("generation", deployment.Generation).
			Int64("observedGeneration", deployment.Status.ObservedGeneration).
			Int32("available", deployment.Status.AvailableReplicas).
			Int32("ready", deployment.Status.ReadyReplicas).
			Int32("updated", deployment.Status.UpdatedReplicas).
			Int32("replicas", deployment.Status.Replicas).
			Int32("desired", desiredReplicas).
			Msg("Waiting for deployment to be ready")
		timer := time.NewTimer(deploymentPollDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf(
				"context deadline exceeded while waiting for deployment: %w",
				ctx.Err(),
			)
		case <-timer.C:
		}
	}
}

func deploymentRolloutComplete(deployment *appsv1.Deployment) bool {
	if deployment == nil || deployment.Spec.Replicas == nil {
		return false
	}
	desired := *deployment.Spec.Replicas
	if deployment.Status.ObservedGeneration < deployment.Generation {
		return false
	}
	if deployment.Status.UpdatedReplicas < desired {
		return false
	}
	if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
		return false
	}
	if deployment.Status.AvailableReplicas < desired {
		return false
	}
	if deployment.Status.ReadyReplicas < desired {
		return false
	}
	return true
}

func cleanupDeployment(
	ctx context.Context,
	_ kubernetes.Interface,
	instance *Instance,
) {
	log.Warn().
		Str("deployment", instance.DeploymentName).
		Msg("Cleaning up failed deployment")
	if err := instance.Delete(ctx); err != nil {
		log.Debug().
			Err(err).
			Str("deployment", instance.DeploymentName).
			Msg("Failed to clean up deployment resources")
	}
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return new(t)
}
