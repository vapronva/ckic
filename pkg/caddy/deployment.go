package caddy

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	caddyHTTPPort          int32 = 80
	caddyHTTPSPort         int32 = 443
	hostPortMin                  = 1
	hostPortMax                  = 65535
	caddyBinary                  = "caddy"
	caddyContainerName           = caddyBinary
	defaultImagePullPolicy       = corev1.PullIfNotPresent
	caddyCapabilityNetAdm        = corev1.Capability("NET_ADMIN")
	caddyCapabilityBind          = corev1.Capability("NET_BIND_SERVICE")
	caddyCapabilityDrop          = corev1.Capability("ALL")
	caddyRunAsUser               = int64(1000)
	caddyRunAsGroup              = int64(1000)
	downwardAPIEnvVarCount       = 3
	portNameHTTPTCP              = "http-tcp"
	portNameHTTPUDP              = "http-udp"
	portNameHTTPSTCP             = "https-tcp"
	portNameHTTPSUDP             = "https-udp"
	fieldManager                 = "ckic"
)

func applyOptions() metav1.ApplyOptions {
	return metav1.ApplyOptions{FieldManager: fieldManager, Force: true}
}

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

type DeployOptions struct {
	Clientset        kubernetes.Interface
	Namespace        string
	CaddyImage       string
	LoadBalancerMode LoadBalancerMode
	EnvSecretName    string
	EnvSecretKeys    []string
	DataVolumePVC    string
	ConfigVolumePVC  string
	ConfigMapName    string
	UseHostNetwork   bool
	HTTPHostPort     int
	HTTPSHostPort    int
	ImagePullPolicy  corev1.PullPolicy
	PrePullImage     bool
}

func (o DeployOptions) imagePullPolicy() corev1.PullPolicy {
	if o.ImagePullPolicy == "" {
		return defaultImagePullPolicy
	}
	return o.ImagePullPolicy
}

func EnsureCaddy(
	ctx context.Context,
	opts DeployOptions,
	nodeName string,
	externalIPs []string,
) (*Instance, error) {
	logger := log.With().Str("node", nodeName).Logger()
	deploymentName := "caddy-" + nodeName
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      opts.Namespace,
		DeploymentName: deploymentName,
		ServiceName:    deploymentName,
		ExternalIPs:    externalIPs,
		KubeClient:     opts.Clientset,
	}
	deployment, err := buildDeploymentApplyConfig(instance, opts, logger)
	if err != nil {
		return nil, err
	}
	if opts.PrePullImage {
		prePullBeforeApply(ctx, opts, instance, logger)
	}
	if _, err = opts.Clientset.AppsV1().
		Deployments(instance.Namespace).
		Apply(ctx, deployment, applyOptions()); err != nil {
		return nil, fmt.Errorf("failed to apply deployment %s: %w", deploymentName, err)
	}
	logger.Debug().Msg("Applied Caddy deployment")
	if err = applyCaddyServices(ctx, opts, instance, logger); err != nil {
		return nil, err
	}
	if podName, resolveErr := resolvePodName(
		ctx, opts.Clientset, opts.Namespace, nodeName,
	); resolveErr != nil {
		logger.Debug().
			Err(resolveErr).
			Msg("Active Caddy pod not resolved yet; will reconcile on next requeue")
	} else {
		instance.PodName = podName
	}
	return instance, nil
}

func applyCaddyServices(
	ctx context.Context,
	opts DeployOptions,
	instance *Instance,
	logger zerolog.Logger,
) error {
	if opts.UseHostNetwork {
		logger.Debug().Msg("Skipping service creation due to hostNetwork mode")
		return nil
	}
	if _, err := opts.Clientset.CoreV1().
		Services(instance.Namespace).
		Apply(ctx, clusterIPServiceApplyConfig(instance), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply service %s: %w", instance.ServiceName, err)
	}
	if opts.LoadBalancerMode != LoadBalancerModeCilium {
		return nil
	}
	lbName := instance.LoadBalancerServiceName()
	if _, err := opts.Clientset.CoreV1().
		Services(instance.Namespace).
		Apply(ctx, loadBalancerServiceApplyConfig(instance), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply loadbalancer service %s: %w", lbName, err)
	}
	return nil
}

func prePullBeforeApply(
	ctx context.Context,
	opts DeployOptions,
	instance *Instance,
	logger zerolog.Logger,
) {
	if err := prePullImage(
		ctx,
		opts.Clientset,
		instance.Namespace,
		instance.NodeName,
		opts.CaddyImage,
		opts.imagePullPolicy(),
		logger,
	); err != nil {
		logger.Warn().
			Err(err).
			Msg("Image pre-pull did not complete; proceeding (kubelet will pull on rollout)")
	}
}

func buildDeploymentApplyConfig(
	instance *Instance,
	opts DeployOptions,
	logger zerolog.Logger,
) (*appsv1ac.DeploymentApplyConfiguration, error) {
	container, err := buildCaddyContainer(opts, logger)
	if err != nil {
		return nil, err
	}
	labels := managedLabels(instance.NodeName)
	podSpec := corev1ac.PodSpec().
		WithNodeSelector(map[string]string{constants.HostLabelHostname: instance.NodeName}).
		WithAutomountServiceAccountToken(false).
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsNonRoot(false).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeRuntimeDefault))).
		WithContainers(container).
		WithVolumes(buildCaddyVolumes(opts, logger)...)
	if opts.UseHostNetwork {
		podSpec = podSpec.WithHostNetwork(true).
			WithDNSPolicy(corev1.DNSClusterFirstWithHostNet)
		logger.Debug().Msg("Enabled hostNetwork mode")
	} else {
		podSpec = podSpec.WithDNSPolicy(corev1.DNSClusterFirst)
	}
	return appsv1ac.Deployment(instance.DeploymentName, instance.Namespace).
		WithLabels(labels).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
			WithStrategy(deploymentStrategy(opts.UseHostNetwork)).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(selectorLabels(instance.NodeName))).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(labels).
				WithSpec(podSpec))), nil
}

func buildCaddyContainer(
	opts DeployOptions,
	logger zerolog.Logger,
) (*corev1ac.ContainerApplyConfiguration, error) {
	trafficPorts, err := buildTrafficPorts(
		opts.UseHostNetwork, opts.HTTPHostPort, opts.HTTPSHostPort, logger,
	)
	if err != nil {
		return nil, err
	}
	ports := append([]*corev1ac.ContainerPortApplyConfiguration{
		corev1ac.ContainerPort().
			WithName("admin").
			WithContainerPort(int32(constants.CaddyAdminPort)).
			WithProtocol(corev1.ProtocolTCP),
	}, trafficPorts...)
	return corev1ac.Container().
		WithName(caddyContainerName).
		WithImage(opts.CaddyImage).
		WithImagePullPolicy(opts.imagePullPolicy()).
		WithPorts(ports...).
		WithVolumeMounts(
			corev1ac.VolumeMount().
				WithName(constants.VolumeNameCaddyConfig).
				WithMountPath("/etc/caddy/Caddyfile").
				WithSubPath(constants.CaddyfileKey),
			corev1ac.VolumeMount().
				WithName(constants.VolumeNameData).
				WithMountPath("/data"),
			corev1ac.VolumeMount().
				WithName(constants.VolumeNameConfig).
				WithMountPath("/config"),
		).
		WithEnv(buildCaddyEnvVars(opts.EnvSecretName, opts.EnvSecretKeys, logger)...).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithRunAsNonRoot(false).
			WithCapabilities(corev1ac.Capabilities().
				WithAdd(caddyCapabilityNetAdm, caddyCapabilityBind).
				WithDrop(caddyCapabilityDrop)).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeRuntimeDefault))), nil
}

func buildTrafficPorts(
	useHostNetwork bool,
	httpHostPort, httpsHostPort int,
	logger zerolog.Logger,
) ([]*corev1ac.ContainerPortApplyConfiguration, error) {
	port := func(name string, container int32, protocol corev1.Protocol) *corev1ac.ContainerPortApplyConfiguration {
		return corev1ac.ContainerPort().
			WithName(name).
			WithContainerPort(container).
			WithProtocol(protocol)
	}
	ports := []*corev1ac.ContainerPortApplyConfiguration{
		port(portNameHTTPTCP, caddyHTTPPort, corev1.ProtocolTCP),
		port(portNameHTTPUDP, caddyHTTPPort, corev1.ProtocolUDP),
		port(portNameHTTPSTCP, caddyHTTPSPort, corev1.ProtocolTCP),
		port(portNameHTTPSUDP, caddyHTTPSPort, corev1.ProtocolUDP),
	}
	if !useHostNetwork {
		return ports, nil
	}
	httpPort32, err := normalizeHostPort(httpHostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP host port: %w", err)
	}
	httpsPort32, err := normalizeHostPort(httpsHostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTPS host port: %w", err)
	}
	logger.Debug().
		Int("httpPort", httpHostPort).
		Int("httpsPort", httpsHostPort).
		Msg("Configuring hostNetwork with host ports")
	hostPorts := []int32{httpPort32, httpPort32, httpsPort32, httpsPort32}
	for idx := range ports {
		ports[idx] = ports[idx].WithHostPort(hostPorts[idx])
	}
	return ports, nil
}

func buildCaddyEnvVars(
	envSecretName string,
	envSecretKeys []string,
	logger zerolog.Logger,
) []*corev1ac.EnvVarApplyConfiguration {
	envVars := make(
		[]*corev1ac.EnvVarApplyConfiguration,
		0,
		len(envSecretKeys)+downwardAPIEnvVarCount,
	)
	if envSecretName != "" && len(envSecretKeys) > 0 {
		logger.Debug().
			Str("secret", envSecretName).
			Strs("keys", envSecretKeys).
			Msg("Configuring environment variables from secret")
		for _, key := range envSecretKeys {
			envVars = append(envVars, corev1ac.EnvVar().WithName(key).WithValueFrom(
				corev1ac.EnvVarSource().WithSecretKeyRef(
					corev1ac.SecretKeySelector().WithName(envSecretName).WithKey(key),
				),
			))
		}
	}
	fieldEnv := func(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
		return corev1ac.EnvVar().WithName(name).WithValueFrom(
			corev1ac.EnvVarSource().WithFieldRef(
				corev1ac.ObjectFieldSelector().WithFieldPath(fieldPath),
			),
		)
	}
	return append(
		envVars,
		fieldEnv(constants.PodNameEnvVar, "metadata.name"),
		fieldEnv(constants.NodeNameEnvVar, "spec.nodeName"),
		fieldEnv(constants.PodIPEnvVar, "status.podIP"),
	)
}

func buildCaddyVolumes(
	opts DeployOptions,
	logger zerolog.Logger,
) []*corev1ac.VolumeApplyConfiguration {
	return []*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().
			WithName(constants.VolumeNameCaddyConfig).
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(opts.ConfigMapName).
				WithItems(corev1ac.KeyToPath().
					WithKey(constants.CaddyfileKey).
					WithPath(constants.CaddyfileKey))),
		buildStorageVolume(
			constants.VolumeNameData,
			opts.DataVolumePVC,
			"/opt/cmld/caddy/data",
			logger,
		),
		buildStorageVolume(
			constants.VolumeNameConfig,
			opts.ConfigVolumePVC,
			"/opt/cmld/caddy/config",
			logger,
		),
	}
}

func buildStorageVolume(
	name, pvcName, hostPath string,
	logger zerolog.Logger,
) *corev1ac.VolumeApplyConfiguration {
	if pvcName != "" {
		logger.Debug().Str("pvc", pvcName).Msgf("Using PVC for %s volume", name)
		return corev1ac.Volume().WithName(name).WithPersistentVolumeClaim(
			corev1ac.PersistentVolumeClaimVolumeSource().WithClaimName(pvcName),
		)
	}
	logger.Debug().Msgf("Using HostPath for %s volume", name)
	return corev1ac.Volume().WithName(name).WithHostPath(
		corev1ac.HostPathVolumeSource().
			WithPath(hostPath).
			WithType(corev1.HostPathDirectoryOrCreate),
	)
}

func deploymentStrategy(
	useHostNetwork bool,
) *appsv1ac.DeploymentStrategyApplyConfiguration {
	if useHostNetwork {
		return appsv1ac.DeploymentStrategy().
			WithType(appsv1.RecreateDeploymentStrategyType)
	}
	pct := intstr.FromString("25%")
	return appsv1ac.DeploymentStrategy().
		WithType(appsv1.RollingUpdateDeploymentStrategyType).
		WithRollingUpdate(appsv1ac.RollingUpdateDeployment().
			WithMaxSurge(pct).
			WithMaxUnavailable(pct))
}

func normalizeHostPort(port int) (int32, error) {
	if port < hostPortMin || port > hostPortMax {
		return 0, fmt.Errorf(
			"port %d is out of valid range [%d, %d]", port, hostPortMin, hostPortMax,
		)
	}
	return int32(port), nil
}

func trafficServicePorts() []*corev1ac.ServicePortApplyConfiguration {
	port := func(name string, p int32, protocol corev1.Protocol) *corev1ac.ServicePortApplyConfiguration {
		return corev1ac.ServicePort().
			WithName(name).
			WithPort(p).
			WithTargetPort(intstr.FromInt32(p)).
			WithProtocol(protocol)
	}
	return []*corev1ac.ServicePortApplyConfiguration{
		port(portNameHTTPTCP, caddyHTTPPort, corev1.ProtocolTCP),
		port(portNameHTTPUDP, caddyHTTPPort, corev1.ProtocolUDP),
		port(portNameHTTPSTCP, caddyHTTPSPort, corev1.ProtocolTCP),
		port(portNameHTTPSUDP, caddyHTTPSPort, corev1.ProtocolUDP),
	}
}

func clusterIPServiceApplyConfig(instance *Instance) *corev1ac.ServiceApplyConfiguration {
	adminPort := corev1ac.ServicePort().
		WithName("admin").
		WithPort(int32(constants.CaddyAdminPort)).
		WithTargetPort(intstr.FromInt32(int32(constants.CaddyAdminPort))).
		WithProtocol(corev1.ProtocolTCP)
	return corev1ac.Service(instance.ServiceName, instance.Namespace).
		WithLabels(managedLabels(instance.NodeName)).
		WithSpec(
			corev1ac.ServiceSpec().
				WithSelector(selectorLabels(instance.NodeName)).
				WithType(corev1.ServiceTypeClusterIP).
				WithPorts(append([]*corev1ac.ServicePortApplyConfiguration{adminPort}, trafficServicePorts()...)...),
		)
}

func loadBalancerServiceApplyConfig(
	instance *Instance,
) *corev1ac.ServiceApplyConfiguration {
	spec := corev1ac.ServiceSpec().
		WithSelector(selectorLabels(instance.NodeName)).
		WithType(corev1.ServiceTypeLoadBalancer).
		WithLoadBalancerClass(constants.CiliumNodeLoadBalancerClass).
		WithExternalTrafficPolicy(corev1.ServiceExternalTrafficPolicyTypeLocal).
		WithPorts(trafficServicePorts()...)
	if len(instance.ExternalIPs) > 0 {
		spec = spec.WithExternalIPs(instance.ExternalIPs...)
	}
	return corev1ac.Service(instance.LoadBalancerServiceName(), instance.Namespace).
		WithLabels(managedLabels(instance.NodeName)).
		WithAnnotations(map[string]string{
			constants.CiliumNodeIPAMAnnotationKey: constants.HostLabelHostname + "=" + instance.NodeName,
		}).
		WithSpec(spec)
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
		backoffFactor  = 2
	)
	labelSelector := constants.InstanceLabelSelector(nodeName)
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
				nodeName, ctx.Err(),
			)
		case <-timer.C:
		}
		backoff = min(backoff*backoffFactor, maxBackoff)
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

func DeleteLegacyPodDisruptionBudget(
	ctx context.Context,
	clientset kubernetes.Interface,
	instance *Instance,
	logger zerolog.Logger,
) {
	err := clientset.PolicyV1().
		PodDisruptionBudgets(instance.Namespace).
		Delete(ctx, instance.DeploymentName, metav1.DeleteOptions{})
	switch {
	case err == nil:
		logger.Info().Msg("Deleted legacy PodDisruptionBudget")
	case apierrors.IsNotFound(err):
	default:
		logger.Warn().Err(err).Msg("Failed to delete legacy PodDisruptionBudget")
	}
}
