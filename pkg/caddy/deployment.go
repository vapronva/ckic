package caddy

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	caddyHTTPPort                  int32 = 80
	caddyHTTPSPort                 int32 = 443
	caddyBinary                          = "caddy"
	caddyContainerName                   = caddyBinary
	defaultImagePullPolicy               = corev1.PullIfNotPresent
	caddyCapabilityNetAdm                = corev1.Capability("NET_ADMIN")
	caddyCapabilityBind                  = corev1.Capability("NET_BIND_SERVICE")
	caddyCapabilityDrop                  = corev1.Capability("ALL")
	caddyRunAsUser                       = int64(1000)
	caddyRunAsGroup                      = int64(1000)
	portNameHTTPTCP                      = "http-tcp"
	portNameHTTPUDP                      = "http-udp"
	portNameHTTPSTCP                     = "https-tcp"
	portNameHTTPSUDP                     = "https-udp"
	fieldManager                         = "ckic"
	adminProbePath                       = "/config/"
	probeTimeoutSeconds                  = 3
	startupProbePeriodSeconds            = 3
	startupProbeFailureThreshold         = 30
	livenessProbePeriodSeconds           = 20
	livenessProbeFailureThreshold        = 3
	readinessProbePeriodSeconds          = 10
	readinessProbeFailureThreshold       = 3
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
	Clientset           kubernetes.Interface
	Namespace           string
	CaddyImage          string
	EnableCiliumLB      bool
	EnvSecretName       string
	EnvSecretKeys       []string
	DataVolumePVC       string
	ConfigVolumePVC     string
	ConfigMapName       string
	CaddyAdminOriginKey string
	UseHostNetwork      bool
	ImagePullPolicy     corev1.PullPolicy
	PrePullImage        bool
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
	if errs := validation.IsValidLabelValue(nodeName); len(errs) > 0 {
		return nil, fmt.Errorf(
			"node %q cannot be used as the %q label value "+
				"(Kubernetes caps label values at 63 characters); Caddy will not be deployed: %s",
			nodeName, constants.LabelInstance, strings.Join(errs, "; "),
		)
	}
	logger := log.With().Str("node", nodeName).Logger()
	deploymentName := "caddy-" + nodeName
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      opts.Namespace,
		DeploymentName: deploymentName,
		ExternalIPs:    externalIPs,
		KubeClient:     opts.Clientset,
	}
	deployment := buildDeploymentApplyConfig(instance, opts, logger)
	if opts.PrePullImage {
		prePullBeforeApply(ctx, opts, instance, logger)
	}
	if _, err := opts.Clientset.AppsV1().
		Deployments(instance.Namespace).
		Apply(ctx, deployment, applyOptions()); err != nil {
		return nil, fmt.Errorf("failed to apply deployment %s: %w", deploymentName, err)
	}
	logger.Debug().Msg("Applied Caddy deployment")
	if err := applyCaddyServices(ctx, opts, instance, logger); err != nil {
		return nil, err
	}
	if pod, resolveErr := resolveActivePod(
		ctx, opts.Clientset, opts.Namespace, nodeName,
	); resolveErr != nil {
		logger.Debug().
			Err(resolveErr).
			Msg("Active Caddy pod not resolved yet; will reconcile on next requeue")
	} else {
		instance.PodName = pod.Name
		instance.PodReady = isPodReady(pod)
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
		logger.Debug().Msg("hostNetwork mode: ensuring no Caddy Services remain")
		return errors.Join(
			instance.deleteService(ctx, instance.DeploymentName, "ClusterIP", logger),
			instance.deleteService(ctx, instance.LoadBalancerServiceName(), "LoadBalancer", logger),
		)
	}
	if _, err := opts.Clientset.CoreV1().
		Services(instance.Namespace).
		Apply(ctx, clusterIPServiceApplyConfig(instance), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply service %s: %w", instance.DeploymentName, err)
	}
	lbName := instance.LoadBalancerServiceName()
	if !opts.EnableCiliumLB {
		return instance.deleteService(ctx, lbName, "LoadBalancer", logger)
	}
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
) *appsv1ac.DeploymentApplyConfiguration {
	container := buildCaddyContainer(opts, logger)
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
				WithSpec(podSpec)))
}

func buildCaddyContainer(
	opts DeployOptions,
	logger zerolog.Logger,
) *corev1ac.ContainerApplyConfiguration {
	ports := append([]*corev1ac.ContainerPortApplyConfiguration{
		corev1ac.ContainerPort().
			WithName("admin").
			WithContainerPort(int32(constants.CaddyAdminPort)).
			WithProtocol(corev1.ProtocolTCP),
	}, buildTrafficPorts(opts.UseHostNetwork)...)
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
		WithStartupProbe(caddyAdminProbe(
			opts.CaddyAdminOriginKey, startupProbePeriodSeconds, startupProbeFailureThreshold,
		)).
		WithLivenessProbe(caddyAdminProbe(
			opts.CaddyAdminOriginKey, livenessProbePeriodSeconds, livenessProbeFailureThreshold,
		)).
		WithReadinessProbe(caddyAdminProbe(
			opts.CaddyAdminOriginKey, readinessProbePeriodSeconds, readinessProbeFailureThreshold,
		)).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithRunAsNonRoot(false).
			WithCapabilities(corev1ac.Capabilities().
				WithAdd(caddyCapabilityNetAdm, caddyCapabilityBind).
				WithDrop(caddyCapabilityDrop)).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeRuntimeDefault)))
}

func caddyAdminProbe(
	originKey string,
	periodSeconds, failureThreshold int32,
) *corev1ac.ProbeApplyConfiguration {
	get := corev1ac.HTTPGetAction().
		WithPath(adminProbePath).
		WithPort(intstr.FromInt32(int32(constants.CaddyAdminPort))).
		WithScheme(corev1.URISchemeHTTP)
	if originKey != "" {
		get = get.WithHTTPHeaders(corev1ac.HTTPHeader().
			WithName("Origin").
			WithValue(adminOrigin(originKey)))
	}
	return corev1ac.Probe().
		WithHTTPGet(get).
		WithPeriodSeconds(periodSeconds).
		WithTimeoutSeconds(probeTimeoutSeconds).
		WithFailureThreshold(failureThreshold)
}

func buildTrafficPorts(useHostNetwork bool) []*corev1ac.ContainerPortApplyConfiguration {
	port := func(name string, container int32, protocol corev1.Protocol) *corev1ac.ContainerPortApplyConfiguration {
		p := corev1ac.ContainerPort().
			WithName(name).
			WithContainerPort(container).
			WithProtocol(protocol)
		if useHostNetwork {
			p = p.WithHostPort(container)
		}
		return p
	}
	return []*corev1ac.ContainerPortApplyConfiguration{
		port(portNameHTTPTCP, caddyHTTPPort, corev1.ProtocolTCP),
		port(portNameHTTPUDP, caddyHTTPPort, corev1.ProtocolUDP),
		port(portNameHTTPSTCP, caddyHTTPSPort, corev1.ProtocolTCP),
		port(portNameHTTPSUDP, caddyHTTPSPort, corev1.ProtocolUDP),
	}
}

func buildCaddyEnvVars(
	envSecretName string,
	envSecretKeys []string,
	logger zerolog.Logger,
) []*corev1ac.EnvVarApplyConfiguration {
	fieldEnv := func(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
		return corev1ac.EnvVar().WithName(name).WithValueFrom(
			corev1ac.EnvVarSource().WithFieldRef(
				corev1ac.ObjectFieldSelector().WithFieldPath(fieldPath),
			),
		)
	}
	downwardAPI := []*corev1ac.EnvVarApplyConfiguration{
		fieldEnv(constants.PodNameEnvVar, "metadata.name"),
		fieldEnv(constants.NodeNameEnvVar, "spec.nodeName"),
		fieldEnv(constants.PodIPEnvVar, "status.podIP"),
	}
	envVars := make(
		[]*corev1ac.EnvVarApplyConfiguration,
		0,
		len(envSecretKeys)+len(downwardAPI),
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
	return append(envVars, downwardAPI...)
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
	return corev1ac.Service(instance.DeploymentName, instance.Namespace).
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
		WithAllocateLoadBalancerNodePorts(false).
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

func resolveActivePod(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, nodeName string,
) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: constants.InstanceLabelSelector(nodeName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for node %s: %w", nodeName, err)
	}
	if pod, ok := selectNewestActivePod(pods.Items); ok {
		return pod, nil
	}
	return nil, fmt.Errorf("no active pod found for node %s", nodeName)
}

func selectNewestActivePod(pods []corev1.Pod) (*corev1.Pod, bool) {
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
	return selected, selected != nil
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
