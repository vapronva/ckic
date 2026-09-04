package caddy

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/constants"
)

const (
	caddyBinary                    = "caddy"
	caddyContainerName             = caddyBinary
	fieldManager                   = "ckic"
	adminProbePath                 = "/config/"
	probeTimeoutSeconds            = 3
	startupProbePeriodSeconds      = 3
	startupProbeFailureThreshold   = 30
	livenessProbePeriodSeconds     = 20
	livenessProbeFailureThreshold  = 3
	readinessProbePeriodSeconds    = 10
	readinessProbeFailureThreshold = 3
)

type trafficPort struct {
	name     string
	port     int32
	protocol corev1.Protocol
}

var trafficPorts = []trafficPort{
	{"http-tcp", 80, corev1.ProtocolTCP},
	{"http-udp", 80, corev1.ProtocolUDP},
	{"https-tcp", 443, corev1.ProtocolTCP},
	{"https-udp", 443, corev1.ProtocolUDP},
}

type DeployOptions struct {
	Clientset           kubernetes.Interface
	Namespace           string
	ConfigMapName       string
	CaddyImage          string
	ImagePullPolicy     corev1.PullPolicy
	PrePullImage        bool
	EnableCiliumLB      bool
	UseHostNetwork      bool
	EnvSecretName       string
	EnvSecretKeys       []string
	DataVolumePVC       string
	ConfigVolumePVC     string
	CaddyAdminOriginKey string
}

func applyOptions() metav1.ApplyOptions {
	return metav1.ApplyOptions{FieldManager: fieldManager, Force: true}
}

func selectorLabels(nodeName string) map[string]string {
	return map[string]string{
		constants.LabelApp:      constants.LabelAppValue,
		constants.LabelInstance: nodeName,
	}
}

func managedLabels(nodeName string) map[string]string {
	managed := selectorLabels(nodeName)
	managed[constants.LabelCaddyManaged] = constants.LabelManagedValue
	return managed
}

func EnsureCaddy(ctx context.Context, opts DeployOptions, nodeName string, externalIPs []string) (*Instance, error) {
	if errs := validation.IsValidLabelValue(nodeName); len(errs) > 0 {
		return nil, fmt.Errorf("node %q cannot be used as an instance label: %s", nodeName, strings.Join(errs, "; "))
	}
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      opts.Namespace,
		DeploymentName: DeploymentName(nodeName),
		ExternalIPs:    externalIPs,
		KubeClient:     opts.Clientset,
	}
	logger := log.With().Str("node", nodeName).Logger()
	if opts.PrePullImage {
		if err := prePullImage(ctx, opts, nodeName, logger); err != nil {
			logger.Warn().Err(err).Msg("Image pre-pull did not complete; proceeding (kubelet will pull on rollout)")
		}
	}
	if _, err := opts.Clientset.AppsV1().Deployments(opts.Namespace).
		Apply(ctx, deploymentApplyConfig(instance, opts), applyOptions()); err != nil {
		return nil, fmt.Errorf("failed to apply deployment %s: %w", instance.DeploymentName, err)
	}
	if err := applyLoadBalancerService(ctx, opts, instance, logger); err != nil {
		return nil, err
	}
	if err := instance.deleteDeploymentsExcept(ctx, instance.DeploymentName, logger); err != nil {
		return nil, err
	}
	pod, err := resolveActivePod(ctx, opts.Clientset, opts.Namespace, nodeName)
	if err != nil {
		logger.Debug().Err(err).Msg("Active Caddy pod not resolved yet; will reconcile on next requeue")
		return instance, nil
	}
	instance.PodName = pod.Name
	instance.PodIP = pod.Status.PodIP
	instance.PodReady = isPodReady(pod)
	instance.ContainerID = caddyContainerID(pod)
	return instance, nil
}

func applyLoadBalancerService(ctx context.Context, opts DeployOptions, instance *Instance, logger zerolog.Logger) error {
	if !opts.EnableCiliumLB || opts.UseHostNetwork {
		return instance.deleteServicesExcept(ctx, "", logger)
	}
	serviceName := instance.LoadBalancerServiceName()
	if _, err := opts.Clientset.CoreV1().Services(instance.Namespace).
		Apply(ctx, loadBalancerServiceApplyConfig(instance), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply loadbalancer service %s: %w", serviceName, err)
	}
	return instance.deleteServicesExcept(ctx, serviceName, logger)
}

func deploymentApplyConfig(instance *Instance, opts DeployOptions) *appsv1ac.DeploymentApplyConfiguration {
	podLabels := managedLabels(instance.NodeName)
	podSpec := corev1ac.PodSpec().
		WithAffinity(nodeNameAffinity(instance.NodeName)).
		WithAutomountServiceAccountToken(false).
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsNonRoot(false).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault))).
		WithContainers(caddyContainer(opts)).
		WithVolumes(caddyVolumes(opts)...).
		WithDNSPolicy(corev1.DNSClusterFirst)
	if opts.UseHostNetwork {
		podSpec = podSpec.WithHostNetwork(true).WithDNSPolicy(corev1.DNSClusterFirstWithHostNet)
	}
	return appsv1ac.Deployment(instance.DeploymentName, instance.Namespace).
		WithLabels(podLabels).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
			WithStrategy(deploymentStrategy(opts.UseHostNetwork)).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(instance.NodeName))).
			WithTemplate(corev1ac.PodTemplateSpec().WithLabels(podLabels).WithSpec(podSpec)))
}

func nodeNameAffinity(nodeName string) *corev1ac.AffinityApplyConfiguration {
	requirement := corev1ac.NodeSelectorRequirement().
		WithKey(metav1.ObjectNameField).
		WithOperator(corev1.NodeSelectorOpIn).
		WithValues(nodeName)
	return corev1ac.Affinity().WithNodeAffinity(corev1ac.NodeAffinity().
		WithRequiredDuringSchedulingIgnoredDuringExecution(corev1ac.NodeSelector().
			WithNodeSelectorTerms(corev1ac.NodeSelectorTerm().WithMatchFields(requirement))))
}

func caddyContainer(opts DeployOptions) *corev1ac.ContainerApplyConfiguration {
	ports := []*corev1ac.ContainerPortApplyConfiguration{
		corev1ac.ContainerPort().WithName("admin").WithContainerPort(constants.CaddyAdminPort).WithProtocol(corev1.ProtocolTCP),
	}
	for _, traffic := range trafficPorts {
		port := corev1ac.ContainerPort().WithName(traffic.name).WithContainerPort(traffic.port).WithProtocol(traffic.protocol)
		if opts.UseHostNetwork {
			port = port.WithHostPort(traffic.port)
		}
		ports = append(ports, port)
	}
	return corev1ac.Container().
		WithName(caddyContainerName).
		WithImage(opts.CaddyImage).
		WithImagePullPolicy(opts.ImagePullPolicy).
		WithPorts(ports...).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName(constants.VolumeNameCaddyConfig).
				WithMountPath("/etc/caddy/Caddyfile").WithSubPath(constants.CaddyfileKey).WithReadOnly(true),
			corev1ac.VolumeMount().WithName(constants.VolumeNameData).WithMountPath("/data"),
			corev1ac.VolumeMount().WithName(constants.VolumeNameConfig).WithMountPath("/config"),
		).
		WithEnv(caddyEnvVars(opts)...).
		WithStartupProbe(adminProbe(opts.CaddyAdminOriginKey, startupProbePeriodSeconds, startupProbeFailureThreshold)).
		WithLivenessProbe(adminProbe(opts.CaddyAdminOriginKey, livenessProbePeriodSeconds, livenessProbeFailureThreshold)).
		WithReadinessProbe(adminProbe(opts.CaddyAdminOriginKey, readinessProbePeriodSeconds, readinessProbeFailureThreshold)).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithRunAsNonRoot(false).
			WithCapabilities(corev1ac.Capabilities().WithAdd("NET_ADMIN", "NET_BIND_SERVICE").WithDrop("ALL")).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))
}

func adminProbe(originKey string, periodSeconds, failureThreshold int32) *corev1ac.ProbeApplyConfiguration {
	get := corev1ac.HTTPGetAction().
		WithPath(adminProbePath).
		WithPort(intstr.FromInt32(constants.CaddyAdminPort)).
		WithScheme(corev1.URISchemeHTTP)
	if originKey != "" {
		get = get.WithHTTPHeaders(corev1ac.HTTPHeader().WithName("Origin").WithValue(adminOrigin(originKey)))
	}
	return corev1ac.Probe().
		WithHTTPGet(get).
		WithPeriodSeconds(periodSeconds).
		WithTimeoutSeconds(probeTimeoutSeconds).
		WithFailureThreshold(failureThreshold)
}

func caddyEnvVars(opts DeployOptions) []*corev1ac.EnvVarApplyConfiguration {
	var envVars []*corev1ac.EnvVarApplyConfiguration
	if opts.EnvSecretName != "" {
		for _, key := range opts.EnvSecretKeys {
			envVars = append(envVars, corev1ac.EnvVar().WithName(key).WithValueFrom(corev1ac.EnvVarSource().
				WithSecretKeyRef(corev1ac.SecretKeySelector().WithName(opts.EnvSecretName).WithKey(key))))
		}
	}
	fieldRefs := []struct{ name, fieldPath string }{
		{constants.PodNameEnvVar, "metadata.name"},
		{constants.NodeNameEnvVar, "spec.nodeName"},
		{constants.PodIPEnvVar, "status.podIP"},
	}
	for _, ref := range fieldRefs {
		envVars = append(envVars, corev1ac.EnvVar().WithName(ref.name).WithValueFrom(corev1ac.EnvVarSource().
			WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath(ref.fieldPath))))
	}
	return envVars
}

func caddyVolumes(opts DeployOptions) []*corev1ac.VolumeApplyConfiguration {
	return []*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().WithName(constants.VolumeNameCaddyConfig).
			WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(opts.ConfigMapName).
				WithItems(corev1ac.KeyToPath().WithKey(constants.CaddyfileKey).WithPath(constants.CaddyfileKey))),
		storageVolume(constants.VolumeNameData, opts.DataVolumePVC, "/opt/cmld/caddy/data"),
		storageVolume(constants.VolumeNameConfig, opts.ConfigVolumePVC, "/opt/cmld/caddy/config"),
	}
}

func storageVolume(name, pvcName, hostPath string) *corev1ac.VolumeApplyConfiguration {
	if pvcName != "" {
		return corev1ac.Volume().WithName(name).
			WithPersistentVolumeClaim(corev1ac.PersistentVolumeClaimVolumeSource().WithClaimName(pvcName))
	}
	return corev1ac.Volume().WithName(name).
		WithHostPath(corev1ac.HostPathVolumeSource().WithPath(hostPath).WithType(corev1.HostPathDirectoryOrCreate))
}

func deploymentStrategy(useHostNetwork bool) *appsv1ac.DeploymentStrategyApplyConfiguration {
	if useHostNetwork {
		return appsv1ac.DeploymentStrategy().WithType(appsv1.RecreateDeploymentStrategyType)
	}
	quarter := intstr.FromString("25%")
	return appsv1ac.DeploymentStrategy().
		WithType(appsv1.RollingUpdateDeploymentStrategyType).
		WithRollingUpdate(appsv1ac.RollingUpdateDeployment().WithMaxSurge(quarter).WithMaxUnavailable(quarter))
}

func loadBalancerServiceApplyConfig(instance *Instance) *corev1ac.ServiceApplyConfiguration {
	ports := make([]*corev1ac.ServicePortApplyConfiguration, 0, len(trafficPorts))
	for _, traffic := range trafficPorts {
		ports = append(ports, corev1ac.ServicePort().
			WithName(traffic.name).
			WithPort(traffic.port).
			WithTargetPort(intstr.FromInt32(traffic.port)).
			WithProtocol(traffic.protocol))
	}
	return corev1ac.Service(instance.LoadBalancerServiceName(), instance.Namespace).
		WithLabels(managedLabels(instance.NodeName)).
		WithSpec(corev1ac.ServiceSpec().
			WithSelector(managedLabels(instance.NodeName)).
			WithType(corev1.ServiceTypeLoadBalancer).
			WithLoadBalancerClass(constants.CiliumNodeLoadBalancerClass).
			WithExternalTrafficPolicy(corev1.ServiceExternalTrafficPolicyLocal).
			WithAllocateLoadBalancerNodePorts(false).
			WithExternalIPs(instance.ExternalIPs...).
			WithPorts(ports...))
}

func resolveActivePod(ctx context.Context, clientset kubernetes.Interface, namespace, nodeName string) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(managedLabels(nodeName)).String(),
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
		if pod.DeletionTimestamp != nil ||
			pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if selected == nil {
			selected = pod
			continue
		}
		switch {
		case pod.CreationTimestamp.After(selected.CreationTimestamp.Time):
			selected = pod
		case pod.CreationTimestamp.Equal(&selected.CreationTimestamp) && pod.Name > selected.Name:
			selected = pod
		}
	}
	return selected, selected != nil
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func caddyContainerID(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == caddyContainerName {
			return status.ContainerID
		}
	}
	return ""
}
