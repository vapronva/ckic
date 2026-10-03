package caddy

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
)

const (
	CaddyfileKey = "Caddyfile"

	caddyBinary                    = "caddy"
	caddyContainerName             = caddyBinary
	fieldManager                   = "ckic"
	volumeNameCaddyConfig          = "caddy-config"
	volumeNameData                 = "opt-data"
	volumeNameConfig               = "opt-config"
	ciliumNodeLoadBalancerClass    = "io.cilium/node"
	adminProbePath                 = "/config/admin/listen"
	readinessProbePath             = "/config/apps/http/http_port"
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

var podFieldEnvVars = []struct{ name, fieldPath string }{
	{"CKIC_POD_NAME", "metadata.name"},
	{"CKIC_NODE_NAME", "spec.nodeName"},
	{"CKIC_POD_IP", "status.podIP"},
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

type Pod struct {
	NodeName    string
	Name        string
	IP          string
	ContainerID string
}

func (o DeployOptions) Validate() error {
	if o.UseHostNetwork && o.EnableCiliumLB {
		return errors.New("cannot combine host networking with the cilium loadbalancer")
	}
	envKeys := make(map[string]bool)
	for _, env := range podFieldEnvVars {
		envKeys[env.name] = true
	}
	for _, key := range o.EnvSecretKeys {
		if envKeys[key] {
			return fmt.Errorf("duplicate or reserved environment key %q", key)
		}
		envKeys[key] = true
	}
	return nil
}

func applyOptions() metav1.ApplyOptions {
	return metav1.ApplyOptions{FieldManager: fieldManager, Force: true}
}

func EnsureCaddy(ctx context.Context, opts DeployOptions, nodeName string, externalIPs []string) (Pod, error) {
	if errs := validation.IsValidLabelValue(nodeName); len(errs) > 0 {
		return Pod{}, fmt.Errorf("node %q cannot be used as an instance label: %s", nodeName, strings.Join(errs, "; "))
	}
	if opts.PrePullImage {
		logger := log.With().Str("node", nodeName).Logger()
		if err := prePullImage(ctx, opts, nodeName, logger); err != nil {
			logger.Warn().Err(err).Msg("Image pre-pull did not complete; proceeding (kubelet will pull on rollout)")
		}
	}
	deploymentName := DeploymentName(nodeName)
	if _, err := opts.Clientset.AppsV1().Deployments(opts.Namespace).
		Apply(ctx, deploymentApplyConfig(opts, nodeName), applyOptions()); err != nil {
		return Pod{}, fmt.Errorf("failed to apply deployment %s: %w", deploymentName, err)
	}
	if err := applyLoadBalancerService(ctx, opts, nodeName, externalIPs); err != nil {
		return Pod{}, err
	}
	if err := deleteDeploymentsExcept(ctx, opts, nodeName, deploymentName); err != nil {
		return Pod{}, err
	}
	return findActivePod(ctx, opts, nodeName)
}

func applyLoadBalancerService(ctx context.Context, opts DeployOptions, nodeName string, externalIPs []string) error {
	if !opts.EnableCiliumLB {
		return deleteServicesExcept(ctx, opts, nodeName, "")
	}
	serviceName := loadBalancerServiceName(nodeName)
	if _, err := opts.Clientset.CoreV1().Services(opts.Namespace).
		Apply(ctx, loadBalancerServiceApplyConfig(opts.Namespace, nodeName, externalIPs), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply loadbalancer service %s: %w", serviceName, err)
	}
	return deleteServicesExcept(ctx, opts, nodeName, serviceName)
}

func deploymentApplyConfig(opts DeployOptions, nodeName string) *appsv1ac.DeploymentApplyConfiguration {
	podSpec := corev1ac.PodSpec().
		WithAffinity(nodeNameAffinity(nodeName)).
		WithAutomountServiceAccountToken(false).
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsNonRoot(false).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault))).
		WithContainers(caddyContainer(opts)).
		WithVolumes(caddyVolumes(opts)...).
		WithDNSPolicy(corev1.DNSClusterFirst)
	strategy := appsv1ac.DeploymentStrategy().
		WithType(appsv1.RollingUpdateDeploymentStrategyType).
		WithRollingUpdate(appsv1ac.RollingUpdateDeployment().
			WithMaxSurge(intstr.FromString("25%")).
			WithMaxUnavailable(intstr.FromString("25%")))
	if opts.UseHostNetwork {
		podSpec = podSpec.WithHostNetwork(true).WithDNSPolicy(corev1.DNSClusterFirstWithHostNet)
		strategy = appsv1ac.DeploymentStrategy().WithType(appsv1.RecreateDeploymentStrategyType)
	}
	podLabels := ManagedLabels(nodeName)
	return appsv1ac.Deployment(DeploymentName(nodeName), opts.Namespace).
		WithLabels(podLabels).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
			WithStrategy(strategy).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(nodeName))).
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
		corev1ac.ContainerPort().WithName("admin").WithContainerPort(adminPort).WithProtocol(corev1.ProtocolTCP),
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
			corev1ac.VolumeMount().WithName(volumeNameCaddyConfig).
				WithMountPath("/etc/caddy/Caddyfile").WithSubPath(CaddyfileKey).WithReadOnly(true),
			corev1ac.VolumeMount().WithName(volumeNameData).WithMountPath("/data"),
			corev1ac.VolumeMount().WithName(volumeNameConfig).WithMountPath("/config"),
		).
		WithEnv(caddyEnvVars(opts)...).
		WithStartupProbe(adminProbe(opts.CaddyAdminOriginKey, adminProbePath, startupProbePeriodSeconds, startupProbeFailureThreshold)).
		WithLivenessProbe(adminProbe(opts.CaddyAdminOriginKey, adminProbePath, livenessProbePeriodSeconds, livenessProbeFailureThreshold)).
		WithReadinessProbe(adminProbe(opts.CaddyAdminOriginKey, readinessProbePath, readinessProbePeriodSeconds, readinessProbeFailureThreshold)).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithRunAsNonRoot(false).
			WithCapabilities(corev1ac.Capabilities().WithAdd("NET_ADMIN", "NET_BIND_SERVICE").WithDrop("ALL")).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))
}

func adminProbe(originKey, path string, periodSeconds, failureThreshold int32) *corev1ac.ProbeApplyConfiguration {
	get := corev1ac.HTTPGetAction().
		WithPath(path).
		WithPort(intstr.FromInt32(adminPort)).
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
	for _, env := range podFieldEnvVars {
		envVars = append(envVars, corev1ac.EnvVar().WithName(env.name).WithValueFrom(corev1ac.EnvVarSource().
			WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath(env.fieldPath))))
	}
	return envVars
}

func caddyVolumes(opts DeployOptions) []*corev1ac.VolumeApplyConfiguration {
	return []*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().WithName(volumeNameCaddyConfig).
			WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(opts.ConfigMapName).
				WithItems(corev1ac.KeyToPath().WithKey(CaddyfileKey).WithPath(CaddyfileKey))),
		storageVolume(volumeNameData, opts.DataVolumePVC, "/opt/cmld/caddy/data"),
		storageVolume(volumeNameConfig, opts.ConfigVolumePVC, "/opt/cmld/caddy/config"),
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

func loadBalancerServiceApplyConfig(namespace, nodeName string, externalIPs []string) *corev1ac.ServiceApplyConfiguration {
	ports := make([]*corev1ac.ServicePortApplyConfiguration, 0, len(trafficPorts))
	for _, traffic := range trafficPorts {
		ports = append(ports, corev1ac.ServicePort().
			WithName(traffic.name).
			WithPort(traffic.port).
			WithTargetPort(intstr.FromInt32(traffic.port)).
			WithProtocol(traffic.protocol))
	}
	return corev1ac.Service(loadBalancerServiceName(nodeName), namespace).
		WithLabels(ManagedLabels(nodeName)).
		WithSpec(corev1ac.ServiceSpec().
			WithSelector(ManagedLabels(nodeName)).
			WithType(corev1.ServiceTypeLoadBalancer).
			WithLoadBalancerClass(ciliumNodeLoadBalancerClass).
			WithExternalTrafficPolicy(corev1.ServiceExternalTrafficPolicyLocal).
			WithAllocateLoadBalancerNodePorts(false).
			WithExternalIPs(externalIPs...).
			WithPorts(ports...))
}

func findActivePod(ctx context.Context, opts DeployOptions, nodeName string) (Pod, error) {
	pods, err := opts.Clientset.CoreV1().Pods(opts.Namespace).List(ctx, managedListOptions(nodeName))
	if err != nil {
		return Pod{}, fmt.Errorf("failed to list pods for node %s: %w", nodeName, err)
	}
	pod, ok := selectNewestActivePod(pods.Items)
	if !ok {
		return Pod{}, nil
	}
	return Pod{NodeName: nodeName, Name: pod.Name, IP: pod.Status.PodIP, ContainerID: caddyContainerID(pod)}, nil
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

func caddyContainerID(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == caddyContainerName {
			return status.ContainerID
		}
	}
	return ""
}
