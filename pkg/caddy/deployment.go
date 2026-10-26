package caddy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	BootConfigMapName   string
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
	Name        string
	IP          string
	ContainerID string
}

func (o DeployOptions) Validate() error {
	if o.UseHostNetwork && o.EnableCiliumLB {
		return errors.New("cannot combine host networking with the cilium loadbalancer")
	}
	if len(o.EnvSecretKeys) > 0 && o.EnvSecretName == "" {
		return fmt.Errorf("environment keys %q require an environment secret", o.EnvSecretKeys)
	}
	for _, optional := range []struct {
		kind, name string
		check      func(string) []string
	}{
		{"environment secret name", o.EnvSecretName, content.IsDNS1123Subdomain},
		{"data PVC name", o.DataVolumePVC, content.IsDNS1123Subdomain},
		{"config PVC name", o.ConfigVolumePVC, content.IsDNS1123Subdomain},
	} {
		if optional.name == "" {
			continue
		}
		if err := ValidateName(optional.kind, optional.name, optional.check); err != nil {
			return err
		}
	}
	if o.CaddyAdminOriginKey != "" {
		if errs := content.IsDNS1123Subdomain(strings.ToLower(o.CaddyAdminOriginKey)); len(errs) > 0 {
			return fmt.Errorf("invalid admin origin key: %s", strings.Join(errs, "; "))
		}
	}
	envKeys := make(map[string]bool)
	for _, env := range podFieldEnvVars {
		envKeys[env.name] = true
	}
	for _, key := range o.EnvSecretKeys {
		if envKeys[key] {
			return fmt.Errorf("duplicate or reserved environment key %q", key)
		}
		if err := ValidateName("environment key", key, validation.IsConfigMapKey); err != nil {
			return err
		}
		envKeys[key] = true
	}
	return nil
}

func applyOptions() metav1.ApplyOptions {
	return metav1.ApplyOptions{FieldManager: fieldManager, Force: true}
}

func ApplyDeployment(ctx context.Context, opts DeployOptions, nodeName string) error {
	if err := ValidateName("instance label for node", nodeName, content.IsLabelValue); err != nil {
		return err
	}
	if _, err := opts.Clientset.AppsV1().Deployments(opts.Namespace).
		Apply(ctx, deploymentApplyConfig(opts, nodeName), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply deployment %s: %w", DeploymentName(nodeName), err)
	}
	return nil
}

func DeleteStaleDeployments(ctx context.Context, opts DeployOptions, nodeName string) error {
	return deleteDeploymentsExcept(ctx, opts, nodeName, DeploymentName(nodeName))
}

func EnsureLoadBalancerService(ctx context.Context, opts DeployOptions, nodeName string, externalIPs []string) error {
	if !opts.EnableCiliumLB {
		return deleteServicesExcept(ctx, opts, nodeName, "")
	}
	return ApplyLoadBalancerService(ctx, opts, nodeName, externalIPs)
}

func ApplyLoadBalancerService(ctx context.Context, opts DeployOptions, nodeName string, externalIPs []string) error {
	serviceName := loadBalancerServiceName(nodeName)
	if _, err := opts.Clientset.CoreV1().Services(opts.Namespace).
		Apply(ctx, loadBalancerServiceApplyConfig(opts.Namespace, nodeName, externalIPs), applyOptions()); err != nil {
		return fmt.Errorf("failed to apply loadbalancer service %s: %w", serviceName, err)
	}
	return deleteServicesExcept(ctx, opts, nodeName, serviceName)
}

func TemplateHash(opts DeployOptions, nodeName string) string {
	return templateHash(podTemplateApplyConfig(opts, nodeName))
}

func AppliedTemplateHash(deployment *appsv1.Deployment) string {
	return deployment.Annotations[annotationTemplateHash]
}

func templateHash(template *corev1ac.PodTemplateSpecApplyConfiguration) string {
	encoded, _ := json.Marshal(template)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:8])
}

func deploymentApplyConfig(opts DeployOptions, nodeName string) *appsv1ac.DeploymentApplyConfiguration {
	template := podTemplateApplyConfig(opts, nodeName)
	strategy := appsv1ac.DeploymentStrategy().
		WithType(appsv1.RollingUpdateDeploymentStrategyType).
		WithRollingUpdate(appsv1ac.RollingUpdateDeployment().
			WithMaxSurge(intstr.FromInt32(1)).
			WithMaxUnavailable(intstr.FromInt32(0)))
	if opts.UseHostNetwork {
		strategy = appsv1ac.DeploymentStrategy().WithType(appsv1.RecreateDeploymentStrategyType)
	}
	return appsv1ac.Deployment(DeploymentName(nodeName), opts.Namespace).
		WithLabels(ManagedLabels(nodeName)).
		WithAnnotations(map[string]string{annotationTemplateHash: templateHash(template)}).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
			WithStrategy(strategy).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(nodeName))).
			WithTemplate(template))
}

func podTemplateApplyConfig(opts DeployOptions, nodeName string) *corev1ac.PodTemplateSpecApplyConfiguration {
	podSpec := corev1ac.PodSpec().
		WithAffinity(nodeNameAffinity(nodeName)).
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
	return corev1ac.PodTemplateSpec().WithLabels(ManagedLabels(nodeName)).WithSpec(podSpec)
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
	for _, key := range opts.EnvSecretKeys {
		envVars = append(envVars, corev1ac.EnvVar().WithName(key).WithValueFrom(corev1ac.EnvVarSource().
			WithSecretKeyRef(corev1ac.SecretKeySelector().WithName(opts.EnvSecretName).WithKey(key))))
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
			WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(opts.BootConfigMapName).
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

func ListDeployments(ctx context.Context, opts DeployOptions) (map[string]*appsv1.Deployment, error) {
	selector := labels.SelectorFromSet(map[string]string{labelApp: labelAppValue, labelCaddyManaged: labelManagedValue})
	list, err := opts.Clientset.AppsV1().Deployments(opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("failed to list Caddy deployments: %w", err)
	}
	deployments := make(map[string]*appsv1.Deployment, len(list.Items))
	for idx := range list.Items {
		deployments[list.Items[idx].Name] = &list.Items[idx]
	}
	return deployments, nil
}

func FindActivePod(ctx context.Context, opts DeployOptions, nodeName string) (Pod, error) {
	pods, err := opts.Clientset.CoreV1().Pods(opts.Namespace).List(ctx, managedListOptions(nodeName))
	if err != nil {
		return Pod{}, fmt.Errorf("failed to list pods for node %s: %w", nodeName, err)
	}
	pod, ok := selectNewestActivePod(pods.Items)
	if !ok {
		return Pod{}, nil
	}
	return Pod{Name: pod.Name, IP: pod.Status.PodIP, ContainerID: startedCaddyContainerID(pod)}, nil
}

func selectNewestActivePod(pods []corev1.Pod) (*corev1.Pod, bool) {
	var selected *corev1.Pod
	for idx := range pods {
		pod := &pods[idx]
		if !IsActivePod(pod) {
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

func IsActivePod(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded
}

func PodProblem(pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodFailed {
		return "pod failed: " + pod.Status.Reason
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
			return condition.Reason + ": " + condition.Message
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if waiting := status.State.Waiting; waiting != nil && waiting.Reason != "ContainerCreating" {
			return waiting.Reason + ": " + waiting.Message
		}
	}
	return ""
}

func DeploymentProblem(deployment *appsv1.Deployment, now time.Time) string {
	failure := deploymentCondition(deployment, appsv1.DeploymentReplicaFailure)
	progressing := deploymentCondition(deployment, appsv1.DeploymentProgressing)
	available := deploymentCondition(deployment, appsv1.DeploymentAvailable)
	switch {
	case failure.Status == corev1.ConditionTrue:
		return failure.Reason + ": " + failure.Message
	case progressing.Status == corev1.ConditionFalse:
		return progressing.Reason + ": " + progressing.Message
	case available.Status == corev1.ConditionFalse && isPastProgressDeadline(deployment, available, now):
		return available.Reason + ": " + available.Message
	}
	return ""
}

func isPastProgressDeadline(deployment *appsv1.Deployment, available appsv1.DeploymentCondition, now time.Time) bool {
	deadline := deployment.Spec.ProgressDeadlineSeconds
	return deadline != nil && deployment.Status.ObservedGeneration >= deployment.Generation &&
		now.Sub(available.LastTransitionTime.Time) > time.Duration(*deadline)*time.Second
}

func deploymentCondition(deployment *appsv1.Deployment, conditionType appsv1.DeploymentConditionType) appsv1.DeploymentCondition {
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == conditionType {
			return condition
		}
	}
	return appsv1.DeploymentCondition{}
}

func startedCaddyContainerID(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == caddyContainerName && status.Started != nil && *status.Started {
			return status.ContainerID
		}
	}
	return ""
}
