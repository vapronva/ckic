package caddy

import (
	"crypto/sha256"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	labelApp            = "app"
	labelInstance       = "instance"
	labelCaddyManaged   = "ckic.cmld.ru/caddy-managed"
	labelType           = "ckic.cmld.ru/type"
	labelAppValue       = "caddy"
	labelManagedValue   = "true"
	labelTypeBootConfig = "aggregated-config"
	labelTypePrePull    = "image-prepull"

	annotationTemplateHash = "ckic.cmld.ru/template-hash"
)

func DeploymentName(nodeName string) string {
	return "caddy-" + nodeName
}

func loadBalancerServiceName(nodeName string) string {
	name := DeploymentName(nodeName) + "-loadbalancer"
	if len(validation.IsDNS1035Label(name)) == 0 {
		return name
	}
	digest := sha256.Sum256([]byte(nodeName))
	suffix := fmt.Sprintf("-%x-lb", digest[:6])
	prefix := DeploymentName(strings.ReplaceAll(nodeName, ".", "-"))
	prefix = prefix[:min(len(prefix), validation.DNS1035LabelMaxLength-len(suffix))]
	return strings.TrimRight(prefix, "-") + suffix
}

func PrePullPodName(nodeName string) string {
	return "caddy-prepull-" + nodeName
}

func selectorLabels(nodeName string) map[string]string {
	return map[string]string{labelApp: labelAppValue, labelInstance: nodeName}
}

func ManagedLabels(nodeName string) map[string]string {
	managed := selectorLabels(nodeName)
	managed[labelCaddyManaged] = labelManagedValue
	return managed
}

func ManagedNodeName(obj metav1.Object) string {
	objLabels := obj.GetLabels()
	if objLabels[labelCaddyManaged] != labelManagedValue {
		return ""
	}
	return objLabels[labelInstance]
}

func BootConfigLabels() map[string]string {
	return map[string]string{labelCaddyManaged: labelManagedValue, labelType: labelTypeBootConfig}
}

func managedListOptions(nodeName string) metav1.ListOptions {
	return metav1.ListOptions{LabelSelector: labels.SelectorFromSet(ManagedLabels(nodeName)).String()}
}

func ValidateName(kind, name string, check func(string) []string) error {
	if errs := check(name); len(errs) > 0 {
		return fmt.Errorf("invalid %s %q: %s", kind, name, strings.Join(errs, "; "))
	}
	return nil
}
