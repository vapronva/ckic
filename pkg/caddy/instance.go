package caddy

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type Instance struct {
	NodeName       string
	Namespace      string
	DeploymentName string
	PodName        string
	PodIP          string
	ContainerID    string
	PodReady       bool
	ExternalIPs    []string
	KubeClient     kubernetes.Interface
}

func (i *Instance) LoadBalancerServiceName() string {
	return loadBalancerServiceName(i.NodeName)
}

func (i *Instance) Delete(ctx context.Context) error {
	logger := log.With().Str("node", i.NodeName).Str("deployment", i.DeploymentName).Logger()
	return errors.Join(
		i.deleteServicesExcept(ctx, "", logger),
		i.deleteDeploymentsExcept(ctx, "", logger),
		deletePrePullPod(ctx, i.KubeClient, i.Namespace, prePullPodName(i.NodeName)),
	)
}

func (i *Instance) deleteDeploymentsExcept(ctx context.Context, keepName string, logger zerolog.Logger) error {
	deployments := i.KubeClient.AppsV1().Deployments(i.Namespace)
	owned, err := deployments.List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(managedLabels(i.NodeName)).String()})
	if err != nil {
		return fmt.Errorf("failed to list deployments for node %s: %w", i.NodeName, err)
	}
	for _, deployment := range owned.Items {
		if deployment.Name == keepName {
			continue
		}
		if err := deployments.Delete(ctx, deployment.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &deployment.UID},
		}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete deployment %s: %w", deployment.Name, err)
		}
		logger.Info().Str("deployment", deployment.Name).Msg("Deleted Caddy deployment")
	}
	return nil
}

func (i *Instance) deleteServicesExcept(ctx context.Context, keepName string, logger zerolog.Logger) error {
	services := i.KubeClient.CoreV1().Services(i.Namespace)
	owned, err := services.List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(managedLabels(i.NodeName)).String()})
	if err != nil {
		return fmt.Errorf("failed to list services for node %s: %w", i.NodeName, err)
	}
	for _, service := range owned.Items {
		if service.Name == keepName {
			continue
		}
		if err := services.Delete(ctx, service.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &service.UID},
		}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete service %s: %w", service.Name, err)
		}
		logger.Info().Str("service", service.Name).Msg("Deleted Caddy service")
	}
	return nil
}
