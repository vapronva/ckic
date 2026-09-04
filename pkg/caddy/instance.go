package caddy

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	if err := errors.Join(
		i.deleteLoadBalancerService(ctx, logger),
		deletePrePullPod(ctx, i.KubeClient, i.Namespace, prePullPodName(i.NodeName)),
	); err != nil {
		return err
	}
	return i.deleteDeployment(ctx, logger)
}

func (i *Instance) deleteDeployment(ctx context.Context, logger zerolog.Logger) error {
	err := i.KubeClient.AppsV1().Deployments(i.Namespace).Delete(ctx, i.DeploymentName, metav1.DeleteOptions{})
	switch {
	case err == nil:
		logger.Info().Msg("Deleted Caddy deployment")
		return nil
	case apierrors.IsNotFound(err):
		return nil
	default:
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, err)
	}
}

func (i *Instance) deleteLoadBalancerService(ctx context.Context, logger zerolog.Logger) error {
	name := i.LoadBalancerServiceName()
	err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	switch {
	case err == nil:
		logger.Info().Str("service", name).Msg("Deleted Caddy LoadBalancer service")
		return nil
	case apierrors.IsNotFound(err):
		return nil
	default:
		return fmt.Errorf("failed to delete service %s: %w", name, err)
	}
}
