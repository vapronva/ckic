package caddy

import (
	"context"
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
	PodName        string
	ServiceName    string
	DeploymentName string
	ExternalIPs    []string
	KubeClient     kubernetes.Interface
}

func (i *Instance) LoadBalancerServiceName() string {
	return i.DeploymentName + "-loadbalancer"
}

func (i *Instance) Delete(ctx context.Context) error {
	logger := log.With().
		Str("node", i.NodeName).
		Str("deployment", i.DeploymentName).
		Logger()
	i.deleteService(ctx, i.ServiceName, "ClusterIP", logger)
	i.deleteService(ctx, i.LoadBalancerServiceName(), "LoadBalancer", logger)
	err := i.KubeClient.AppsV1().Deployments(i.Namespace).Delete(
		ctx, i.DeploymentName, metav1.DeleteOptions{},
	)
	DeleteLegacyPodDisruptionBudget(ctx, i.KubeClient, i, logger)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug().Msg("Caddy deployment already deleted")
			return nil
		}
		logger.Error().Err(err).Msg("Failed to delete Caddy deployment")
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, err)
	}
	logger.Info().Msg("Deleted Caddy deployment")
	return nil
}

func (i *Instance) deleteService(
	ctx context.Context,
	name, kind string,
	logger zerolog.Logger,
) {
	err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, name, metav1.DeleteOptions{},
	)
	switch {
	case err == nil:
		logger.Info().Msgf("Deleted %s Caddy service", kind)
	case apierrors.IsNotFound(err):
		logger.Debug().Msgf("%s Caddy service already deleted or never created", kind)
	default:
		logger.Warn().Err(err).Msgf("Failed to delete %s Caddy service", kind)
	}
}
