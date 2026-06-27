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
	PodName        string
	PodReady       bool
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
	prePullPod := prePullPodName(i.NodeName)
	prePullErr := deletePrePullPod(ctx, i.KubeClient, i.Namespace, prePullPod)
	if prePullErr != nil {
		logger.Warn().Err(prePullErr).Str("prepullPod", prePullPod).Msg("Failed to delete leftover pre-pull pod")
		prePullErr = fmt.Errorf("failed to delete pre-pull pod %s: %w", prePullPod, prePullErr)
	}
	return errors.Join(
		i.deleteService(ctx, i.DeploymentName, "ClusterIP", logger),
		i.deleteService(ctx, i.LoadBalancerServiceName(), "LoadBalancer", logger),
		i.deleteDeployment(ctx, logger),
		prePullErr,
	)
}

func (i *Instance) deleteDeployment(ctx context.Context, logger zerolog.Logger) error {
	err := i.KubeClient.AppsV1().Deployments(i.Namespace).Delete(
		ctx, i.DeploymentName, metav1.DeleteOptions{},
	)
	switch {
	case err == nil:
		logger.Info().Msg("Deleted Caddy deployment")
		return nil
	case apierrors.IsNotFound(err):
		logger.Debug().Msg("Caddy deployment already deleted")
		return nil
	default:
		logger.Error().Err(err).Msg("Failed to delete Caddy deployment")
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, err)
	}
}

func (i *Instance) deleteService(
	ctx context.Context,
	name, kind string,
	logger zerolog.Logger,
) error {
	err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, name, metav1.DeleteOptions{},
	)
	switch {
	case err == nil:
		logger.Info().Msgf("Deleted %s Caddy service", kind)
		return nil
	case apierrors.IsNotFound(err):
		logger.Debug().Msgf("%s Caddy service already deleted or never created", kind)
		return nil
	default:
		logger.Warn().Err(err).Msgf("Failed to delete %s Caddy service", kind)
		return fmt.Errorf("failed to delete %s service %s: %w", kind, name, err)
	}
}
