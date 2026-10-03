package caddy

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Teardown(ctx context.Context, opts DeployOptions, nodeName string) error {
	return errors.Join(
		deleteServicesExcept(ctx, opts, nodeName, ""),
		deleteDeploymentsExcept(ctx, opts, nodeName, ""),
		deletePrePullPod(ctx, opts.Clientset, opts.Namespace, prePullPodName(nodeName), nil),
	)
}

func deleteDeploymentsExcept(ctx context.Context, opts DeployOptions, nodeName, keepName string) error {
	deployments := opts.Clientset.AppsV1().Deployments(opts.Namespace)
	owned, err := deployments.List(ctx, managedListOptions(nodeName))
	if err != nil {
		return fmt.Errorf("failed to list deployments for node %s: %w", nodeName, err)
	}
	for _, deployment := range owned.Items {
		if deployment.Name == keepName {
			continue
		}
		err := deployments.Delete(ctx, deployment.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &deployment.UID},
		})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to delete deployment %s: %w", deployment.Name, err)
		}
		log.Info().Str("node", nodeName).Str("deployment", deployment.Name).Msg("Deleted Caddy deployment")
	}
	return nil
}

func deleteServicesExcept(ctx context.Context, opts DeployOptions, nodeName, keepName string) error {
	services := opts.Clientset.CoreV1().Services(opts.Namespace)
	owned, err := services.List(ctx, managedListOptions(nodeName))
	if err != nil {
		return fmt.Errorf("failed to list services for node %s: %w", nodeName, err)
	}
	for _, service := range owned.Items {
		if service.Name == keepName {
			continue
		}
		err := services.Delete(ctx, service.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &service.UID},
		})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to delete service %s: %w", service.Name, err)
		}
		log.Info().Str("node", nodeName).Str("service", service.Name).Msg("Deleted Caddy service")
	}
	return nil
}
