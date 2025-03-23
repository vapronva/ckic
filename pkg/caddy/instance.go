package caddy

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Instance struct {
	NodeName       string                `json:"nodeName"`
	Namespace      string                `json:"namespace"`
	PodName        string                `json:"podName"`
	ServiceName    string                `json:"serviceName"`
	DeploymentName string                `json:"deploymentName"`
	FailureCount   int                   `json:"failureCount"`
	KubeClient     *kubernetes.Clientset `json:"-"`
}

func (i *Instance) Delete() error {
	ctx := context.Background()
	logger := log.With().
		Str("node", i.NodeName).
		Str("deployment", i.DeploymentName).
		Logger()
	if err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, i.ServiceName, metav1.DeleteOptions{}); err != nil {
		logger.Warn().Err(err).Msg("Failed to delete ClusterIP Caddy service")
	} else {
		logger.Info().Msg("Deleted ClusterIP Caddy service")
	}
	nodePortServiceName := i.DeploymentName + "-nodeport"
	if err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, nodePortServiceName, metav1.DeleteOptions{}); err != nil {
		logger.Warn().Err(err).Msg("Failed to delete NodePort Caddy service (if exists)")
	} else {
		logger.Info().Msg("Deleted NodePort Caddy service")
	}
	if err := i.KubeClient.AppsV1().Deployments(i.Namespace).Delete(
		ctx, i.DeploymentName, metav1.DeleteOptions{}); err != nil {
		logger.Error().Err(err).Msg("Failed to delete Caddy deployment")
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, err)
	}
	logger.Info().Msg("Deleted Caddy deployment")
	return nil
}
