package caddy

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Instance struct {
	NodeName       string               `json:"nodeName"`
	Namespace      string               `json:"namespace"`
	PodName        string               `json:"podName"`
	ServiceName    string               `json:"serviceName"`
	DeploymentName string               `json:"deploymentName"`
	FailureCount   atomic.Int32         `json:"-"`
	ExternalIPs    []string             `json:"externalIPs,omitempty"`
	KubeClient     kubernetes.Interface `json:"-"`
}

type instanceJSON struct {
	NodeName       string   `json:"nodeName"`
	Namespace      string   `json:"namespace"`
	PodName        string   `json:"podName"`
	ServiceName    string   `json:"serviceName"`
	DeploymentName string   `json:"deploymentName"`
	FailureCount   int32    `json:"failureCount"`
	ExternalIPs    []string `json:"externalIPs,omitempty"`
}

func (i *Instance) StateKey() string {
	if i == nil {
		return ""
	}
	return i.NodeName + "|" + i.Namespace + "|" + i.DeploymentName + "|" + i.ServiceName + "|" + i.PodName
}

func (i *Instance) MarshalJSON() ([]byte, error) {
	return json.Marshal(instanceJSON{
		NodeName:       i.NodeName,
		Namespace:      i.Namespace,
		PodName:        i.PodName,
		ServiceName:    i.ServiceName,
		DeploymentName: i.DeploymentName,
		FailureCount:   i.FailureCount.Load(),
		ExternalIPs:    i.ExternalIPs,
	})
}

func (i *Instance) UnmarshalJSON(data []byte) error {
	var j instanceJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	i.NodeName = j.NodeName
	i.Namespace = j.Namespace
	i.PodName = j.PodName
	i.ServiceName = j.ServiceName
	i.DeploymentName = j.DeploymentName
	i.FailureCount.Store(j.FailureCount)
	i.ExternalIPs = j.ExternalIPs
	return nil
}

func (i *Instance) LoadBalancerServiceName() string {
	return i.DeploymentName + "-loadbalancer"
}

func (i *Instance) Delete(ctx context.Context) error {
	logger := log.With().
		Str("node", i.NodeName).
		Str("deployment", i.DeploymentName).
		Logger()
	if err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, i.ServiceName, metav1.DeleteOptions{},
	); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug().Msg("ClusterIP Caddy service already deleted or never created")
		} else {
			logger.Warn().Err(err).Msg("Failed to delete ClusterIP Caddy service")
		}
	} else {
		logger.Info().Msg("Deleted ClusterIP Caddy service")
	}
	if err := i.KubeClient.CoreV1().Services(i.Namespace).Delete(
		ctx, i.LoadBalancerServiceName(), metav1.DeleteOptions{},
	); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug().
				Msg("LoadBalancer Caddy service already deleted or never created")
		} else {
			logger.Warn().
				Err(err).
				Msg("Failed to delete LoadBalancer Caddy service")
		}
	} else {
		logger.Info().Msg("Deleted LoadBalancer Caddy service")
	}
	if err := i.KubeClient.AppsV1().Deployments(i.Namespace).Delete(
		ctx, i.DeploymentName, metav1.DeleteOptions{},
	); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Debug().Msg("Caddy deployment already deleted")
			DeleteLegacyPodDisruptionBudget(ctx, i.KubeClient, i, logger)
			return nil
		}
		logger.Error().Err(err).Msg("Failed to delete Caddy deployment")
		DeleteLegacyPodDisruptionBudget(ctx, i.KubeClient, i, logger)
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, err)
	}
	logger.Info().Msg("Deleted Caddy deployment")
	DeleteLegacyPodDisruptionBudget(ctx, i.KubeClient, i, logger)
	return nil
}
