package utils

import (
	"errors"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func GetKubernetesClient(kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := getKubernetesConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	return clientset, nil
}

func getKubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if os.Getenv(clientcmd.RecommendedConfigPathEnvVar) == "" {
		config, err := rest.InClusterConfig()
		if err == nil {
			return config, nil
		}
		if !errors.Is(err, rest.ErrNotInCluster) {
			return nil, fmt.Errorf("in-cluster config failed: %w", err)
		}
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}
