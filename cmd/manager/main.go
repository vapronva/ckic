package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
	"gl.vprw.ru/vapronva/ckic/pkg/controller"
	"gl.vprw.ru/vapronva/ckic/pkg/utils"
)

func main() {
	kubeconfigPath := pflag.String("kubeconfig", "", "Path to kubeconfig file")
	nodeLabel := pflag.String("node-label", "ckic.cmld.ru/enabled", "Node label to watch for")
	configMapName := pflag.String("config-map", "caddy-config", "ConfigMap containing Caddy configuration")
	configMapNamespace := pflag.String("config-namespace", "caddy-system", "Namespace of the ConfigMap and deployments")
	communicationMethod := pflag.String("comm-method", "clusterip", "Communication method (clusterip or direct)")
	logLevel := pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	caddyImage := pflag.String("caddy-image", "rg.gl.vprw.ru/oss-images/zerossl-caddy/caddy:2.9.1-alpine", "Caddy image (format image:tag)")
	enableLoadBalancer := pflag.Bool("enable-loadbalancer", false, "Enable LoadBalancer service exposure")
	preferSavedState := pflag.Bool("prefer-saved-state", false, "Prefer saved (persistent) state during reconciliation")
	secretName := pflag.String("env-secret", "", "Name of the Kubernetes Secret to use for environment variables")
	secretEnvKeys := pflag.StringSlice("env-keys", []string{}, "Keys from the Secret to use as environment variables")
	dataVolumePVC := pflag.String("data-pvc", "", "Name of PVC to use for the /data volume (defaults to HostPath if empty)")
	configVolumePVC := pflag.String("config-pvc", "", "Name of PVC to use for the /config volume (defaults to HostPath if empty)")
	pflag.Parse()
	var commMethod caddy.CommunicationMethod
	switch *communicationMethod {
	case "clusterip":
		commMethod = caddy.CommunicationMethodClusterIP
	case "direct":
		commMethod = caddy.CommunicationMethodDirect
	default:
		log.Warn().Msgf("Unknown communication method %s, defaulting to clusterip", *communicationMethod)
		commMethod = caddy.CommunicationMethodClusterIP
	}
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	utils.SetupLogger(level)
	clientset, err := utils.GetKubernetesClient(*kubeconfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to build Kubernetes client")
	}
	cfg := controller.ControllerConfig{
		Kubeconfig:          *kubeconfigPath,
		NodeLabel:           *nodeLabel,
		ConfigMapName:       *configMapName,
		ConfigMapNamespace:  *configMapNamespace,
		CommunicationMethod: commMethod,
		CaddyImage:          *caddyImage,
		EnableLoadBalancer:  *enableLoadBalancer,
		PreferSavedState:    *preferSavedState,
		EnvSecretName:       *secretName,
		EnvSecretKeys:       *secretEnvKeys,
		DataVolumePVC:       *dataVolumePVC,
		ConfigVolumePVC:     *configVolumePVC,
	}
	ctrl, err := controller.NewController(clientset, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize controller")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down")
		cancel()
		time.Sleep(5 * time.Second)
		os.Exit(0)
	}()
	log.Info().Msg("Starting CKIC manager")
	if err := ctrl.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("Controller exited with error")
	}
}
