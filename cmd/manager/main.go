package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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
	communicationMethod := pflag.String("comm-method", "clusterip", "Communication method (clusterip, direct, hostnetwork)")
	logLevel := pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	caddyImage := pflag.String("caddy-image", "rg.gl.vprw.ru/oss-images/zerossl-caddy/caddy:2.10.2-alpine", "Caddy image (format image:tag)")
	enableLoadBalancer := pflag.Bool("enable-loadbalancer", false, "Enable LoadBalancer service exposure")
	preferSavedState := pflag.Bool("prefer-saved-state", false, "Prefer saved (aka persistent) state during reconciliation")
	secretName := pflag.String("env-secret", "", "Name of the Kubernetes Secret to use for environment variables")
	secretEnvKeys := pflag.StringSlice("env-keys", []string{}, "Keys from the Secret to use as environment variables")
	dataVolumePVC := pflag.String("data-pvc", "", "Name of PVC to use for the /data volume (defaults to HostPath if empty)")
	configVolumePVC := pflag.String("config-pvc", "", "Name of PVC to use for the /config volume (defaults to HostPath if empty)")
	externalEndpoints := pflag.StringArray("external-endpoints", []string{}, "External endpoints for nodes (format: nodeName=ip1,ip2,...)")
	externalEndpointsFile := pflag.String("external-endpoints-file", "", "Path to JSON file containing external endpoints mapping")
	useHostNetwork := pflag.Bool("use-host-network", false, "Use hostNetwork for Caddy pods")
	caddyAdminOriginKey := pflag.String("caddy-admin-origin-key", "", "Origin key for Caddy admin API security")
	httpHostPort := pflag.Int("http-host-port", 80, "Host port for HTTP when using hostNetwork")
	httpsHostPort := pflag.Int("https-host-port", 443, "Host port for HTTPS when using hostNetwork")
	externalEnable := pflag.Bool("external-enable", false, "Enable external namespace ConfigMap aggregation")
	externalLabel := pflag.String("external-label", "ckic.cmld.ru/aggregate=true", "Label selector for external ConfigMaps")
	externalNsMode := pflag.String("external-ns-mode", "all", "Namespace mode: all, allow, or deny")
	externalAllowNamespaces := pflag.String("external-allow-namespaces", "", "Comma-separated list of allowed namespaces (for allow mode)")
	externalDenyNamespaces := pflag.String("external-deny-namespaces", "", "Comma-separated list of denied namespaces (for deny mode)")
	externalPublishAggregated := pflag.Bool("external-publish-aggregated", true, "Publish aggregated Caddyfile to a mirror ConfigMap")
	externalAggregatedConfigName := pflag.String("external-aggregated-config-name", "ckic-caddy-config-working", "Name of the mirror ConfigMap for aggregated config")
	pflag.Parse()
	var commMethod caddy.CommunicationMethod
	switch *communicationMethod {
	case "clusterip":
		commMethod = caddy.CommunicationMethodClusterIP
	case "direct":
		commMethod = caddy.CommunicationMethodDirect
	case "hostnetwork":
		commMethod = caddy.CommunicationMethodHostNetwork
	default:
		log.Warn().Msgf("Unknown communication method %s, defaulting to clusterip", *communicationMethod)
		commMethod = caddy.CommunicationMethodClusterIP
	}
	if *useHostNetwork && *enableLoadBalancer {
		log.Fatal().Msg("Cannot use both hostNetwork and LoadBalancer at the same time")
	}
	if *useHostNetwork && commMethod != caddy.CommunicationMethodHostNetwork {
		log.Info().Msg("Automatically setting communication method to \"hostnetwork\" when using hostNetwork")
		commMethod = caddy.CommunicationMethodHostNetwork
	}
	if commMethod == caddy.CommunicationMethodHostNetwork && !*useHostNetwork {
		log.Fatal().Msg("Communication method 'hostnetwork' requires --use-host-network=true")
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
	extEndpointsMap, err := utils.ParseExternalEndpoints(*externalEndpoints, *externalEndpointsFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse external endpoints")
	}
	cfg := controller.ControllerConfig{
		Kubeconfig:                   *kubeconfigPath,
		NodeLabel:                    *nodeLabel,
		ConfigMapName:                *configMapName,
		ConfigMapNamespace:           *configMapNamespace,
		CommunicationMethod:          commMethod,
		CaddyImage:                   *caddyImage,
		EnableLoadBalancer:           *enableLoadBalancer,
		PreferSavedState:             *preferSavedState,
		EnvSecretName:                *secretName,
		EnvSecretKeys:                *secretEnvKeys,
		DataVolumePVC:                *dataVolumePVC,
		ConfigVolumePVC:              *configVolumePVC,
		ExternalEndpoints:            extEndpointsMap,
		UseHostNetwork:               *useHostNetwork,
		CaddyAdminOriginKey:          *caddyAdminOriginKey,
		HTTPHostPort:                 *httpHostPort,
		HTTPSHostPort:                *httpsHostPort,
		ExternalEnable:               *externalEnable,
		ExternalLabel:                *externalLabel,
		ExternalNsMode:               *externalNsMode,
		ExternalAllowNamespaces:      *externalAllowNamespaces,
		ExternalDenyNamespaces:       *externalDenyNamespaces,
		ExternalPublishAggregated:    *externalPublishAggregated,
		ExternalAggregatedConfigName: *externalAggregatedConfigName,
	}
	ctrl, err := controller.NewController(clientset, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize controller")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down")
		cancel()
	}()
	log.Info().Msg("Starting CKIC manager")
	if err := ctrl.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("Controller exited with error")
	}
}
