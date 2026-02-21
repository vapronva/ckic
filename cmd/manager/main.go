package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/controller"
	"git.horse/vapronva/ckic/pkg/utils"
)

const (
	defaultHTTPHostPort    = 80
	defaultHTTPSHostPort   = 443
	probeShutdownTimeout   = 5 * time.Second
	probeReadHeaderTimeout = 10 * time.Second
)

type cliOptions struct {
	kubeconfigPath               string
	nodeLabel                    string
	configMapName                string
	configMapNamespace           string
	bootstrapDefaultConfig       bool
	healthBindAddress            string
	communicationMethod          string
	logLevel                     string
	caddyImage                   string
	enableLoadBalancer           bool
	preferSavedState             bool
	secretName                   string
	secretEnvKeys                []string
	dataVolumePVC                string
	configVolumePVC              string
	externalEndpoints            []string
	externalEndpointsFile        string
	useHostNetwork               bool
	caddyAdminOriginKey          string
	httpHostPort                 int
	httpsHostPort                int
	externalEnable               bool
	externalLabel                string
	externalNsMode               string
	externalAllowNamespaces      string
	externalDenyNamespaces       string
	externalPublishAggregated    bool
	externalAggregatedConfigName string
	leaderElectionEnabled        bool
	leaderElectionLeaseName      string
	leaderElectionLeaseNamespace string
	leaderElectionLeaseDuration  time.Duration
	leaderElectionRenewDeadline  time.Duration
	leaderElectionRetryPeriod    time.Duration
}

func main() {
	options := parseCLIOptions()
	level := parseLogLevel(options.logLevel)
	utils.SetupLogger(level)
	commMethod, err := resolveCommunicationMethod(
		options.communicationMethod,
		options.useHostNetwork,
		options.enableLoadBalancer,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid communication mode configuration")
	}
	clientset, err := utils.GetKubernetesClient(options.kubeconfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to build Kubernetes client")
	}
	extEndpointsMap, err := utils.ParseExternalEndpoints(
		options.externalEndpoints,
		options.externalEndpointsFile,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse external endpoints")
	}
	cfg := controller.ControllerConfig{
		Kubeconfig:                   options.kubeconfigPath,
		NodeLabel:                    options.nodeLabel,
		ConfigMapName:                options.configMapName,
		ConfigMapNamespace:           options.configMapNamespace,
		BootstrapDefaultConfig:       options.bootstrapDefaultConfig,
		CommunicationMethod:          commMethod,
		CaddyImage:                   options.caddyImage,
		EnableLoadBalancer:           options.enableLoadBalancer,
		PreferSavedState:             options.preferSavedState,
		EnvSecretName:                options.secretName,
		EnvSecretKeys:                options.secretEnvKeys,
		DataVolumePVC:                options.dataVolumePVC,
		ConfigVolumePVC:              options.configVolumePVC,
		ExternalEndpoints:            extEndpointsMap,
		UseHostNetwork:               options.useHostNetwork,
		CaddyAdminOriginKey:          options.caddyAdminOriginKey,
		HTTPHostPort:                 options.httpHostPort,
		HTTPSHostPort:                options.httpsHostPort,
		ExternalEnable:               options.externalEnable,
		ExternalLabel:                options.externalLabel,
		ExternalNsMode:               options.externalNsMode,
		ExternalAllowNamespaces:      options.externalAllowNamespaces,
		ExternalDenyNamespaces:       options.externalDenyNamespaces,
		ExternalPublishAggregated:    options.externalPublishAggregated,
		ExternalAggregatedConfigName: options.externalAggregatedConfigName,
	}
	ctrl := newControllerOrDie(clientset, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	readiness := &atomic.Bool{}
	readiness.Store(false)
	probeErrCh := startHealthProbeServer(ctx, options.healthBindAddress, readiness)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down")
		cancel()
	}()
	log.Info().Msg("Starting CKIC manager")
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- runControllerWithLeaderElection(ctx, clientset, options, ctrl, readiness)
	}()
	var runErr error
	select {
	case runErr = <-runErrCh:
	case probeErr := <-probeErrCh:
		runErr = probeErr
		cancel()
		<-runErrCh
	}
	signal.Stop(sigCh)
	cancel()
	if runErr != nil {
		log.Error().Err(runErr).Msg("Controller exited with error")
		os.Exit(1)
	}
}

//nolint:funlen
func parseCLIOptions() cliOptions {
	kubeconfigPath := pflag.String("kubeconfig", "", "Path to kubeconfig file")
	nodeLabel := pflag.String("node-label", "ckic.cmld.ru/enabled", "Node label to watch for")
	configMapName := pflag.String(
		"config-map",
		"caddy-config",
		"ConfigMap containing Caddy configuration",
	)
	configMapNamespace := pflag.String(
		"config-namespace",
		"caddy-system",
		"Namespace of the ConfigMap and deployments",
	)
	bootstrapDefaultConfig := pflag.Bool(
		"bootstrap-default-config",
		false,
		"Create a default ConfigMap on startup only when it is missing",
	)
	healthBindAddress := pflag.String(
		"health-bind-address",
		":8081",
		"Address where health and readiness probes are served (set empty to disable)",
	)
	communicationMethod := pflag.String(
		"comm-method",
		"clusterip",
		"Communication method (clusterip, direct, hostnetwork)",
	)
	logLevel := pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	caddyImage := pflag.String(
		"caddy-image",
		"docker.horse/oss-images/zerossl-caddy/caddy:2.10.2-alpine",
		"Caddy image (format image:tag)",
	)
	enableLoadBalancer := pflag.Bool(
		"enable-loadbalancer",
		false,
		"Enable LoadBalancer service exposure",
	)
	preferSavedState := pflag.Bool(
		"prefer-saved-state",
		false,
		"Prefer saved (aka persistent) state during reconciliation",
	)
	secretName := pflag.String(
		"env-secret",
		"",
		"Name of the Kubernetes Secret to use for environment variables",
	)
	secretEnvKeys := pflag.StringSlice(
		"env-keys",
		[]string{},
		"Keys from the Secret to use as environment variables",
	)
	dataVolumePVC := pflag.String(
		"data-pvc",
		"",
		"Name of PVC to use for the /data volume (defaults to HostPath if empty)",
	)
	configVolumePVC := pflag.String(
		"config-pvc",
		"",
		"Name of PVC to use for the /config volume (defaults to HostPath if empty)",
	)
	externalEndpoints := pflag.StringArray(
		"external-endpoints",
		[]string{},
		"External endpoints for nodes (format: nodeName=ip1,ip2,...)",
	)
	externalEndpointsFile := pflag.String(
		"external-endpoints-file",
		"",
		"Path to JSON file containing external endpoints mapping",
	)
	useHostNetwork := pflag.Bool("use-host-network", false, "Use hostNetwork for Caddy pods")
	caddyAdminOriginKey := pflag.String(
		"caddy-admin-origin-key",
		"",
		"Origin key for Caddy admin API security",
	)
	httpHostPort := pflag.Int(
		"http-host-port",
		defaultHTTPHostPort,
		"Host port for HTTP when using hostNetwork",
	)
	httpsHostPort := pflag.Int(
		"https-host-port",
		defaultHTTPSHostPort,
		"Host port for HTTPS when using hostNetwork",
	)
	externalEnable := pflag.Bool(
		"external-enable",
		false,
		"Enable external namespace ConfigMap aggregation",
	)
	externalLabel := pflag.String(
		"external-label",
		"ckic.cmld.ru/aggregate=true",
		"Label selector for external ConfigMaps",
	)
	externalNsMode := pflag.String("external-ns-mode", "all", "Namespace mode: all, allow, or deny")
	externalAllowNamespaces := pflag.String(
		"external-allow-namespaces",
		"",
		"Comma-separated list of allowed namespaces (for allow mode)",
	)
	externalDenyNamespaces := pflag.String(
		"external-deny-namespaces",
		"",
		"Comma-separated list of denied namespaces (for deny mode)",
	)
	externalPublishAggregated := pflag.Bool(
		"external-publish-aggregated",
		true,
		"Publish aggregated Caddyfile to a mirror ConfigMap",
	)
	externalAggregatedConfigName := pflag.String(
		"external-aggregated-config-name",
		"ckic-caddy-config-working",
		"Name of the mirror ConfigMap for aggregated config",
	)
	leaderElectionEnabled := pflag.Bool(
		"leader-elect",
		true,
		"Enable leader election so only one manager instance reconciles resources",
	)
	leaderElectionLeaseName := pflag.String(
		"leader-election-lease-name",
		"ckic-manager-leader",
		"Name of the Lease resource used for leader election",
	)
	leaderElectionLeaseNamespace := pflag.String(
		"leader-election-lease-namespace",
		"",
		"Namespace of the Lease resource used for leader election (defaults to --config-namespace)",
	)
	leaderElectionLeaseDuration := pflag.Duration(
		"leader-election-lease-duration",
		15*time.Second,
		"Duration non-leaders wait before forcing a leader election",
	)
	leaderElectionRenewDeadline := pflag.Duration(
		"leader-election-renew-deadline",
		10*time.Second,
		"Duration the acting leader retries refreshing leadership before giving up",
	)
	leaderElectionRetryPeriod := pflag.Duration(
		"leader-election-retry-period",
		2*time.Second,
		"Time between attempts by clients to acquire or renew leadership",
	)
	pflag.Parse()
	return cliOptions{
		kubeconfigPath:               *kubeconfigPath,
		nodeLabel:                    *nodeLabel,
		configMapName:                *configMapName,
		configMapNamespace:           *configMapNamespace,
		bootstrapDefaultConfig:       *bootstrapDefaultConfig,
		healthBindAddress:            *healthBindAddress,
		communicationMethod:          *communicationMethod,
		logLevel:                     *logLevel,
		caddyImage:                   *caddyImage,
		enableLoadBalancer:           *enableLoadBalancer,
		preferSavedState:             *preferSavedState,
		secretName:                   *secretName,
		secretEnvKeys:                *secretEnvKeys,
		dataVolumePVC:                *dataVolumePVC,
		configVolumePVC:              *configVolumePVC,
		externalEndpoints:            *externalEndpoints,
		externalEndpointsFile:        *externalEndpointsFile,
		useHostNetwork:               *useHostNetwork,
		caddyAdminOriginKey:          *caddyAdminOriginKey,
		httpHostPort:                 *httpHostPort,
		httpsHostPort:                *httpsHostPort,
		externalEnable:               *externalEnable,
		externalLabel:                *externalLabel,
		externalNsMode:               *externalNsMode,
		externalAllowNamespaces:      *externalAllowNamespaces,
		externalDenyNamespaces:       *externalDenyNamespaces,
		externalPublishAggregated:    *externalPublishAggregated,
		externalAggregatedConfigName: *externalAggregatedConfigName,
		leaderElectionEnabled:        *leaderElectionEnabled,
		leaderElectionLeaseName:      *leaderElectionLeaseName,
		leaderElectionLeaseNamespace: *leaderElectionLeaseNamespace,
		leaderElectionLeaseDuration:  *leaderElectionLeaseDuration,
		leaderElectionRenewDeadline:  *leaderElectionRenewDeadline,
		leaderElectionRetryPeriod:    *leaderElectionRetryPeriod,
	}
}

func parseLogLevel(level string) zerolog.Level {
	parsedLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		return zerolog.InfoLevel
	}
	return parsedLevel
}

func resolveCommunicationMethod(
	method string,
	useHostNetwork, enableLoadBalancer bool,
) (caddy.CommunicationMethod, error) {
	if useHostNetwork && enableLoadBalancer {
		return caddy.CommunicationMethodClusterIP, errors.New(
			"cannot use both hostNetwork and LoadBalancer at the same time",
		)
	}
	commMethod := caddy.CommunicationMethodClusterIP
	switch method {
	case "clusterip":
		commMethod = caddy.CommunicationMethodClusterIP
	case "direct":
		commMethod = caddy.CommunicationMethodDirect
	case "hostnetwork":
		commMethod = caddy.CommunicationMethodHostNetwork
	default:
		log.Warn().Msgf("Unknown communication method %s, defaulting to clusterip", method)
	}
	if useHostNetwork && commMethod != caddy.CommunicationMethodHostNetwork {
		log.Info().
			Msg("Automatically setting communication method to \"hostnetwork\" when using hostNetwork")
		commMethod = caddy.CommunicationMethodHostNetwork
	}
	if commMethod == caddy.CommunicationMethodHostNetwork && !useHostNetwork {
		return commMethod, errors.New(
			"communication method 'hostnetwork' requires --use-host-network=true",
		)
	}
	return commMethod, nil
}

func newControllerOrDie(
	clientset *kubernetes.Clientset,
	cfg controller.ControllerConfig,
) *controller.Controller {
	ctrl, err := controller.NewController(clientset, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize controller")
	}
	return ctrl
}

func runControllerWithLeaderElection(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	options cliOptions,
	ctrl *controller.Controller,
	readiness *atomic.Bool,
) error {
	if !options.leaderElectionEnabled {
		readiness.Store(true)
		defer readiness.Store(false)
		return ctrl.Run(ctx)
	}
	leaseNamespace := options.leaderElectionLeaseNamespace
	if leaseNamespace == "" {
		leaseNamespace = options.configMapNamespace
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "ckic-manager"
	}
	identity := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      options.leaderElectionLeaseName,
			Namespace: leaseNamespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	runErrCh := make(chan error, 1)
	reportRunResult := func(err error) {
		select {
		case runErrCh <- err:
		default:
		}
	}
	log.Info().
		Str("leaseName", options.leaderElectionLeaseName).
		Str("leaseNamespace", leaseNamespace).
		Dur("leaseDuration", options.leaderElectionLeaseDuration).
		Dur("renewDeadline", options.leaderElectionRenewDeadline).
		Dur("retryPeriod", options.leaderElectionRetryPeriod).
		Msg("Leader election is enabled")
	go leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   options.leaderElectionLeaseDuration,
		RenewDeadline:   options.leaderElectionRenewDeadline,
		RetryPeriod:     options.leaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				log.Info().Str("identity", identity).Msg("Acquired leadership")
				readiness.Store(true)
				defer readiness.Store(false)
				runErr := ctrl.Run(leadCtx)
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					reportRunResult(runErr)
					return
				}
				reportRunResult(nil)
			},
			OnStoppedLeading: func() {
				if ctx.Err() != nil {
					return
				}
				reportRunResult(errors.New("leader election lost"))
			},
			OnNewLeader: func(newLeaderIdentity string) {
				log.Info().
					Str("leaderIdentity", newLeaderIdentity).
					Bool("isLocalLeader", newLeaderIdentity == identity).
					Msg("Observed leader election update")
			},
		},
	})
	select {
	case runErr := <-runErrCh:
		return runErr
	case <-ctx.Done():
		select {
		case runErr := <-runErrCh:
			return runErr
		default:
			return nil
		}
	}
}

func startHealthProbeServer(
	ctx context.Context,
	bindAddress string,
	readiness *atomic.Bool,
) <-chan error {
	errCh := make(chan error, 1)
	if bindAddress == "" {
		return errCh
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readiness.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})
	server := &http.Server{
		Addr:              bindAddress,
		Handler:           mux,
		ReadHeaderTimeout: probeReadHeaderTimeout,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), probeShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		log.Info().Str("bindAddress", bindAddress).Msg("Starting probe server")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("probe server failed: %w", err)
			return
		}
		errCh <- nil
	}()
	return errCh
}
