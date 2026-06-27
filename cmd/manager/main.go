package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	probeShutdownTimeout       = 5 * time.Second
	probeReadHeaderTimeout     = 10 * time.Second
	leaderRunErrTimeout        = 30 * time.Second
	leaderElectionJitterFactor = 1.2
)

const (
	commMethodClusterIP    = "clusterip"
	commMethodDirect       = "direct"
	loadBalancerModeNone   = "none"
	loadBalancerModeCilium = "cilium"
)

var errLeaderElectionLost = errors.New("leader election lost")

type cliOptions struct {
	kubeconfigPath               string
	nodeLabel                    string
	configMapName                string
	namespace                    string
	bootstrapDefaultConfig       bool
	configResyncInterval         time.Duration
	healthBindAddress            string
	communicationMethod          string
	logLevel                     string
	caddyImage                   string
	imagePullPolicy              string
	prePullImage                 bool
	loadBalancerMode             string
	secretName                   string
	secretEnvKeys                []string
	dataVolumePVC                string
	configVolumePVC              string
	externalEndpoints            []string
	useHostNetwork               bool
	caddyAdminOriginKey          string
	externalEnable               bool
	externalLabel                string
	externalNsMode               string
	externalAllowNamespaces      string
	externalDenyNamespaces       string
	externalPublishAggregated    bool
	externalAggregatedConfigName string
	leaderElectionEnabled        bool
	leaderElectionLeaseName      string
	leaderElectionLeaseDuration  time.Duration
	leaderElectionRenewDeadline  time.Duration
	leaderElectionRetryPeriod    time.Duration
}

type controllerRunner interface {
	Run(context.Context) error
}

type leaderElectionRunFunc func(context.Context, leaderelection.LeaderElectionConfig)

func main() {
	options := parseCLIOptions()
	setupLogger(options.logLevel)
	enableCiliumLB, err := resolveEnableCiliumLB(options.loadBalancerMode)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid loadbalancer mode")
	}
	commMethod, err := resolveCommunicationMethod(
		options.communicationMethod,
		options.useHostNetwork,
		enableCiliumLB,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid communication mode configuration")
	}
	imagePullPolicy, err := resolveImagePullPolicy(options.imagePullPolicy)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid image pull policy")
	}
	if leErr := validateLeaderElectionTimings(options); leErr != nil {
		log.Fatal().Err(leErr).Msg("Invalid leader election configuration")
	}
	clientset, err := utils.GetKubernetesClient(options.kubeconfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to build Kubernetes client")
	}
	extEndpointsMap, err := utils.ParseExternalEndpoints(options.externalEndpoints)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse external endpoints")
	}
	options.namespace, err = resolveNamespace(options.namespace)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to resolve namespace")
	}
	cfg := buildControllerConfig(options, commMethod, imagePullPolicy, enableCiliumLB, extEndpointsMap)
	ctrl := newControllerOrDie(clientset, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	readiness := &atomic.Bool{}
	probeErrCh := startHealthProbeServer(ctx, options.healthBindAddress, readiness)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().
			Str("signal", sig.String()).
			Msg("Received termination signal, shutting down")
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
		cancel()
		ctrlErr := <-runErrCh
		if probeErr != nil {
			runErr = probeErr
		} else {
			runErr = ctrlErr
		}
	}
	signal.Stop(sigCh)
	cancel()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error().Err(runErr).Msg("Controller exited with error")
		os.Exit(1)
	}
}

func parseCLIOptions() cliOptions {
	var opts cliOptions
	registerCoreCLIFlags(&opts)
	registerExternalCLIFlags(&opts)
	registerLeaderElectionCLIFlags(&opts)
	pflag.Parse()
	return opts
}

func registerCoreCLIFlags(opts *cliOptions) {
	pflag.StringVar(&opts.kubeconfigPath, "kubeconfig", "", "Path to kubeconfig file")
	pflag.StringVar(
		&opts.nodeLabel,
		"node-label",
		"ckic.cmld.ru/enabled=true",
		"Kubernetes label selector used to choose managed nodes (empty for all nodes)",
	)
	pflag.StringVar(
		&opts.configMapName,
		"config-map",
		"caddy-config",
		"ConfigMap containing Caddy configuration",
	)
	pflag.StringVar(
		&opts.namespace,
		"namespace",
		"",
		"Namespace for managed Caddy resources, ConfigMaps and leases "+
			"(defaults to the pod namespace via CKIC_NAMESPACE or the in-cluster service account)",
	)
	pflag.BoolVar(
		&opts.bootstrapDefaultConfig,
		"bootstrap-default-config",
		false,
		"Create a default ConfigMap on startup only when it is missing",
	)
	pflag.DurationVar(
		&opts.configResyncInterval,
		"config-resync-interval",
		0,
		"Periodically re-push the merged Caddyfile to all instances even when "+
			"unchanged (0 disables; e.g. 5m)",
	)
	pflag.StringVar(
		&opts.healthBindAddress,
		"health-bind-address",
		":8081",
		"Address where health and readiness probes are served (set empty to disable)",
	)
	pflag.StringVar(
		&opts.communicationMethod,
		"comm-method",
		commMethodClusterIP,
		"Communication method (clusterip or direct; hostNetwork uses --use-host-network)",
	)
	pflag.StringVar(
		&opts.logLevel,
		"log-level",
		"info",
		"Log level (debug, info, warn, error)",
	)
	pflag.StringVar(
		&opts.caddyImage,
		"caddy-image",
		"docker.horse/oss-images/zerossl-caddy/caddy:2.11.4-alpine",
		"Caddy image (format image:tag)",
	)
	pflag.StringVar(
		&opts.imagePullPolicy,
		"image-pull-policy",
		"IfNotPresent",
		"ImagePullPolicy for deployed Caddy pods (Always, IfNotPresent, Never)",
	)
	pflag.BoolVar(
		&opts.prePullImage,
		"prepull-image",
		true,
		"Pre-pull the Caddy image on a node before creating or updating its Deployment",
	)
	pflag.StringVar(
		&opts.loadBalancerMode,
		"loadbalancer-mode",
		loadBalancerModeNone,
		"LoadBalancer strategy: none, or cilium (one LB per node)",
	)
	pflag.StringVar(
		&opts.secretName,
		"env-secret",
		"",
		"Name of the Kubernetes Secret to use for environment variables",
	)
	pflag.StringSliceVar(
		&opts.secretEnvKeys,
		"env-keys",
		[]string{},
		"Keys from the Secret to use as environment variables",
	)
	pflag.StringVar(
		&opts.dataVolumePVC,
		"data-pvc",
		"",
		"Name of PVC to use for the /data volume (defaults to HostPath if empty)",
	)
	pflag.StringVar(
		&opts.configVolumePVC,
		"config-pvc",
		"",
		"Name of PVC to use for the /config volume (defaults to HostPath if empty)",
	)
	registerCoreNetworkingCLIFlags(opts)
}

func registerCoreNetworkingCLIFlags(opts *cliOptions) {
	pflag.StringArrayVar(
		&opts.externalEndpoints,
		"external-endpoints",
		[]string{},
		"External endpoints for nodes (format: nodeName=ip1,ip2,...)",
	)
	pflag.BoolVar(
		&opts.useHostNetwork,
		"use-host-network",
		false,
		"Use hostNetwork for Caddy pods",
	)
	pflag.StringVar(
		&opts.caddyAdminOriginKey,
		"caddy-admin-origin-key",
		"",
		"Origin key for Caddy admin API security",
	)
}

func registerExternalCLIFlags(opts *cliOptions) {
	pflag.BoolVar(
		&opts.externalEnable,
		"external-enable",
		false,
		"Enable external namespace ConfigMap aggregation",
	)
	pflag.StringVar(
		&opts.externalLabel,
		"external-label",
		"ckic.cmld.ru/aggregate=true",
		"Label selector for external ConfigMaps",
	)
	pflag.StringVar(
		&opts.externalNsMode,
		"external-ns-mode",
		"all",
		"Namespace mode: all, allow, or deny",
	)
	pflag.StringVar(
		&opts.externalAllowNamespaces,
		"external-allow-namespaces",
		"",
		"Comma-separated list of allowed namespaces (for allow mode)",
	)
	pflag.StringVar(
		&opts.externalDenyNamespaces,
		"external-deny-namespaces",
		"",
		"Comma-separated list of denied namespaces (for deny mode)",
	)
	pflag.BoolVar(
		&opts.externalPublishAggregated,
		"external-publish-aggregated",
		true,
		"Publish aggregated Caddyfile to a mirror ConfigMap",
	)
	pflag.StringVar(
		&opts.externalAggregatedConfigName,
		"external-aggregated-config-name",
		"ckic-caddy-config-working",
		"Name of the mirror ConfigMap for aggregated config",
	)
}

func registerLeaderElectionCLIFlags(opts *cliOptions) {
	pflag.BoolVar(
		&opts.leaderElectionEnabled,
		"leader-elect",
		true,
		"Enable leader election so only one manager instance reconciles resources",
	)
	pflag.StringVar(
		&opts.leaderElectionLeaseName,
		"leader-election-lease-name",
		"ckic-manager-leader",
		"Name of the Lease resource used for leader election",
	)
	pflag.DurationVar(
		&opts.leaderElectionLeaseDuration,
		"leader-election-lease-duration",
		15*time.Second,
		"Duration non-leaders wait before forcing a leader election",
	)
	pflag.DurationVar(
		&opts.leaderElectionRenewDeadline,
		"leader-election-renew-deadline",
		10*time.Second,
		"Duration the acting leader retries refreshing leadership before giving up",
	)
	pflag.DurationVar(
		&opts.leaderElectionRetryPeriod,
		"leader-election-retry-period",
		2*time.Second,
		"Time between attempts by clients to acquire or renew leadership",
	)
}

func setupLogger(level string) {
	parsedLevel, err := zerolog.ParseLevel(level)
	if err != nil || parsedLevel == zerolog.NoLevel {
		parsedLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(parsedLevel)
	var output io.Writer = os.Stdout
	if os.Getenv("LOG_FORMAT") != "json" {
		output = zerolog.SyncWriter(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}
	//nolint:reassign // zerolog, configuring global logger
	log.Logger = log.Output(output).
		With().
		Str("service", "ckic-manager").
		Logger()
}

func resolveEnableCiliumLB(mode string) (bool, error) {
	switch mode {
	case loadBalancerModeNone:
		return false, nil
	case loadBalancerModeCilium:
		return true, nil
	default:
		return false, fmt.Errorf(
			"invalid loadbalancer mode %q (want none or cilium)", mode,
		)
	}
}

func resolveCommunicationMethod(
	method string,
	useHostNetwork bool,
	enableCiliumLB bool,
) (caddy.CommunicationMethod, error) {
	var base caddy.CommunicationMethod
	switch method {
	case commMethodClusterIP:
		base = caddy.CommunicationMethodClusterIP
	case commMethodDirect:
		base = caddy.CommunicationMethodDirect
	default:
		return caddy.CommunicationMethodClusterIP, fmt.Errorf(
			"unknown communication method %q (want clusterip or direct)", method,
		)
	}
	if useHostNetwork {
		if enableCiliumLB {
			return caddy.CommunicationMethodClusterIP, errors.New(
				"cannot combine --use-host-network with the cilium loadbalancer",
			)
		}
		return caddy.CommunicationMethodHostNetwork, nil
	}
	return base, nil
}

func resolveImagePullPolicy(policy string) (string, error) {
	switch policy {
	case "Always", "IfNotPresent", "Never":
		return policy, nil
	default:
		return "", fmt.Errorf(
			"invalid image pull policy %q (want Always, IfNotPresent, or Never)",
			policy,
		)
	}
}

func validateLeaderElectionTimings(options cliOptions) error {
	if !options.leaderElectionEnabled {
		return nil
	}
	lease := options.leaderElectionLeaseDuration
	renew := options.leaderElectionRenewDeadline
	retry := options.leaderElectionRetryPeriod
	if lease <= 0 || renew <= 0 || retry <= 0 {
		return errors.New("leader election durations must all be positive")
	}
	if lease <= renew {
		return fmt.Errorf(
			"leader-election-lease-duration (%s) must be greater than "+
				"leader-election-renew-deadline (%s)",
			lease, renew,
		)
	}
	if minRenew := time.Duration(leaderElectionJitterFactor * float64(retry)); renew <= minRenew {
		return fmt.Errorf(
			"leader-election-renew-deadline (%s) must be greater than %s "+
				"(%.1f × leader-election-retry-period)",
			renew, minRenew, leaderElectionJitterFactor,
		)
	}
	return nil
}

func buildControllerConfig(
	options cliOptions,
	commMethod caddy.CommunicationMethod,
	imagePullPolicy string,
	enableCiliumLB bool,
	extEndpoints utils.ExternalEndpointsMap,
) controller.Config {
	return controller.Config{
		NodeLabel:                    options.nodeLabel,
		ConfigMapName:                options.configMapName,
		Namespace:                    options.namespace,
		BootstrapDefaultConfig:       options.bootstrapDefaultConfig,
		ConfigResyncInterval:         options.configResyncInterval,
		CommunicationMethod:          commMethod,
		CaddyImage:                   options.caddyImage,
		ImagePullPolicy:              imagePullPolicy,
		PrePullImage:                 options.prePullImage,
		EnableCiliumLB:               enableCiliumLB,
		EnvSecretName:                options.secretName,
		EnvSecretKeys:                options.secretEnvKeys,
		DataVolumePVC:                options.dataVolumePVC,
		ConfigVolumePVC:              options.configVolumePVC,
		ExternalEndpoints:            extEndpoints,
		UseHostNetwork:               options.useHostNetwork,
		CaddyAdminOriginKey:          options.caddyAdminOriginKey,
		ExternalEnable:               options.externalEnable,
		ExternalLabel:                options.externalLabel,
		ExternalNsMode:               options.externalNsMode,
		ExternalAllowNamespaces:      options.externalAllowNamespaces,
		ExternalDenyNamespaces:       options.externalDenyNamespaces,
		ExternalPublishAggregated:    options.externalPublishAggregated,
		ExternalAggregatedConfigName: options.externalAggregatedConfigName,
	}
}

func newControllerOrDie(
	clientset *kubernetes.Clientset,
	cfg controller.Config,
) *controller.Controller {
	ctrl, err := controller.NewController(clientset, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize controller")
	}
	return ctrl
}

func runControllerWithLeaderElection(
	ctx context.Context,
	clientset kubernetes.Interface,
	options cliOptions,
	ctrl controllerRunner,
	readiness *atomic.Bool,
) error {
	return runControllerWithLeaderElectionWithRunner(
		ctx,
		clientset,
		options,
		ctrl,
		readiness,
		leaderelection.RunOrDie,
	)
}

func runControllerWithLeaderElectionWithRunner(
	ctx context.Context,
	clientset kubernetes.Interface,
	options cliOptions,
	ctrl controllerRunner,
	readiness *atomic.Bool,
	runLeaderElection leaderElectionRunFunc,
) error {
	readiness.Store(true)
	defer readiness.Store(false)
	if !options.leaderElectionEnabled {
		return ctrl.Run(ctx)
	}
	leaseNamespace := options.namespace
	identity := leaderElectionIdentity()
	logLeaderElectionEnabled(options, leaseNamespace)
	leCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var leading atomic.Bool
	runErrCh := make(chan error, 1)
	runLeaderElection(leCtx, leaderelection.LeaderElectionConfig{
		Lock:            leaderElectionLock(clientset, options, leaseNamespace, identity),
		ReleaseOnCancel: true,
		LeaseDuration:   options.leaderElectionLeaseDuration,
		RenewDeadline:   options.leaderElectionRenewDeadline,
		RetryPeriod:     options.leaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				leading.Store(true)
				log.Info().Str("identity", identity).Msg("Acquired leadership")
				runErr := ctrl.Run(leadCtx)
				if errors.Is(runErr, context.Canceled) {
					runErr = nil
				}
				runErrCh <- runErr
				cancel()
			},
			OnStoppedLeading: func() {
				if leading.Load() {
					log.Info().Str("identity", identity).Msg("Lost leadership")
				}
			},
			OnNewLeader: func(leader string) {
				log.Info().
					Str("leaderIdentity", leader).
					Bool("isLocalLeader", leader == identity).
					Msg("Observed leader election update")
			},
		},
	})
	if ctx.Err() != nil {
		if !leading.Load() {
			return nil
		}
		select {
		case runErr := <-runErrCh:
			return runErr
		case <-time.After(leaderRunErrTimeout):
			return nil
		}
	}
	select {
	case runErr := <-runErrCh:
		if runErr != nil {
			return runErr
		}
		return errLeaderElectionLost
	case <-time.After(leaderRunErrTimeout):
		return errLeaderElectionLost
	}
}

func resolveNamespace(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if env := strings.TrimSpace(os.Getenv("CKIC_NAMESPACE")); env != "" {
		return env, nil
	}
	const saNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	if data, err := os.ReadFile(saNamespacePath); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns, nil
		}
	}
	return "", errors.New(
		"namespace not set: provide --namespace, CKIC_NAMESPACE, or run in-cluster",
	)
}

func leaderElectionIdentity() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "ckic-manager"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func leaderElectionLock(
	clientset kubernetes.Interface,
	options cliOptions,
	leaseNamespace, identity string,
) resourcelock.Interface {
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      options.leaderElectionLeaseName,
			Namespace: leaseNamespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
}

func logLeaderElectionEnabled(options cliOptions, leaseNamespace string) {
	log.Info().
		Str("leaseName", options.leaderElectionLeaseName).
		Str("leaseNamespace", leaseNamespace).
		Dur("leaseDuration", options.leaderElectionLeaseDuration).
		Dur("renewDeadline", options.leaderElectionRenewDeadline).
		Dur("retryPeriod", options.leaderElectionRetryPeriod).
		Msg("Leader election is enabled")
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
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			probeShutdownTimeout,
		)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn().Err(err).Msg("Probe server shutdown returned error")
		}
	}()
	go func() {
		log.Info().Str("bindAddress", bindAddress).Msg("Starting probe server")
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("probe server failed: %w", err)
			return
		}
		errCh <- nil
	}()
	return errCh
}
