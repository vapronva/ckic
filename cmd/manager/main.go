package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defaultHTTPHostPort           = 80
	defaultHTTPSHostPort          = 443
	probeShutdownTimeout          = 5 * time.Second
	probeReadHeaderTimeout        = 10 * time.Second
	leaderElectionShutdownMinWait = 1 * time.Second
	leaderElectionShutdownMaxWait = 30 * time.Second
)

var errLeaderElectionLost = errors.New("leader election lost")

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
	readinessRequireLeader       bool
	leaderElectionLeaseName      string
	leaderElectionLeaseNamespace string
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
	level := parseLogLevel(options.logLevel)
	setupLogger(level)
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
	pflag.StringVar(&opts.nodeLabel,
		"node-label",
		"ckic.cmld.ru/enabled=true",
		"Kubernetes label selector used to choose managed nodes",
	)
	pflag.StringVar(&opts.configMapName,
		"config-map",
		"caddy-config",
		"ConfigMap containing Caddy configuration",
	)
	pflag.StringVar(&opts.configMapNamespace,
		"config-namespace",
		"caddy-system",
		"Namespace of the ConfigMap and deployments",
	)
	pflag.BoolVar(&opts.bootstrapDefaultConfig,
		"bootstrap-default-config",
		false,
		"Create a default ConfigMap on startup only when it is missing",
	)
	pflag.StringVar(&opts.healthBindAddress,
		"health-bind-address",
		":8081",
		"Address where health and readiness probes are served (set empty to disable)",
	)
	pflag.StringVar(&opts.communicationMethod,
		"comm-method",
		"clusterip",
		"Communication method (clusterip, direct, hostnetwork)",
	)
	pflag.StringVar(&opts.logLevel,
		"log-level",
		"info",
		"Log level (debug, info, warn, error)",
	)
	pflag.StringVar(&opts.caddyImage,
		"caddy-image",
		"docker.horse/oss-images/zerossl-caddy/caddy:2.11.2-alpine",
		"Caddy image (format image:tag)",
	)
	pflag.BoolVar(&opts.enableLoadBalancer,
		"enable-loadbalancer",
		false,
		"Enable LoadBalancer service exposure",
	)
	pflag.BoolVar(&opts.preferSavedState,
		"prefer-saved-state",
		false,
		"Prefer saved (aka persistent) state during reconciliation",
	)
	pflag.StringVar(&opts.secretName,
		"env-secret",
		"",
		"Name of the Kubernetes Secret to use for environment variables",
	)
	pflag.StringSliceVar(&opts.secretEnvKeys,
		"env-keys",
		[]string{},
		"Keys from the Secret to use as environment variables",
	)
	pflag.StringVar(&opts.dataVolumePVC,
		"data-pvc",
		"",
		"Name of PVC to use for the /data volume (defaults to HostPath if empty)",
	)
	pflag.StringVar(&opts.configVolumePVC,
		"config-pvc",
		"",
		"Name of PVC to use for the /config volume (defaults to HostPath if empty)",
	)
	registerCoreNetworkingCLIFlags(opts)
}

func registerCoreNetworkingCLIFlags(opts *cliOptions) {
	pflag.StringArrayVar(&opts.externalEndpoints,
		"external-endpoints",
		[]string{},
		"External endpoints for nodes (format: nodeName=ip1,ip2,...)",
	)
	pflag.StringVar(&opts.externalEndpointsFile,
		"external-endpoints-file",
		"",
		"Path to JSON file containing external endpoints mapping",
	)
	pflag.BoolVar(&opts.useHostNetwork,
		"use-host-network",
		false,
		"Use hostNetwork for Caddy pods",
	)
	pflag.StringVar(&opts.caddyAdminOriginKey,
		"caddy-admin-origin-key",
		"",
		"Origin key for Caddy admin API security",
	)
	pflag.IntVar(&opts.httpHostPort,
		"http-host-port",
		defaultHTTPHostPort,
		"Host port for HTTP when using hostNetwork",
	)
	pflag.IntVar(&opts.httpsHostPort,
		"https-host-port",
		defaultHTTPSHostPort,
		"Host port for HTTPS when using hostNetwork",
	)
}

func registerExternalCLIFlags(opts *cliOptions) {
	pflag.BoolVar(&opts.externalEnable,
		"external-enable",
		false,
		"Enable external namespace ConfigMap aggregation",
	)
	pflag.StringVar(&opts.externalLabel,
		"external-label",
		"ckic.cmld.ru/aggregate=true",
		"Label selector for external ConfigMaps",
	)
	pflag.StringVar(&opts.externalNsMode,
		"external-ns-mode",
		"all",
		"Namespace mode: all, allow, or deny",
	)
	pflag.StringVar(&opts.externalAllowNamespaces,
		"external-allow-namespaces",
		"",
		"Comma-separated list of allowed namespaces (for allow mode)",
	)
	pflag.StringVar(&opts.externalDenyNamespaces,
		"external-deny-namespaces",
		"",
		"Comma-separated list of denied namespaces (for deny mode)",
	)
	pflag.BoolVar(&opts.externalPublishAggregated,
		"external-publish-aggregated",
		true,
		"Publish aggregated Caddyfile to a mirror ConfigMap",
	)
	pflag.StringVar(&opts.externalAggregatedConfigName,
		"external-aggregated-config-name",
		"ckic-caddy-config-working",
		"Name of the mirror ConfigMap for aggregated config",
	)
}

func registerLeaderElectionCLIFlags(opts *cliOptions) {
	pflag.BoolVar(&opts.leaderElectionEnabled,
		"leader-elect",
		true,
		"Enable leader election so only one manager instance reconciles resources",
	)
	pflag.BoolVar(&opts.readinessRequireLeader,
		"readiness-require-leader",
		false,
		"Report readiness only while leading (set true to keep legacy behavior)",
	)
	pflag.StringVar(&opts.leaderElectionLeaseName,
		"leader-election-lease-name",
		"ckic-manager-leader",
		"Name of the Lease resource used for leader election",
	)
	pflag.StringVar(
		&opts.leaderElectionLeaseNamespace,
		"leader-election-lease-namespace",
		"",
		"Namespace of the Lease resource used for leader election (defaults to --config-namespace)",
	)
	pflag.DurationVar(&opts.leaderElectionLeaseDuration,
		"leader-election-lease-duration",
		15*time.Second,
		"Duration non-leaders wait before forcing a leader election",
	)
	pflag.DurationVar(&opts.leaderElectionRenewDeadline,
		"leader-election-renew-deadline",
		10*time.Second,
		"Duration the acting leader retries refreshing leadership before giving up",
	)
	pflag.DurationVar(&opts.leaderElectionRetryPeriod,
		"leader-election-retry-period",
		2*time.Second,
		"Time between attempts by clients to acquire or renew leadership",
	)
}

func setupLogger(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	var output io.Writer = os.Stdout
	if os.Getenv("LOG_FORMAT") != "json" {
		output = zerolog.SyncWriter(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}
	log.Logger = log.Output(output). //nolint:reassign
						With().
						Str("service", "ckic-manager").
						Logger()
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
		log.Warn().
			Msgf("Unknown communication method %s, defaulting to clusterip", method)
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
	if !options.leaderElectionEnabled {
		return runControllerWithoutLeaderElection(ctx, ctrl, readiness)
	}
	readiness.Store(!options.readinessRequireLeader)
	defer readiness.Store(false)
	leaseNamespace := leaderElectionLeaseNamespace(options)
	identity := leaderElectionIdentity()
	lock := leaderElectionLock(clientset, options, leaseNamespace, identity)
	runErrCh := make(chan error, 1)
	controllerDoneCh := make(chan error, 1)
	controllerStarted := &atomic.Bool{}
	electionDoneCh := startLeaderElectionLoop(
		ctx,
		lock,
		options,
		identity,
		ctrl,
		readiness,
		options.readinessRequireLeader,
		runLeaderElection,
		runErrCh,
		controllerDoneCh,
		controllerStarted,
		leaseNamespace,
	)
	return waitForLeaderElectionResult(
		ctx,
		options,
		runErrCh,
		controllerDoneCh,
		controllerStarted,
		electionDoneCh,
	)
}

func runControllerWithoutLeaderElection(
	ctx context.Context,
	ctrl controllerRunner,
	readiness *atomic.Bool,
) error {
	readiness.Store(true)
	defer readiness.Store(false)
	return ctrl.Run(ctx)
}

func leaderElectionLeaseNamespace(options cliOptions) string {
	if options.leaderElectionLeaseNamespace != "" {
		return options.leaderElectionLeaseNamespace
	}
	return options.configMapNamespace
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

func startLeaderElectionLoop(
	ctx context.Context,
	lock resourcelock.Interface,
	options cliOptions,
	identity string,
	ctrl controllerRunner,
	readiness *atomic.Bool,
	readinessRequireLeader bool,
	runLeaderElection leaderElectionRunFunc,
	runErrCh chan<- error,
	controllerDoneCh chan<- error,
	controllerStarted *atomic.Bool,
	leaseNamespace string,
) <-chan struct{} {
	electionDoneCh := make(chan struct{})
	logLeaderElectionEnabled(options, leaseNamespace)
	go func() {
		defer close(electionDoneCh)
		runLeaderElection(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   options.leaderElectionLeaseDuration,
			RenewDeadline:   options.leaderElectionRenewDeadline,
			RetryPeriod:     options.leaderElectionRetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leadCtx context.Context) {
					onStartedLeading(
						leadCtx,
						identity,
						ctrl,
						readiness,
						readinessRequireLeader,
						runErrCh,
						controllerDoneCh,
						controllerStarted,
					)
				},
				OnStoppedLeading: func() {
					onStoppedLeading(ctx, readiness, readinessRequireLeader, runErrCh)
				},
				OnNewLeader: func(newLeaderIdentity string) {
					onNewLeader(identity, newLeaderIdentity)
				},
			},
		})
	}()
	return electionDoneCh
}

func logLeaderElectionEnabled(options cliOptions, leaseNamespace string) {
	log.Info().
		Str("leaseName", options.leaderElectionLeaseName).
		Str("leaseNamespace", leaseNamespace).
		Dur("leaseDuration", options.leaderElectionLeaseDuration).
		Dur("renewDeadline", options.leaderElectionRenewDeadline).
		Dur("retryPeriod", options.leaderElectionRetryPeriod).
		Bool("readinessRequireLeader", options.readinessRequireLeader).
		Msg("Leader election is enabled")
}

func onStartedLeading(
	leadCtx context.Context,
	identity string,
	ctrl controllerRunner,
	readiness *atomic.Bool,
	readinessRequireLeader bool,
	runErrCh chan<- error,
	controllerDoneCh chan<- error,
	controllerStarted *atomic.Bool,
) {
	log.Info().Str("identity", identity).Msg("Acquired leadership")
	controllerStarted.Store(true)
	if readinessRequireLeader {
		readiness.Store(true)
		defer readiness.Store(false)
	}
	runErr := ctrl.Run(leadCtx)
	if errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	reportRunResult(controllerDoneCh, runErr)
	reportRunResult(runErrCh, runErr)
}

func onStoppedLeading(
	ctx context.Context,
	readiness *atomic.Bool,
	readinessRequireLeader bool,
	runErrCh chan<- error,
) {
	if readinessRequireLeader {
		readiness.Store(false)
	}
	if ctx.Err() != nil {
		return
	}
	reportRunResult(runErrCh, errLeaderElectionLost)
}

func onNewLeader(identity, newLeaderIdentity string) {
	log.Info().
		Str("leaderIdentity", newLeaderIdentity).
		Bool("isLocalLeader", newLeaderIdentity == identity).
		Msg("Observed leader election update")
}

func reportRunResult(runErrCh chan<- error, runErr error) {
	select {
	case runErrCh <- runErr:
	default:
	}
}

func waitForLeaderElectionResult(
	ctx context.Context,
	options cliOptions,
	runErrCh <-chan error,
	controllerDoneCh <-chan error,
	controllerStarted *atomic.Bool,
	electionDoneCh <-chan struct{},
) error {
	for {
		select {
		case runErr := <-runErrCh:
			return handleRunResult(
				ctx,
				options,
				runErr,
				electionDoneCh,
				controllerDoneCh,
				controllerStarted,
			)
		case <-ctx.Done():
			return handleContextDone(
				options,
				runErrCh,
				electionDoneCh,
				controllerDoneCh,
				controllerStarted,
			)
		case <-electionDoneCh:
			return handleElectionDone(
				ctx,
				options,
				runErrCh,
				controllerDoneCh,
				controllerStarted,
			)
		}
	}
}

func handleRunResult(
	ctx context.Context,
	options cliOptions,
	runErr error,
	electionDoneCh <-chan struct{},
	controllerDoneCh <-chan error,
	controllerStarted *atomic.Bool,
) error {
	if runErr != nil {
		if errors.Is(runErr, errLeaderElectionLost) {
			waitForElectionShutdown(options, electionDoneCh)
			if controllerRunErr, hasControllerRunErr := waitForControllerShutdownResult(
				options,
				controllerStarted,
				controllerDoneCh,
			); hasControllerRunErr && controllerRunErr != nil {
				return controllerRunErr
			}
		}
		return runErr
	}
	waitForElectionShutdown(options, electionDoneCh)
	if isContextDone(ctx) {
		return nil
	}
	return errors.New("controller loop exited unexpectedly without context cancellation")
}

func handleContextDone(
	options cliOptions,
	runErrCh <-chan error,
	electionDoneCh <-chan struct{},
	controllerDoneCh <-chan error,
	controllerStarted *atomic.Bool,
) error {
	waitForElectionShutdown(options, electionDoneCh)
	if controllerRunErr, hasControllerRunErr := waitForControllerShutdownResult(
		options,
		controllerStarted,
		controllerDoneCh,
	); hasControllerRunErr {
		if controllerRunErr != nil {
			return controllerRunErr
		}
		return nil
	}
	if runErr, hasRunErr := readRunErrNonBlocking(runErrCh); hasRunErr && runErr != nil {
		return runErr
	}
	return nil
}

func handleElectionDone(
	ctx context.Context,
	options cliOptions,
	runErrCh <-chan error,
	controllerDoneCh <-chan error,
	controllerStarted *atomic.Bool,
) error {
	if runErr, hasRunErr := readRunErrNonBlocking(runErrCh); hasRunErr && runErr != nil {
		if errors.Is(runErr, errLeaderElectionLost) {
			if controllerRunErr, hasControllerRunErr := waitForControllerShutdownResult(
				options,
				controllerStarted,
				controllerDoneCh,
			); hasControllerRunErr && controllerRunErr != nil {
				return controllerRunErr
			}
		}
		return runErr
	}
	if isContextDone(ctx) {
		if controllerRunErr, hasControllerRunErr := waitForControllerShutdownResult(
			options,
			controllerStarted,
			controllerDoneCh,
		); hasControllerRunErr && controllerRunErr != nil {
			return controllerRunErr
		}
		return nil
	}
	return errors.New("leader election loop exited unexpectedly")
}

func readRunErrNonBlocking(runErrCh <-chan error) (error, bool) {
	select {
	case runErr := <-runErrCh:
		return runErr, true
	default:
		return nil, false
	}
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func waitForElectionShutdown(options cliOptions, electionDoneCh <-chan struct{}) {
	timeout := leaderElectionShutdownTimeout(
		options.leaderElectionRenewDeadline,
		options.leaderElectionRetryPeriod,
	)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-electionDoneCh:
	case <-timer.C:
		log.Warn().
			Dur("timeout", timeout).
			Msg("Timed out waiting for leader election shutdown; lease handoff may wait for expiration")
	}
}

func waitForControllerShutdownResult(
	options cliOptions,
	controllerStarted *atomic.Bool,
	controllerDoneCh <-chan error,
) (error, bool) {
	if controllerStarted == nil || !controllerStarted.Load() {
		return nil, false
	}
	timeout := leaderElectionShutdownTimeout(
		options.leaderElectionRenewDeadline,
		options.leaderElectionRetryPeriod,
	)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case runErr := <-controllerDoneCh:
		return runErr, true
	case <-timer.C:
		log.Warn().
			Dur("timeout", timeout).
			Msg("Timed out waiting for controller shutdown completion")
		return nil, false
	}
}

func leaderElectionShutdownTimeout(
	renewDeadline, retryPeriod time.Duration,
) time.Duration {
	timeout := renewDeadline + retryPeriod
	switch {
	case timeout < leaderElectionShutdownMinWait:
		return leaderElectionShutdownMinWait
	case timeout > leaderElectionShutdownMaxWait:
		return leaderElectionShutdownMaxWait
	default:
		return timeout
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
	//nolint:gosec
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			probeShutdownTimeout,
		)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
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
