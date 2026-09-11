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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"git.horse/vapronva/ckic/pkg/controller"
	"git.horse/vapronva/ckic/pkg/utils"
)

const (
	probeShutdownTimeout       = 5 * time.Second
	probeReadHeaderTimeout     = 10 * time.Second
	leaderRunErrTimeout        = 30 * time.Second
	leaderElectionJitterFactor = 1.2
)

var errLeaderElectionLost = errors.New("leader election lost")

type options struct {
	kubeconfigPath    string
	logLevel          string
	healthBindAddress string
	loadBalancerMode  string
	imagePullPolicy   string
	externalEndpoints []string
	leaderElection    leaderElectionOptions
	controller        controller.Config
}

type leaderElectionOptions struct {
	enabled       bool
	leaseName     string
	leaseDuration time.Duration
	renewDeadline time.Duration
	retryPeriod   time.Duration
}

func main() {
	opts := parseFlags()
	if err := setupLogger(opts.logLevel); err != nil {
		log.Fatal().Err(err).Msg("Invalid log level")
	}
	cfg, err := opts.resolveControllerConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid configuration")
	}
	if err := validateLeaderElectionTimings(opts.leaderElection); err != nil {
		log.Fatal().Err(err).Msg("Invalid leader election configuration")
	}
	clientset, err := utils.GetKubernetesClient(opts.kubeconfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to build Kubernetes client")
	}
	ctrl, err := controller.NewController(clientset, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize controller")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	readiness := &atomic.Bool{}
	probeErrCh := startHealthProbeServer(ctx, opts.healthBindAddress, readiness)
	log.Info().Msg("Starting CKIC manager")
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- runWithLeaderElection(ctx, clientset, opts.leaderElection, cfg.Namespace, ctrl, readiness)
	}()
	var runErr error
	select {
	case runErr = <-runErrCh:
	case runErr = <-probeErrCh:
		cancel()
		<-runErrCh
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error().Err(runErr).Msg("Controller exited with error")
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	cfg := &opts.controller
	deploy := &cfg.Deploy
	le := &opts.leaderElection
	pflag.StringVar(&opts.kubeconfigPath, "kubeconfig", "", "Path to kubeconfig file")
	pflag.StringVar(&cfg.NodeLabel, "node-label", "ckic.cmld.ru/enabled=true", "Kubernetes label selector used to choose managed nodes (empty for all nodes)")
	pflag.StringVar(&cfg.ConfigMapName, "config-map", "caddy-config", "ConfigMap containing Caddy configuration")
	pflag.StringVar(&cfg.Namespace, "namespace", "", "Namespace for managed Caddy resources, ConfigMaps and leases (defaults to CKIC_NAMESPACE or the in-cluster service account namespace)")
	pflag.DurationVar(&cfg.ConfigResyncInterval, "config-resync-interval", 0, "Periodically re-push the merged Caddyfile to all instances even when unchanged (0 disables; e.g. 5m)")
	pflag.StringVar(&opts.healthBindAddress, "health-bind-address", ":8081", "Address where health and readiness probes are served (set empty to disable)")
	pflag.StringVar(&opts.logLevel, "log-level", "info", "Log level (trace, debug, info, warn, error, fatal, panic, disabled)")
	pflag.StringVar(&deploy.CaddyImage, "caddy-image", "docker.horse/oss-images/zerossl-caddy/caddy:2.11.4-alpine", "Caddy image (format image:tag)")
	pflag.StringVar(&opts.imagePullPolicy, "image-pull-policy", "IfNotPresent", "ImagePullPolicy for deployed Caddy pods (Always, IfNotPresent, Never)")
	pflag.BoolVar(&deploy.PrePullImage, "prepull-image", true, "Pre-pull the Caddy image on a node before creating or updating its Deployment")
	pflag.StringVar(&opts.loadBalancerMode, "loadbalancer-mode", "none", "LoadBalancer strategy: none, or cilium (one LB per node)")
	pflag.StringVar(&deploy.EnvSecretName, "env-secret", "", "Name of the Kubernetes Secret to use for environment variables")
	pflag.StringSliceVar(&deploy.EnvSecretKeys, "env-keys", nil, "Keys from the Secret to use as environment variables")
	pflag.StringVar(&deploy.DataVolumePVC, "data-pvc", "", "Name of PVC to use for the /data volume (defaults to HostPath if empty)")
	pflag.StringVar(&deploy.ConfigVolumePVC, "config-pvc", "", "Name of PVC to use for the /config volume (defaults to HostPath if empty)")
	pflag.StringArrayVar(&opts.externalEndpoints, "external-endpoints", nil, "External endpoints for nodes (format: nodeName=ip1,ip2,...)")
	pflag.BoolVar(&deploy.UseHostNetwork, "use-host-network", false, "Use hostNetwork for Caddy pods")
	pflag.StringVar(&deploy.CaddyAdminOriginKey, "caddy-admin-origin-key", "", "Origin check for the Caddy admin API (not authentication)")
	pflag.BoolVar(&cfg.ExternalEnable, "external-enable", false, "Enable external namespace ConfigMap aggregation")
	pflag.StringVar(&cfg.ExternalLabel, "external-label", "ckic.cmld.ru/aggregate=true", "Label selector for external ConfigMaps")
	pflag.StringSliceVar(&cfg.ExternalAllowNamespaces, "external-allow-namespaces", nil, "Only aggregate ConfigMaps from these namespaces (empty allows all)")
	pflag.StringSliceVar(&cfg.ExternalDenyNamespaces, "external-deny-namespaces", nil, "Never aggregate ConfigMaps from these namespaces")
	pflag.StringVar(&cfg.ExternalAggregatedConfigName, "external-aggregated-config-name", "ckic-caddy-config-working", "Name of the ConfigMap holding the last accepted boot Caddyfile")
	pflag.BoolVar(&le.enabled, "leader-elect", true, "Enable leader election so only one manager instance reconciles resources")
	pflag.StringVar(&le.leaseName, "leader-election-lease-name", "ckic-manager-leader", "Name of the Lease resource used for leader election")
	pflag.DurationVar(&le.leaseDuration, "leader-election-lease-duration", 15*time.Second, "Duration non-leaders wait before forcing a leader election")
	pflag.DurationVar(&le.renewDeadline, "leader-election-renew-deadline", 10*time.Second, "Duration the acting leader retries refreshing leadership before giving up")
	pflag.DurationVar(&le.retryPeriod, "leader-election-retry-period", 2*time.Second, "Time between attempts by clients to acquire or renew leadership")
	pflag.Parse()
	return opts
}

func (o options) resolveControllerConfig() (controller.Config, error) {
	cfg := o.controller
	switch o.loadBalancerMode {
	case "none":
	case "cilium":
		cfg.Deploy.EnableCiliumLB = true
	default:
		return cfg, fmt.Errorf("invalid loadbalancer mode %q (want none or cilium)", o.loadBalancerMode)
	}
	switch policy := corev1.PullPolicy(o.imagePullPolicy); policy {
	case corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
		cfg.Deploy.ImagePullPolicy = policy
	default:
		return cfg, fmt.Errorf("invalid image pull policy %q (want Always, IfNotPresent, or Never)", o.imagePullPolicy)
	}
	endpoints, err := utils.ParseExternalEndpoints(o.externalEndpoints)
	if err != nil {
		return cfg, err
	}
	cfg.ExternalEndpoints = endpoints
	cfg.Namespace, err = resolveNamespace(cfg.Namespace)
	return cfg, err
}

func setupLogger(level string) error {
	parsedLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level %q: %w", level, err)
	}
	if parsedLevel == zerolog.NoLevel {
		return fmt.Errorf("invalid log level %q", level)
	}
	zerolog.SetGlobalLevel(parsedLevel)
	var output io.Writer = os.Stdout
	if os.Getenv("LOG_FORMAT") != "json" {
		output = zerolog.SyncWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	}
	//nolint:reassign // zerolog, configuring global logger
	log.Logger = log.Output(output).With().Str("service", "ckic-manager").Logger()
	return nil
}

func validateLeaderElectionTimings(le leaderElectionOptions) error {
	if !le.enabled {
		return nil
	}
	if le.leaseDuration <= 0 || le.renewDeadline <= 0 || le.retryPeriod <= 0 {
		return errors.New("leader election durations must all be positive")
	}
	if le.leaseDuration <= le.renewDeadline {
		return fmt.Errorf(
			"leader-election-lease-duration (%s) must be greater than leader-election-renew-deadline (%s)",
			le.leaseDuration, le.renewDeadline,
		)
	}
	if minRenew := time.Duration(leaderElectionJitterFactor * float64(le.retryPeriod)); le.renewDeadline <= minRenew {
		return fmt.Errorf(
			"leader-election-renew-deadline (%s) must be greater than %s (%.1f × leader-election-retry-period)",
			le.renewDeadline, minRenew, leaderElectionJitterFactor,
		)
	}
	return nil
}

func runWithLeaderElection(
	ctx context.Context,
	clientset kubernetes.Interface,
	le leaderElectionOptions,
	namespace string,
	ctrl *controller.Controller,
	readiness *atomic.Bool,
) error {
	readiness.Store(true)
	defer readiness.Store(false)
	if !le.enabled {
		return ctrl.Run(ctx)
	}
	identity := leaderElectionIdentity()
	log.Info().
		Str("leaseName", le.leaseName).
		Str("leaseNamespace", namespace).
		Dur("leaseDuration", le.leaseDuration).
		Dur("renewDeadline", le.renewDeadline).
		Dur("retryPeriod", le.retryPeriod).
		Msg("Leader election is enabled")
	electionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var leading atomic.Bool
	runErrCh := make(chan error, 1)
	leaderelection.RunOrDie(electionCtx, leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Name: le.leaseName, Namespace: namespace},
			Client:     clientset.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
		},
		ReleaseOnCancel: false,
		LeaseDuration:   le.leaseDuration,
		RenewDeadline:   le.renewDeadline,
		RetryPeriod:     le.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				leading.Store(true)
				log.Info().Str("identity", identity).Msg("Acquired leadership")
				runErrCh <- ctrl.Run(leadCtx)
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
	if !leading.Load() {
		return nil
	}
	var runErr error
	select {
	case runErr = <-runErrCh:
	case <-time.After(leaderRunErrTimeout):
		log.Warn().Msg("Controller did not stop in time after leadership ended")
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	if ctx.Err() != nil {
		return nil
	}
	return errLeaderElectionLost
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
	return "", errors.New("namespace not set: provide --namespace, CKIC_NAMESPACE, or run in-cluster")
}

func leaderElectionIdentity() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "ckic-manager"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func startHealthProbeServer(ctx context.Context, bindAddress string, readiness *atomic.Bool) <-chan error {
	errCh := make(chan error, 1)
	if bindAddress == "" {
		return errCh
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !readiness.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready")
	})
	server := &http.Server{Addr: bindAddress, Handler: mux, ReadHeaderTimeout: probeReadHeaderTimeout}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn().Err(err).Msg("Probe server shutdown returned error")
		}
	}()
	go func() {
		log.Info().Str("bindAddress", bindAddress).Msg("Starting probe server")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("probe server failed: %w", err)
		}
	}()
	return errCh
}
