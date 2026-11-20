package main

import (
	"fmt"
	"os"
	"strconv"

	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"thesis/controller"
	"thesis/types"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(traefikv3.AddToScheme(scheme))
}

func main() {
	log := ctrl.Log.WithName("custom-controller")

	env := os.Getenv("ENV")

	opts := zap.Options{
		Development: env == "dev",
	}

	ctrl.SetLogger(zap.New((zap.UseFlagOptions(&opts))))

	metricsOptions := server.Options{
		BindAddress: os.Getenv("METRICS_ADDR"),
	}

	options := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOptions,
		HealthProbeBindAddress: os.Getenv("HEALTH_PROBE_BIND_ADDRESS"),
	}

	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		log.Error(err, "could not create manager")
		os.Exit(1)
	}

	meshMetricsAddr := os.Getenv("METRICS_API_ADDRESS")
	log.Info("METRICS_API_ADDRESS", "value", meshMetricsAddr)

	prometheusMetricsAddr := os.Getenv("PROMETHEUS_ADDRESS")
	log.Info("PROMETHEUS_ADDRESS", "value", prometheusMetricsAddr)

	log.Info("METRICS_ADDRESS", "value", meshMetricsAddr)

	est, err := types.MapLatencyEstimationAlgorithm(os.Getenv("LATENCY_ESTIMATION_ALGORITHM"))

	if err != nil {
		log.Error(err, "Check the LATENCY_ESTIMATION_ALGORITHM value")
		os.Exit(1)
	}

	algo, err := types.MapAlgorithm(os.Getenv("AGGREGATION_ALGORITHM"))

	if err != nil {
		log.Error(err, "Check the AGGREGATION_ALGORITHM value")
		os.Exit(1)
	}

	function, err := types.MapForwardDecayFunction(os.Getenv("AGGREGATION_FUNCTION"))

	if err != nil {
		log.Error(err, "Check the AGGREGATION_FUNCTION value")
		os.Exit(1)
	}

	edParam, err := strconv.ParseFloat(os.Getenv("EXPONENTIAL_DECAY_PARAM"), 64)

	if err != nil {
		log.Error(err, "Check the EXPONENTIAL_DECAY_PARAM value")
		os.Exit(1)
	}

	pdParam, err := strconv.ParseFloat(os.Getenv("POLYNOMIAL_DECAY_PARAM"), 64)

	if err != nil {
		log.Error(err, "Check the POLYNOMIAL_DECAY_PARAM value")
		os.Exit(1)
	}

	sloThresholds, err := types.MapSloMetricThresholds(os.Getenv("SLOS"))
	if err != nil {
		log.Error(err, "Check the SLOs value")
		os.Exit(1)
	}

	failurePenalty, err := strconv.ParseFloat(os.Getenv("FAILURE_PENALTY"), 64)
	if err != nil {
		log.Error(err, "Check the FAILURE_PENALTY value")
		os.Exit(1)
	}

	l3FailurePenalty, err := strconv.ParseFloat(os.Getenv("L3_FAILURE_PENALTY"), 64)
	if err != nil {
		log.Error(err, "Check the L3_FAILURE_PENALTY value")
		os.Exit(1)
	}

	refreshPeriod, err := strconv.ParseFloat(os.Getenv("FD_REFRESH_PERIOD"), 64)
	if err != nil {
		log.Error(err, "Check the FD_REFRESH_PERIOD value")
		os.Exit(1)
	}

	config := types.Config{
		MetricsAddress:            meshMetricsAddr,
		PrometheusAddress:         prometheusMetricsAddr,
		MetricsTimeWindow:         os.Getenv("METRICS_TIME_WINDOW"),
		PrometheusTimeWindow:      os.Getenv("PROMETHEUS_TIME_WINDOW"),
		EstimationAlgorithm:       est,
		AggregationAlgorithm:      algo,
		AggregationFunction:       function,
		ExpDecayParam:             edParam,
		PolyDecayParam:            pdParam,
		EwmaDecayParam:            os.Getenv("EWMA_DECAY_PARAM"),
		AggregationTimeWindow:     os.Getenv("AGGREGATION_TIME_WINDOW"),
		SloMetricThresholds:       sloThresholds,
		FailurePenalty:            failurePenalty,
		L3FailurePenalty:          l3FailurePenalty,
		ForwardDecayRefreshPeriod: refreshPeriod,
		L3RateControllerEnabled:   os.Getenv("RATE_CONTROLLER_ENABLED") == "true",
	}

	log.Info("L3RateControllerEnabled", "value", config.L3RateControllerEnabled)

	appToManage := os.Getenv("APPLICATION_TO_MANAGE")
	if appToManage == "" {
		log.Error(fmt.Errorf("APPLICATION_TO_MANAGE not set"), "Check the APPLICATION_TO_MANAGE value")
		os.Exit(1)
	}

	testingEnv := os.Getenv("TESTING_ENVIRONMENT")
	if testingEnv == "" {
		log.Error(fmt.Errorf("TESTING_ENVIRONMENT not set"), "Check the TESTING_ENVIRONMENT value")
		os.Exit(1)
	}
	routeToManage := fmt.Sprintf("%s-%s-route", appToManage, testingEnv)
	log.Info("Route to manage", "route", routeToManage)

	// only reconcile print-response-backend-route for dev
	includeByName := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetName() == routeToManage
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetName() == routeToManage
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object.GetName() == routeToManage
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return e.Object.GetName() == routeToManage
		},
	}
	// Watches
	err = builder.
		ControllerManagedBy(manager).   // Create the Controller
		For(&traefikv3.IngressRoute{}). // IngressRoute is the Application API
		WithEventFilter((includeByName)).
		Complete(&controller.IngressRouteReconciler{Client: manager.GetClient(), Config: config})

	if err != nil {
		log.Error(err, "could not create controller")
		os.Exit(1)
	}

	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "could not start manager")
		os.Exit(1)
	}
}
