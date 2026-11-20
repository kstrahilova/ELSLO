package latency_estimation

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"thesis/metrics"
	"thesis/types"

	aggregation "thesis/aggregation-algo"

	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	elsEstimator *ELSEstimator
	elsOnce      sync.Once
)

type ELSEstimator struct {
	mu                  sync.Mutex
	prometheusMetrics   metrics.PrometheusMetrics
	uptimeMetrics       metrics.ReplicaUptimeMetrics
	reachabilityMetrics metrics.ReachabilityMetrics
	config              types.Config
	algo                aggregation.AggregationAlgorithm
	lastSum             float64
}

func GetELSEstimator(inputCtx context.Context, config types.Config) (*ELSEstimator, error) {
	var (
		promMetr  *metrics.PrometheusMetrics
		reachMetr *metrics.ReachabilityMetrics
		err       error
	)

	promMetr, err = metrics.GetPrometheusMetrics(inputCtx, config)

	if err != nil {
		return nil, err
	}

	reachMetr, err = metrics.GetReachabilityMetrics(inputCtx, config)

	if err != nil {
		return nil, err
	}

	var algo aggregation.AggregationAlgorithm
	if types.ForwardDecay == config.AggregationAlgorithm {
		algo, err = aggregation.GetForwardDecay(ctx, config)
	} else if types.EWMA == config.AggregationAlgorithm {
		algo, err = aggregation.GetEWMA(ctx, config)
	} else {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	if err != nil {
		return nil, err
	}

	elsOnce.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(ctx)
		elsEstimator = &ELSEstimator{
			prometheusMetrics:   *promMetr,
			uptimeMetrics:       metrics.GetUptimeMetrics(),
			reachabilityMetrics: *reachMetr,
			config:              config,
			algo:                algo,
			lastSum:             100.0,
		}
		ctx = inputCtx
	})

	if elsEstimator.algo == nil {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	return elsEstimator, nil
}

func (estimator *ELSEstimator) Estimate(weightedReplica *traefikv3.LoadBalancerSpec) (*traefikv3.LoadBalancerSpec, error) {
	return weightedReplica, nil
}

func (estimator *ELSEstimator) EstimateAll(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	log.Info("Estimating with the Els Estimator")
	err := estimator.refreshLandmark()
	if err != nil {
		return nil, err
	}
	return estimator.getLatencyBasedWeights(weightedReplicas)
}

func (estimator *ELSEstimator) refreshLandmark() error {
	now := time.Now().UTC()

	fd, ok := estimator.algo.(*aggregation.ForwardDecay)
	// If we have a landmark and it is more than 20min ago
	if ok && now.Sub(fd.GetLandmark()) > fd.GetRefreshPeriod() {
		log.Info("Will refresh landmark", "old", fd.GetPreviousLandmark(), "current", fd.GetLandmark())
		fd.RefreshLandmark()
		log.Info("Refreshed landmark", "previous", fd.GetPreviousLandmark(), "current", fd.GetLandmark())
		err := estimator.prometheusMetrics.RefreshValues()
		if err != nil {
			return err
		}
	}

	return nil
}

func (estimator *ELSEstimator) getLatencyBasedWeights(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	var processedReplicas []traefikv3.LoadBalancerSpec

	err := estimator.reachabilityMetrics.GetMetrics(ctx, weightedReplicas)

	if err != nil {
		return nil, err
	}
	log.Info("Reachability metrics were gotten")

	_, _, prometheusMetricsMap, successPrometheusMetricsMap, failedPrometheusMetricsMap, err := estimator.prometheusMetrics.GetMetrics(ctx, weightedReplicas)
	if err != nil {
		return nil, err
	}
	log.Info("Prometheus metrics were gotten")

	absWeights := make(map[types.Key]float64)
	sum := 0.0
	negativeWeights := false
	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}

		log.Info(key.Name, "R", estimator.uptimeMetrics.GetUptimeR(key.Name, key.Namespace), "P", estimator.uptimeMetrics.GetUptimeP(key.Name, key.Namespace))

		// if  not accessible at all
		if !estimator.uptimeMetrics[key].R {
			log.Info("Replica is not accessible at all, setting absolute weight to 0")
			absWeights[key] = 0
			continue
		}
		log.Info("Replica is accessible, proceeding with weight calculation")

		var absWeight float64 = 0
		log.Info("Calculating absolute weight based on Prometheus metrics")
		prometheusMetrics := prometheusMetricsMap[key]
		successPrometheusMetrics := successPrometheusMetricsMap[key]
		log.Info("Retrieved Prometheus metrics for replica")
		failedPrometheusMetrics := failedPrometheusMetricsMap[key]
		log.Info("Retrieved failed Prometheus metrics for replica")

		// P99 latency of successful requests
		l := successPrometheusMetrics[types.LatencyMsP99].Value
		log.Info(weightedReplica.Name, "latency", l)
		// P99 latency of failed requests
		f := failedPrometheusMetrics[types.LatencyMsP99].Value
		log.Info(weightedReplica.Name, "failed latency", f)
		rps := successPrometheusMetrics[types.SuccessCount].Value + successPrometheusMetrics[types.FailureCount].Value
		log.Info(weightedReplica.Name, "rps", rps)
		// successRate
		s := successPrometheusMetrics[types.SuccessCount].Value / rps
		if rps < 1 {
			s = 1
		}
		log.Info(weightedReplica.Name, "success rate", s)

		// expected arrival time
		// L := l + (f+800)/(1/s-1)
		L := l + f*(1/s-1)
		log.Info(weightedReplica.Name, "expected arrival time", L, "l", l, "f", f, "f+800", f+800, "s", s, "s-1", s-1, "(1/s-1)", 1/s-1, "(f+800)/(1/s-1)", (f+800)/(1/s-1), "l + (f+800)/(1/s-1)", l+(f+800)/(1/s-1), "f/(1/s-1)", f/(1/s-1), "l + f/(1/s-1)", l+f/(1/s-1))

		// number of in-flight requests
		q := prometheusMetrics[types.InFlightRequestCount].Value

		// expected latency
		E := L * (1 + q)
		log.Info(weightedReplica.Name, "expected latency", E)

		// absolute weight based on expected latency and in-flight requests
		absWeight = 1 / E
		log.Info(weightedReplica.Name, "absolute weight", absWeight)

		sum += absWeight
		absWeights[key] = absWeight
		if absWeight < 0 {
			negativeWeights = true
		}
		log.Info(weightedReplica.Name, "absWeights[key]", absWeights[key])
	}

	estimator.lastSum = sum

	if negativeWeights {
		log.Info("Warning: Negative weights detected")
	}
	normalizedWeights := NormalizeTo100(absWeights)
	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}
		ratio := (absWeights[key] / sum) * 100
		var weight int
		if negativeWeights {
			weight = int(math.Round(absWeights[key]))
		} else {
			weight = normalizedWeights[key]
		}
		normalizedRatio := (float64(normalizedWeights[key]) / sum) * 100

		log.Info(weightedReplica.Name, "expLatency", 1/absWeights[key], "1/expLatency", absWeights[key], "sum", sum, "ratio", ratio, "normalizedRatio", normalizedRatio, "weight", weight)
		log.Info(weightedReplica.Name, "weight", weight)

		nativeLB := true
		var newService traefikv3.LoadBalancerSpec = traefikv3.LoadBalancerSpec{
			Kind:      weightedReplica.Kind,
			Name:      weightedReplica.Name,
			Namespace: weightedReplica.Namespace,
			Port:      weightedReplica.Port,
			Weight:    &weight,
			NativeLB:  &nativeLB,
		}
		processedReplicas = append(processedReplicas, newService)

	}

	weightedReplicas.Items = processedReplicas

	return weightedReplicas, nil
}
