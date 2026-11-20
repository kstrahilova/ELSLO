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
	lEstimator *ELSLOEstimator
	lOnce      sync.Once
)

type ELSLOEstimator struct {
	mu                  sync.Mutex
	prometheusMetrics   metrics.PrometheusMetrics
	uptimeMetrics       metrics.ReplicaUptimeMetrics
	reachabilityMetrics metrics.ReachabilityMetrics
	config              types.Config
	algo                aggregation.AggregationAlgorithm
	lastSum             float64
}

func GetELSLOEstimator(inputCtx context.Context, config types.Config) (*ELSLOEstimator, error) {
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

	lOnce.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(ctx)
		lEstimator = &ELSLOEstimator{
			prometheusMetrics:   *promMetr,
			uptimeMetrics:       metrics.GetUptimeMetrics(),
			reachabilityMetrics: *reachMetr,
			config:              config,
			algo:                algo,
			lastSum:             100.0,
		}
		ctx = inputCtx
	})

	if lEstimator.algo == nil {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	return lEstimator, nil
}

func (estimator *ELSLOEstimator) Estimate(weightedReplica *traefikv3.LoadBalancerSpec) (*traefikv3.LoadBalancerSpec, error) {
	return weightedReplica, nil
}

func (estimator *ELSLOEstimator) EstimateAll(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	log.Info("Estimating with the Latency Estimator")
	err := estimator.refreshLandmark()
	if err != nil {
		return nil, err
	}

	return estimator.getLatencyBasedWeights(weightedReplicas)
}

func (estimator *ELSLOEstimator) refreshLandmark() error {
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

func (estimator *ELSLOEstimator) getLatencyBasedWeights(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	var processedReplicas []traefikv3.LoadBalancerSpec

	err := estimator.reachabilityMetrics.GetMetrics(ctx, weightedReplicas)

	if err != nil {
		return nil, err
	}

	code200SloViolations, non200SloViolations, prometheusMetricsMap, successPrometheusMetricsMap, failedPrometheusMetricsMap, err := estimator.prometheusMetrics.GetMetrics(ctx, weightedReplicas)
	if err != nil {
		return nil, err
	}

	absWeights := make(map[types.Key]float64)
	sum := 0.0
	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}

		log.Info(key.Name, "R", estimator.uptimeMetrics.GetUptimeR(key.Name, key.Namespace), "M", estimator.uptimeMetrics.GetUptimeM(key.Name, key.Namespace), "P", estimator.uptimeMetrics.GetUptimeP(key.Name, key.Namespace))

		// if not accessible at all
		if !estimator.uptimeMetrics[key].R {
			absWeights[key] = 0
			continue
		}

		var absWeight float64 = 0

		successPrometheusMetrics := successPrometheusMetricsMap[key]
		failedPrometheusMetrics := failedPrometheusMetricsMap[key]
		prometheusMetrics := prometheusMetricsMap[key]

		totalCount := successPrometheusMetrics[types.SuccessCount].Value + successPrometheusMetrics[types.FailureCount].Value
		log.Info(weightedReplica.Name, "totalCount", totalCount)
		successRate := successPrometheusMetrics[types.SuccessCount].Value / totalCount
		// prevent division by numbers close to zero
		if totalCount < 1 {
			successRate = 1
		}
		log.Info(weightedReplica.Name, "successRate", successRate)
		log.Info(weightedReplica.Name, "failedP99", failedPrometheusMetrics[types.LatencyMsP99].Value)
		log.Info(weightedReplica.Name, "non200SloViolations", non200SloViolations[key][types.Latency].Value)
		log.Info(weightedReplica.Name, "successP99", successPrometheusMetrics[types.LatencyMsP99].Value)
		log.Info(weightedReplica.Name, "code200SloViolations", code200SloViolations[key][types.Latency].Value)

		var inFlightRatio = 0.0
		if totalCount != 0.0 {
			inFlightRatio = prometheusMetrics[types.InFlightRequestCount].Value
		}
		log.Info(weightedReplica.Name, "inFlightRatio", inFlightRatio)

		penalty := failedPrometheusMetrics[types.LatencyMsP99].Value + non200SloViolations[key][types.Latency].Value

		if penalty == math.Inf(1) {
			penalty = estimator.config.FailurePenalty
		}
		log.Info(weightedReplica.Name, "penalty", penalty)
		expectedLatency := successPrometheusMetrics[types.LatencyMsP99].Value + code200SloViolations[key][types.Latency].Value + penalty*(1/successRate-1) //*math.Pow(inFlightRatio+1, 2)
		log.Info(weightedReplica.Name, "expectedLatency", expectedLatency)

		absWeight = 1 / expectedLatency
		log.Info(weightedReplica.Name, "absWeight", absWeight)
		sum += absWeight
		absWeights[key] = absWeight
		log.Info(weightedReplica.Name, "absWeights[key]", absWeights[key])
	}

	estimator.lastSum = sum

	normalizedWeights := NormalizeTo100(absWeights)
	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}
		ratio := (absWeights[key] / sum) * 100
		var weight = normalizedWeights[key]

		log.Info(weightedReplica.Name, "expLatency", 1/absWeights[key], "1/expLatency", absWeights[key], "sum", sum, "ratio", ratio, "weight", weight)
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
