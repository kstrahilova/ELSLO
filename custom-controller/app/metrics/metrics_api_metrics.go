package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	aggregation "thesis/aggregation-algo"
	"thesis/types"
	"time"

	cl "thesis/metrics-api-client"

	viz "thesis/metrics-api-client/linkerd2/viz/metrics-api/gen/viz"

	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	metricsApiMetrics *MetricsApiMetrics
	mamOnce           sync.Once
)

var worstCaseStats = viz.BasicStats{
	SuccessCount:       0,
	FailureCount:       9999999,
	LatencyMsP50:       9999999,
	LatencyMsP95:       9999999,
	LatencyMsP99:       9999999,
	ActualSuccessCount: 0,
	ActualFailureCount: 9999999,
}

type MetricsApiMetrics struct {
	metricsApiClient  viz.ApiClient
	algo              aggregation.AggregationAlgorithm
	metrics           types.ReplicaMetricsList
	absoluteMetrics   types.AbsoluteReplicaMetricsList
	metricsTimeWindow string
}

func GetMetricsApiMetrics(ctx context.Context, config types.Config) (*MetricsApiMetrics, error) {
	mamOnce.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(emptyCtx)
		metricApiClient, err := cl.NewInternalClient(config.MetricsAddress)
		if err != nil {
			panic(1)
		}

		var algo aggregation.AggregationAlgorithm
		if types.ForwardDecay == config.AggregationAlgorithm {
			algo, err = aggregation.GetForwardDecay(ctx, config)
		} else if types.EWMA == config.AggregationAlgorithm {
			algo, err = aggregation.GetEWMA(ctx, config)
		} else {
			panic("Wrong aggregation algo value")
		}

		if err != nil {
			panic(err)
		}

		metricsApiMetrics = &MetricsApiMetrics{
			metricsApiClient:  metricApiClient,
			metrics:           types.ReplicaMetricsList{},
			absoluteMetrics:   types.AbsoluteReplicaMetricsList{},
			metricsTimeWindow: config.MetricsTimeWindow,
			algo:              algo,
		}
	})

	if metricsApiMetrics.algo == nil {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	return metricsApiMetrics, nil
}

func (metricsApiMetrics *MetricsApiMetrics) GetMetrics(ctx context.Context, weightedReplicas *types.WeightedReplicasList) (types.ReplicaMetricsList, types.AbsoluteReplicaMetricsList, error) {
	uptimeMu.Lock()
	defer uptimeMu.Unlock()

	for _, replica := range weightedReplicas.Items {

		// TODO: Can we get a timestamp from the metrics themselves instead?
		ti := time.Now()

		if !metricsApiMetrics.metrics.ReplicaMetricsExists(replica.Name, replica.Namespace) {
			log.Info("replicametrics doesn't exist, so we initialize")
			metricsApiMetrics.metrics.InitReplicaMetrics(replica.Name, replica.Namespace, false)
			log.Info("replicametrics was initialized")
		}

		if !metricsApiMetrics.absoluteMetrics.ReplicaMetricsExists(replica.Name, replica.Namespace) {
			log.Info("absolute replicametrics doesn't exist, so we initialize")
			metricsApiMetrics.absoluteMetrics.InitReplicaMetrics(replica.Name, replica.Namespace, false)
			log.Info("absolute replicametrics was initialized")
		}

		if !uptimeMetrics.UptimeMetricsExists(replica.Name, replica.Namespace) {
			log.Info("UptimeMetrics doesn't exist, so we initialize to false")
			uptimeMetrics.InitUptime(replica.Name, replica.Namespace)
		}

		stats, err := metricsApiMetrics.GetMetricsForDeployment(ctx, replica.Name, replica.Namespace)
		if err != nil {
			if !strings.Contains(err.Error(), "code = NotFound") {
				log.Error(err, "Error querying metrics-api")
				return nil, nil, err
			}
			// If no metrics are available for this replica, it is not available
			log.Info(replica.Name, "error", "There were no metrics found for this replica")
			uptimeMetrics.SetUptimeM(replica.Name, replica.Namespace, false)
		}

		replicaMetrics, err := metricsApiMetrics.metrics.GetReplicaMetricsByNameAndNamespace(replica.Name, replica.Namespace, log)
		if err != nil {
			return nil, nil, err
		}

		replicaAbsoluteMetrics, err := metricsApiMetrics.absoluteMetrics.GetReplicaMetricsByNameAndNamespace(replica.Name, replica.Namespace, log)
		if err != nil {
			return nil, nil, err
		}

		if stats != nil {
			uptimeMetrics.SetUptimeM(replica.Name, replica.Namespace, true)

			measuredMetrics, err := types.MapVizBasicStatsToMeasuredMetrics(stats)
			if err != nil {
				return nil, nil, err
			}

			for metric, value := range measuredMetrics {
				replicaAbsoluteMetrics[metric] = value
				err = metricsApiMetrics.algo.ComputeNewAverage(value, ti, replicaMetrics[metric])
				if err != nil {
					return nil, nil, err
				}
			}
		} else {
			uptimeMetrics.SetUptimeM(replica.Name, replica.Namespace, false)
		}
	}

	return metricsApiMetrics.metrics, metricsApiMetrics.absoluteMetrics, nil
}

func (metricsApiMetrics *MetricsApiMetrics) GetMetricsForDeployment(ctx context.Context, deployment string, namespace string) (*viz.BasicStats, error) {
	statSummaryRequest := viz.StatSummaryRequest{
		Selector: &viz.ResourceSelection{
			Resource: &viz.Resource{
				Type:      "service",
				Name:      deployment,
				Namespace: namespace,
			},
		},
		TimeWindow: metricsApiMetrics.metricsTimeWindow,
	}

	resp, err := metricsApiMetrics.metricsApiClient.StatSummary(ctx, &statSummaryRequest)

	if err != nil {
		if !strings.Contains(err.Error(), "code = NotFound") {
			log.Error(err, "Error querying metrics-api")
		}
		return nil, err
	}

	response := resp.GetOk().GetStatTables()

	for _, table := range response {
		for _, row := range table.GetPodGroup().GetRows() {
			log.Info(deployment, "metricsApiRow", row)
			// Is it possible to get more than one row?
			stats := row.GetStats()

			return stats, nil
		}
	}

	return nil, nil
}

func (metr *MetricsApiMetrics) RefreshValues() error {
	fd, ok := metr.algo.(*aggregation.ForwardDecay)
	// If we have a landmark and it is more than 20min ago
	if ok {
		for key, metrics := range metr.metrics {
			for metric, values := range metrics {
				log.Info(key.Name, "metric", metric, "oldValues", values)
				err := fd.RescaleRelativeToLandmark(values)

				if err != nil {
					return err
				}
				log.Info(key.Name, "metric", metric, "newValues", values)
			}
		}
	}

	return nil
}
