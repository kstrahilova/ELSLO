package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"thesis/types"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

type ReachabilityMetrics struct{}

var (
	rOnce               sync.Once
	reachabilityMetrics *ReachabilityMetrics
)

func (reachabilityMetrics *ReachabilityMetrics) CheckServiceReachability(deployment string, namespace string) bool {
	client := &http.Client{
		Timeout: time.Second,
	}

	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8081/health", deployment, namespace)
	log = ctrl.LoggerFrom(emptyCtx)
	resp, err := client.Get(url)
	if err != nil {
		log.Info("Checked service reachability", "url", url, "reachable", false)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Info("Checked service reachability", "url", url, "reachable", false)
		return false
	}

	log.Info("Checked service reachability", "url", url, "reachable", true)
	return true
}

func (reachabilityMetrics *ReachabilityMetrics) GetMetrics(ctx context.Context, weightedReplicas *types.WeightedReplicasList) error {
	uptimeMu.Lock()
	defer uptimeMu.Unlock()

	for _, replica := range weightedReplicas.Items {

		if !uptimeMetrics.UptimeMetricsExists(replica.Name, replica.Namespace) {
			log.Info("UptimeMetrics doesn't exist, so we initialize to false")
			uptimeMetrics.InitUptime(replica.Name, replica.Namespace)
		}

		reachable := reachabilityMetrics.CheckServiceReachability(replica.Name, replica.Namespace)
		uptimeMetrics.SetUptimeR(replica.Name, replica.Namespace, reachable)
	}

	return nil
}

func GetReachabilityMetrics(ctx context.Context, config types.Config) (*ReachabilityMetrics, error) {
	rOnce.Do(func() {
		log = ctrl.LoggerFrom(emptyCtx)
		reachabilityMetrics = &ReachabilityMetrics{}
	})

	return reachabilityMetrics, nil
}
