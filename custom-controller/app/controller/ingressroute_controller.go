package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	latency_estimation "thesis/latency-estimation"
	"thesis/types"

	"github.com/go-logr/logr"
	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	weightedReplicas types.WeightedReplicasList
	mutex            sync.Mutex
	log              logr.Logger
	emptyCtx         context.Context
)

type IngressRouteReconciler struct {
	client.Client
	Config types.Config
	Scheme *runtime.Scheme
}

func (a *IngressRouteReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {

	// Get logger
	log = ctrl.LoggerFrom(emptyCtx)

	ir := &traefikv3.IngressRoute{}
	// Get retrieves an obj for the given object key from the Kubernetes Cluster
	err := a.Get(ctx, req.NamespacedName, ir)
	if err != nil {
		return reconcile.Result{}, err
	}

	mutex.Lock()
	defer mutex.Unlock()

	log.Info("mutex is locked for ingressroute")
	// sync weighted replicas
	log.Info("Syncing weights for replicas ...")
	err = syncWeightedReplicas(*ir)
	if err != nil {
		return reconcile.Result{}, err
	}

	// update weighted replicas weights
	log.Info("Updating weights for replicas ...")

	var estimator latency_estimation.Estimator
	switch a.Config.EstimationAlgorithm {
	case types.ELSLO:
		var err error
		estimator, err = latency_estimation.GetELSLOEstimator(ctx, a.Config)
		if err != nil {
			return reconcile.Result{}, err
		}
	case types.L3:
		var err error
		estimator, err = latency_estimation.GetL3Estimator(ctx, a.Config)
		if err != nil {
			return reconcile.Result{}, err
		}
	case types.ELS:
		var err error
		estimator, err = latency_estimation.GetELSEstimator(ctx, a.Config)
		if err != nil {
			return reconcile.Result{}, err
		}
	default:
		return reconcile.Result{}, fmt.Errorf("Unknown estimation algorithm")
	}

	weightedReplicasPointer, err := updateWeights(estimator)
	weightedReplicas = *weightedReplicasPointer

	if err != nil {
		return reconcile.Result{}, err
	}

	var replicas []traefikv3.Service

	for _, weightedReplica := range weightedReplicas.Items {
		replicas = append(replicas, traefikv3.Service{LoadBalancerSpec: weightedReplica})
	}

	requeueAfter := 10 * time.Second

	if GetWeightSum(replicas) < 90 && a.Config.EstimationAlgorithm != types.L3 {
		log.Info("The sum of the weights is lower han 90, so we are probably not receiving any metrics from both Prometheus and the metrics API.")
		log.Info("Skipping reconciliation of the IngressRoute")
		return reconcile.Result{RequeueAfter: requeueAfter * 5}, nil
	}

	log.Info("Reconcile replicas")
	ir.Spec.Routes[0].Services = replicas
	ir.Labels["weightsComputedAt"] = time.Now().UTC().Format("2006-01-02T15-04-05Z")

	// Update updates the given obj in the Kubernetes cluster.
	// obj must be a struct pointer so that obj can be updated with the content returned by the Server.
	err = a.Update(ctx, ir)

	if err != nil {
		return reconcile.Result{RequeueAfter: requeueAfter * 5}, err
	}

	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func syncWeightedReplicas(ingressRoute traefikv3.IngressRoute) error {
	// NOTE: We are assuming that the IngressRoutes only have one route for now, containing multiple Services (replicas)
	for _, route := range ingressRoute.Spec.Routes {

		// New services list needs to take the services that are in the Ingress route, but with the weights they had before
		var newWeightedReplicas types.WeightedReplicasList = types.WeightedReplicasList{}
		// Loop over new services (replicas) in the route
		for _, replica := range route.Services {
			// Add it to the new replicas list as is in the repo (non-historic weight)
			newWeightedReplicas.AddWeightedReplica(replica.LoadBalancerSpec, log)
			var oldReplicaObject, err = weightedReplicas.GetWeightedReplicaByNameAndNamespaceFromWeightedReplica(&replica.LoadBalancerSpec, log)
			// If the error is nil, update the service with its old weight
			if err != nil {
				log.Info(err.Error())
				continue
			}

			newWeightedReplicas.UpdateWeightedReplicaWeight(oldReplicaObject)
		}
		// remove unsed ones
		weightedReplicas = newWeightedReplicas
	}
	return nil
}

func updateWeights(estimator latency_estimation.Estimator) (*types.WeightedReplicasList, error) {
	output, err := estimator.EstimateAll(&weightedReplicas)
	return output, err
}

func GetWeightSum(replicas []traefikv3.Service) int {

	sum := 0
	for _, values := range replicas {
		sum += *values.Weight
	}

	return sum
}
