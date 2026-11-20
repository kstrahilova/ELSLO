package types

import (
	"fmt"

	"github.com/go-logr/logr"
	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type WeightedReplicasList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []traefikv3.LoadBalancerSpec `json:"items"`
}

func (weightedReplicas *WeightedReplicasList) GetWeightedReplicaByNameAndNamespace(name string, namespace string, port intstr.IntOrString, log logr.Logger) (*traefikv3.LoadBalancerSpec, error) {
	for _, b := range weightedReplicas.Items {
		if b.Name == name && b.Namespace == namespace && b.Port == port {
			return &b, nil
		}
	}

	return nil, fmt.Errorf("the weighted replica %s in the %s namespace is not in the list of weightedReplicas, so it could not be returned", name, namespace)
}

func (weightedReplicas *WeightedReplicasList) GetWeightedReplicaByNameAndNamespaceFromWeightedReplica(weightedReplica *traefikv3.LoadBalancerSpec, log logr.Logger) (*traefikv3.LoadBalancerSpec, error) {
	return weightedReplicas.GetWeightedReplicaByNameAndNamespace(weightedReplica.Name, weightedReplica.Namespace, weightedReplica.Port, log)
}

func (weightedReplicas *WeightedReplicasList) AddWeightedReplica(weightedReplica traefikv3.LoadBalancerSpec, log logr.Logger) {
	if !weightedReplicas.WeightedReplicaExists(&weightedReplica) {
		weightedReplicas.Items = append(weightedReplicas.Items, weightedReplica)
	} else {
	}
}

func (weightedReplicas *WeightedReplicasList) RemoveWeightedReplica(weightedReplica *traefikv3.LoadBalancerSpec) {
	for i, b := range weightedReplicas.Items {
		if b.Name == weightedReplica.Name && b.Namespace == weightedReplica.Namespace && b.Port == weightedReplica.Port {
			weightedReplicas.Items = append(weightedReplicas.Items[:i], weightedReplicas.Items[i+1:]...)
			break
		}
	}
}

func (weightedReplicas *WeightedReplicasList) UpdateWeightedReplicaWeight(weightedReplica *traefikv3.LoadBalancerSpec) {
	for i, b := range weightedReplicas.Items {
		if b.Name == weightedReplica.Name && b.Namespace == weightedReplica.Namespace && b.Port == weightedReplica.Port {
			weightedReplicas.Items[i].Weight = weightedReplica.Weight
			break
		}
	}
}

func (weightedReplicas *WeightedReplicasList) GetWeightedReplicaWeight(weightedReplica *traefikv3.LoadBalancerSpec) (int, error) {
	for _, b := range weightedReplicas.Items {
		if b.Name == weightedReplica.Name && b.Namespace == weightedReplica.Namespace && b.Port == weightedReplica.Port {
			return *weightedReplica.Weight, nil
		}
	}

	return -1, fmt.Errorf("the weighted replica %s in the %s namespace is not in the list of weightedReplicas, so its weight could not be returned", weightedReplica.Name, weightedReplica.Namespace)
}

func (weightedReplicas *WeightedReplicasList) WeightedReplicaExists(weightedReplica *traefikv3.LoadBalancerSpec) bool {
	for _, b := range weightedReplicas.Items {
		if b.Name == weightedReplica.Name && b.Namespace == weightedReplica.Namespace && b.Port == weightedReplica.Port {
			return true
		}
	}

	return false
}
