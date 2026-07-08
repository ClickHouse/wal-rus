package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// hasOwner reports whether every desired owner reference is already
// present on the existing object (by UID).
func hasOwner(existing, desired []metav1.OwnerReference) bool {
	for _, d := range desired {
		found := false
		for _, e := range existing {
			if e.UID == d.UID && e.Controller != nil && d.Controller != nil && *e.Controller == *d.Controller {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// statefulSetEqual reports whether the mutable, operator-managed parts of
// two StatefulSets are equal: the pod template (including the config-hash
// annotation and resources), replicas, labels, and owner refs. The
// immutable selector/serviceName are not compared.
func statefulSetEqual(existing, desired *appsv1.StatefulSet) bool {
	if !mapsEqual(existing.Labels, desired.Labels) {
		return false
	}
	if !hasOwner(existing.OwnerReferences, desired.OwnerReferences) {
		return false
	}
	if (existing.Spec.Replicas == nil) != (desired.Spec.Replicas == nil) {
		return false
	}
	if existing.Spec.Replicas != nil && desired.Spec.Replicas != nil &&
		*existing.Spec.Replicas != *desired.Spec.Replicas {
		return false
	}
	return equality.Semantic.DeepEqual(existing.Spec.Template, desired.Spec.Template)
}
