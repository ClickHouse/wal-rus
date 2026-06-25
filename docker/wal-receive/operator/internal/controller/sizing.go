package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

// flatDefaultResources is the baseline resource envelope applied to every
// receiver pod regardless of spec.primaryTier. The per-tier table was
// retired in favor of this flat baseline plus the resourcesOverride escape
// hatch: receivers are disk-bound, not CPU-bound (~11 fit on an m7gd.large),
// so a small request with room to burst is sufficient. See DESIGN §6.
//
//	CPU:    request 100m (10% of a vCPU), limit 2 (burst/auto-grow)
//	Memory: request 256Mi, limit 512Mi
var flatDefaultResources = rp{"100m", "2", "256Mi", "512Mi"}

// rp is one resource envelope: CPU/memory request and limit.
type rp struct {
	cpuReq, cpuLim, memReq, memLim string
}

// resourcesForSpec returns the container resources for a WalReceiver. An
// explicit spec.resourcesOverride wins; otherwise every receiver gets the
// flat default envelope. Sizing no longer depends on primaryTier.
func resourcesForSpec(wr *walgv1.WalReceiver) corev1.ResourceRequirements {
	if wr.Spec.ResourcesOverride != nil {
		return *wr.Spec.ResourcesOverride
	}
	row := flatDefaultResources
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(row.cpuReq),
			corev1.ResourceMemory: resource.MustParse(row.memReq),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(row.cpuLim),
			corev1.ResourceMemory: resource.MustParse(row.memLim),
		},
	}
}
