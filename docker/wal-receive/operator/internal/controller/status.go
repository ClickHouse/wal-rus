package controller

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

// Phase constants for WalReceiverStatus.Phase.
const (
	PhasePending     = "Pending"
	PhaseReconciling = "Reconciling"
	PhaseRunning     = "Running"
	PhaseDegraded    = "Degraded"
	PhaseTerminating = "Terminating"
)

// Condition types (standard K8s patterns).
const (
	condReady       = "Ready"
	condProgressing = "Progressing"
	condDegraded    = "Degraded"
)

// crashLoopGrace is how long a pod may be CrashLoopBackOff before we
// flip the phase to Degraded.
const crashLoopGrace = 3 * time.Minute

// updateStatus reads the managed StatefulSet/pod, derives a phase and
// conditions, and writes them back to the WalReceiver status subresource.
func (r *WalReceiverReconciler) updateStatus(ctx context.Context, wr *walgv1.WalReceiver) (ctrl.Result, error) {
	phase, podName, degradedReason := r.observePhase(ctx, wr)

	wr.Status.Phase = phase
	wr.Status.PodName = podName
	wr.Status.ControlEndpoint = r.observeControlEndpoint(ctx, wr)
	wr.Status.ObservedGeneration = wr.Generation

	switch phase {
	case PhaseRunning:
		setCondition(wr, condReady, metav1.ConditionTrue, "PodReady", "Receiver pod is Ready")
		setCondition(wr, condProgressing, metav1.ConditionFalse, "Stable", "Receiver is stable")
	case PhaseReconciling:
		setCondition(wr, condReady, metav1.ConditionFalse, "Rolling", "Receiver pod is rolling")
		setCondition(wr, condProgressing, metav1.ConditionTrue, "Rolling", "Receiver pod is rolling out a new spec")
	default: // Pending
		setCondition(wr, condReady, metav1.ConditionFalse, "PodNotReady", "Receiver pod is not Ready yet")
		setCondition(wr, condProgressing, metav1.ConditionTrue, "Starting", "Receiver pod is starting")
	}

	if phase == PhaseDegraded {
		setCondition(wr, condDegraded, metav1.ConditionTrue, "PodUnhealthy", degradedReason)
	} else {
		setCondition(wr, condDegraded, metav1.ConditionFalse, "Healthy", "No degraded conditions")
	}

	if err := r.Status().Update(ctx, wr); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue while not yet Running so transient pod states converge.
	if phase != PhaseRunning {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// observePhase inspects the StatefulSet and its pod to derive a phase.
func (r *WalReceiverReconciler) observePhase(ctx context.Context, wr *walgv1.WalReceiver) (phase, podName, degradedReason string) {
	var sts appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Name: stsName(wr), Namespace: wr.Namespace}, &sts)
	if apierrors.IsNotFound(err) {
		return PhasePending, "", ""
	}
	if err != nil {
		return PhasePending, "", ""
	}

	podName = stsName(wr) + "-0"

	// If the STS is mid-rollout (updated revision not yet fully ready),
	// report Reconciling.
	if sts.Status.UpdatedReplicas < *replicasOrOne(&sts) || sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		// Still surface Degraded if the pod is crash-looping below.
		phase = PhaseReconciling
	}

	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: wr.Namespace}, &pod); err != nil {
		if phase == "" {
			return PhasePending, podName, ""
		}
		return phase, podName, ""
	}

	if reason, degraded := crashLooping(&pod); degraded {
		return PhaseDegraded, podName, reason
	}

	if podReady(&pod) {
		if phase == PhaseReconciling {
			return PhaseReconciling, podName, ""
		}
		return PhaseRunning, podName, ""
	}

	if phase == PhaseReconciling {
		return PhaseReconciling, podName, ""
	}
	return PhasePending, podName, ""
}

// observeControlEndpoint reports the reachable address of the control API's
// LoadBalancer Service (Option B), or "" until it has an address / when the
// control API is disabled.
func (r *WalReceiverReconciler) observeControlEndpoint(ctx context.Context, wr *walgv1.WalReceiver) string {
	if wr.Spec.Control.Disabled {
		return ""
	}
	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: controlServiceName(wr), Namespace: wr.Namespace}, &svc); err != nil {
		return ""
	}
	return controlEndpointFromService(&svc)
}

func replicasOrOne(sts *appsv1.StatefulSet) *int32 {
	if sts.Spec.Replicas != nil {
		return sts.Spec.Replicas
	}
	one := int32(1)
	return &one
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// crashLooping returns a reason and true if the pod has a container in
// CrashLoopBackOff for longer than crashLoopGrace.
func crashLooping(pod *corev1.Pod) (string, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		w := cs.State.Waiting
		if w == nil || w.Reason != "CrashLoopBackOff" {
			continue
		}
		// Use the last terminated time as the start of the crash loop.
		if t := cs.LastTerminationState.Terminated; t != nil {
			if time.Since(t.FinishedAt.Time) < crashLoopGrace {
				return "", false
			}
		}
		msg := w.Reason
		if w.Message != "" {
			msg = w.Reason + ": " + w.Message
		}
		return msg, true
	}
	return "", false
}

func setCondition(wr *walgv1.WalReceiver, condType string, status metav1.ConditionStatus, reason, message string) {
	cond := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: wr.Generation,
	}
	for i := range wr.Status.Conditions {
		if wr.Status.Conditions[i].Type == condType {
			if wr.Status.Conditions[i].Status == status &&
				wr.Status.Conditions[i].Reason == reason &&
				wr.Status.Conditions[i].Message == message {
				// No change other than generation; keep transition time.
				wr.Status.Conditions[i].ObservedGeneration = wr.Generation
				return
			}
			cond.LastTransitionTime = metav1.Now()
			wr.Status.Conditions[i] = cond
			return
		}
	}
	cond.LastTransitionTime = metav1.Now()
	wr.Status.Conditions = append(wr.Status.Conditions, cond)
}
