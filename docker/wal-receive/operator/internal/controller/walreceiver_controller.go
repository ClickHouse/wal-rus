/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

const finalizer = "walg.io/wal-receiver-cleanup"

// WalReceiverReconciler reconciles a WalReceiver object.
type WalReceiverReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=walg.io,resources=walreceivers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=walg.io,resources=walreceivers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=walg.io,resources=walreceivers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// Reconcile turns a WalReceiver into a ConfigMap + StatefulSet, reacting
// to spec changes via a config-hash pod annotation and reporting status.
func (r *WalReceiverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var wr walgv1.WalReceiver
	if err := r.Get(ctx, req.NamespacedName, &wr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// --- finalizer / teardown (§8) ---
	if !wr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &wr)
	}
	if !controllerutil.ContainsFinalizer(&wr, finalizer) {
		controllerutil.AddFinalizer(&wr, finalizer)
		if err := r.Update(ctx, &wr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// --- desired downstream objects ---
	cm := r.desiredConfigMap(&wr)
	configHash := hashEnv(cm.Data)
	sts := r.desiredStatefulSet(&wr, configHash)

	for _, obj := range []client.Object{cm, sts} {
		if err := controllerutil.SetControllerReference(&wr, obj, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.applyConfigMap(ctx, cm); err != nil {
		l.Error(err, "applying ConfigMap")
		return ctrl.Result{}, err
	}
	if err := r.applyStatefulSet(ctx, sts); err != nil {
		l.Error(err, "applying StatefulSet")
		return ctrl.Result{}, err
	}

	// --- Option B control API LoadBalancer (nil when disabled) ---
	if svc := r.desiredControlService(&wr); svc != nil {
		if err := controllerutil.SetControllerReference(&wr, svc, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.applyService(ctx, svc); err != nil {
			l.Error(err, "applying control Service")
			return ctrl.Result{}, err
		}
	}

	// --- status (§7) ---
	return r.updateStatus(ctx, &wr)
}

// applyConfigMap create-or-updates the ConfigMap, writing only when the
// data or controlled metadata differ (no churn on identical spec).
func (r *WalReceiverReconciler) applyConfigMap(ctx context.Context, desired *corev1.ConfigMap) error {
	var existing corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if mapsEqual(existing.Data, desired.Data) &&
		mapsEqual(existing.Labels, desired.Labels) &&
		hasOwner(existing.OwnerReferences, desired.OwnerReferences) {
		return nil
	}
	existing.Data = desired.Data
	existing.Labels = desired.Labels
	existing.OwnerReferences = desired.OwnerReferences
	return r.Update(ctx, &existing)
}

// applyStatefulSet create-or-updates the StatefulSet. It updates only the
// mutable parts of the spec (template, replicas) so an unchanged spec
// produces no write and no rolling restart.
func (r *WalReceiverReconciler) applyStatefulSet(ctx context.Context, desired *appsv1.StatefulSet) error {
	var existing appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if statefulSetEqual(&existing, desired) {
		return nil
	}
	// Selector and serviceName are immutable; keep the existing ones and
	// only patch the mutable template/replicas plus controlled metadata.
	existing.Labels = desired.Labels
	existing.OwnerReferences = desired.OwnerReferences
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template = desired.Spec.Template
	return r.Update(ctx, &existing)
}

// applyService create-or-updates the control LoadBalancer Service, patching
// only the mutable spec bits so cluster-assigned fields (clusterIP, the
// provisioned LB) and an unchanged spec produce no write.
func (r *WalReceiverReconciler) applyService(ctx context.Context, desired *corev1.Service) error {
	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if existing.Spec.Type == desired.Spec.Type &&
		mapsEqual(existing.Spec.Selector, desired.Spec.Selector) &&
		mapsEqual(existing.Annotations, desired.Annotations) &&
		mapsEqual(existing.Labels, desired.Labels) &&
		servicePortsEqual(existing.Spec.Ports, desired.Spec.Ports) {
		return nil
	}
	existing.Labels = desired.Labels
	existing.Annotations = desired.Annotations
	existing.OwnerReferences = desired.OwnerReferences
	existing.Spec.Type = desired.Spec.Type
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.ExternalTrafficPolicy = desired.Spec.ExternalTrafficPolicy
	return r.Update(ctx, &existing)
}

// reconcileDelete performs ordered teardown: StatefulSet, then ConfigMap;
// the Secret is externally managed and left alone. OwnerRefs would
// cascade anyway, but the finalizer guarantees ordering and surfaces a
// reason if teardown wedges.
func (r *WalReceiverReconciler) reconcileDelete(ctx context.Context, wr *walgv1.WalReceiver) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(wr, finalizer) {
		return ctrl.Result{}, nil
	}

	wr.Status.Phase = PhaseTerminating
	_ = r.Status().Update(ctx, wr)

	// 1) StatefulSet (grace lets wal-g close its replication slot).
	if err := r.deleteIfExists(ctx, &appsv1.StatefulSet{}, stsName(wr), wr.Namespace); err != nil {
		return ctrl.Result{}, err
	}
	// 2) ConfigMap.
	if err := r.deleteIfExists(ctx, &corev1.ConfigMap{}, configMapName(wr), wr.Namespace); err != nil {
		return ctrl.Result{}, err
	}
	// 3) Control Service (frees the LoadBalancer).
	if err := r.deleteIfExists(ctx, &corev1.Service{}, controlServiceName(wr), wr.Namespace); err != nil {
		return ctrl.Result{}, err
	}
	// 4) Secret intentionally left in place.

	controllerutil.RemoveFinalizer(wr, finalizer)
	if err := r.Update(ctx, wr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *WalReceiverReconciler) deleteIfExists(ctx context.Context, obj client.Object, name, namespace string) error {
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !obj.GetDeletionTimestamp().IsZero() {
		// Already terminating.
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, obj))
}

// SetupWithManager wires the controller to watch WalReceivers and the
// downstream ConfigMap + StatefulSet it owns.
func (r *WalReceiverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&walgv1.WalReceiver{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
