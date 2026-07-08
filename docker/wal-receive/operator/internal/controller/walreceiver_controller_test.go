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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

const testUbid = "pvda4whfnm2y2gp0tkt8e2w5rs"

func newReconciler() *WalReceiverReconciler {
	return &WalReceiverReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
}

func sampleWR(namespace string) *walgv1.WalReceiver {
	return &walgv1.WalReceiver{
		ObjectMeta: metav1.ObjectMeta{Name: "walg-recv-demo", Namespace: namespace},
		Spec: walgv1.WalReceiverSpec{
			PostgresUbid:         testUbid,
			TenantName:           "async-ha-demo",
			PrimaryTier:          "m8gd.4xlarge",
			CredentialsSecretRef: "walg-recv-demo-secrets",
			Primary: walgv1.PrimarySpec{
				Host:     "10.0.1.20",
				User:     "ubi_replication",
				SlotName: "walg_sync",
			},
		},
	}
}

// reconcileUntilStable drives Reconcile repeatedly (finalizer add, then
// object creation) until it returns without an error and without an
// immediate requeue request, returning the last result.
func reconcileUntilStable(r *WalReceiverReconciler, key types.NamespacedName) reconcile.Result {
	var res reconcile.Result
	var err error
	for i := 0; i < 5; i++ {
		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}
	return res
}

var _ = Describe("WalReceiver controller", func() {
	var (
		r   *WalReceiverReconciler
		ns  string
		key types.NamespacedName
		wr  *walgv1.WalReceiver
	)

	BeforeEach(func() {
		r = newReconciler()
		ns = "default"
		wr = sampleWR(ns)
		key = types.NamespacedName{Name: wr.Name, Namespace: ns}
		Expect(k8sClient.Create(ctx, wr)).To(Succeed())
		reconcileUntilStable(r, key)
	})

	AfterEach(func() {
		var got walgv1.WalReceiver
		if err := k8sClient.Get(ctx, key, &got); err == nil {
			Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
			// Drive teardown via the finalizer.
			for i := 0; i < 5; i++ {
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			}
		}
	})

	It("creates a ConfigMap with all expected env keys", func() {
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid + "-config", Namespace: ns,
		}, &cm)).To(Succeed())

		expectedKeys := []string{
			"WALG_PRIMARY_HOST", "WALG_PRIMARY_PORT", "WALG_PRIMARY_USER",
			"WALG_PRIMARY_DB", "WALG_APPLICATION_NAME", "WALG_SLOT_NAME",
			"WALG_TENANT_NAME", "WALG_WAL_RECEIVE_PARTIAL_DIR",
			"WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS", "WALG_LOG_LEVEL",
			// Option B control API + dr-catchup push (Control enabled by default).
			"WALG_WAL_RECEIVE_CONTROL_LISTEN", "WALG_WAL_RECEIVE_CONTROL_TLS_CERT",
			"WALG_WAL_RECEIVE_CONTROL_TLS_KEY", "WALG_WAL_RECEIVE_CONTROL_CLIENT_CA",
			"WALG_WAL_RECEIVE_PUSH_CLIENT_CERT", "WALG_WAL_RECEIVE_PUSH_CLIENT_KEY",
			"WALG_WAL_RECEIVE_PUSH_SERVER_CA",
		}
		Expect(cm.Data).To(HaveLen(len(expectedKeys)))
		for _, k := range expectedKeys {
			Expect(cm.Data).To(HaveKey(k))
		}
		Expect(cm.Data["WALG_PRIMARY_PORT"]).To(Equal("5432"))
		Expect(cm.Data["WALG_APPLICATION_NAME"]).To(Equal("walg_sync"))
	})

	It("creates a StatefulSet with correct selector and labels", func() {
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid, Namespace: ns,
		}, &sts)).To(Succeed())

		Expect(sts.Spec.Replicas).NotTo(BeNil())
		Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
		Expect(sts.Spec.ServiceName).To(Equal("walg-recv-" + testUbid))

		sel := sts.Spec.Selector.MatchLabels
		Expect(sel).To(HaveKeyWithValue("ubicloud.io/postgres-resource-ubid", testUbid))
		// Selector must be a subset of the pod template labels.
		for k, v := range sel {
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue(k, v))
		}

		c := sts.Spec.Template.Spec.Containers[0]
		Expect(c.EnvFrom[0].ConfigMapRef.Name).To(Equal("walg-recv-" + testUbid + "-config"))
		Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("walg.io/local-nvme", "true"))
		Expect(*sts.Spec.Template.Spec.SecurityContext.RunAsUser).To(Equal(int64(10001)))
		// hostPathNVMe default storage.
		var partials *corev1.Volume
		for i := range sts.Spec.Template.Spec.Volumes {
			if sts.Spec.Template.Spec.Volumes[i].Name == "partials" {
				partials = &sts.Spec.Template.Spec.Volumes[i]
			}
		}
		Expect(partials).NotTo(BeNil())
		Expect(partials.HostPath).NotTo(BeNil())
		Expect(partials.HostPath.Path).To(Equal("/mnt/nvme/walg-partials/" + testUbid))
	})

	It("adds a root chown initContainer that fixes partials-dir ownership", func() {
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid, Namespace: ns,
		}, &sts)).To(Succeed())

		ics := sts.Spec.Template.Spec.InitContainers
		Expect(ics).To(HaveLen(1))
		ic := ics[0]

		// Runs as root so it can chown the kubelet-created mount.
		Expect(ic.SecurityContext).NotTo(BeNil())
		Expect(ic.SecurityContext.RunAsUser).NotTo(BeNil())
		Expect(*ic.SecurityContext.RunAsUser).To(Equal(int64(0)))
		Expect(ic.SecurityContext.RunAsGroup).NotTo(BeNil())
		Expect(*ic.SecurityContext.RunAsGroup).To(Equal(int64(0)))

		// Chowns the mount to the receiver uid/gid (10001).
		Expect(ic.Command).To(HaveLen(3))
		Expect(ic.Command[0]).To(Equal("sh"))
		Expect(ic.Command[1]).To(Equal("-c"))
		Expect(ic.Command[2]).To(ContainSubstring("chown -R 10001:10001 /var/lib/walg/partials"))

		// Mounts the SAME partials volume at the same path the main
		// container uses, so the chown lands on the real mount.
		var mount *corev1.VolumeMount
		for i := range ic.VolumeMounts {
			if ic.VolumeMounts[i].Name == "partials" {
				mount = &ic.VolumeMounts[i]
			}
		}
		Expect(mount).NotTo(BeNil())
		Expect(mount.MountPath).To(Equal("/var/lib/walg/partials"))

		// Defaults to a busybox image when spec.initChownImage is empty.
		Expect(ic.Image).To(Equal("public.ecr.aws/docker/library/busybox:latest"))
	})

	It("propagates primary.host into the ConfigMap", func() {
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid + "-config", Namespace: ns,
		}, &cm)).To(Succeed())
		Expect(cm.Data["WALG_PRIMARY_HOST"]).To(Equal("10.0.1.20"))
	})

	It("is idempotent on unchanged spec (no churn)", func() {
		stsKey := types.NamespacedName{Name: "walg-recv-" + testUbid, Namespace: ns}
		var before appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, stsKey, &before)).To(Succeed())

		var cmBefore corev1.ConfigMap
		cmKey := types.NamespacedName{Name: "walg-recv-" + testUbid + "-config", Namespace: ns}
		Expect(k8sClient.Get(ctx, cmKey, &cmBefore)).To(Succeed())

		// Reconcile several more times; resource versions must not change.
		reconcileUntilStable(r, key)

		var after appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, stsKey, &after)).To(Succeed())
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))

		var cmAfter corev1.ConfigMap
		Expect(k8sClient.Get(ctx, cmKey, &cmAfter)).To(Succeed())
		Expect(cmAfter.ResourceVersion).To(Equal(cmBefore.ResourceVersion))
	})

	It("does NOT roll the pod on a primary.host change (live re-target via failover-primary), but updates the ConfigMap", func() {
		stsKey := types.NamespacedName{Name: "walg-recv-" + testUbid, Namespace: ns}
		var before appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, stsKey, &before)).To(Succeed())
		hashBefore := before.Spec.Template.Annotations[configHashAnnotation]
		Expect(hashBefore).NotTo(BeEmpty())

		// Simulate failover: control plane rewrites primary.host.
		var got walgv1.WalReceiver
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		got.Spec.Primary.Host = "10.0.9.99"
		Expect(k8sClient.Update(ctx, &got)).To(Succeed())

		reconcileUntilStable(r, key)

		// The roll-hash is UNCHANGED: primary.host is excluded from it so the pod
		// is not restarted (which would reset the receiver's fsync frontier to
		// 0/0). The running receiver re-targets via the failover-primary API.
		var after appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, stsKey, &after)).To(Succeed())
		hashAfter := after.Spec.Template.Annotations[configHashAnnotation]
		Expect(hashAfter).To(Equal(hashBefore))

		// The ConfigMap still tracks the new primary as the declarative cold-start
		// value (used on the next pod (re)start).
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid + "-config", Namespace: ns,
		}, &cm)).To(Succeed())
		Expect(cm.Data["WALG_PRIMARY_HOST"]).To(Equal("10.0.9.99"))
	})

	It("adds no zone nodeAffinity when placement.zone is empty", func() {
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid, Namespace: ns,
		}, &sts)).To(Succeed())
		// No zone set in sampleWR -> no affinity, but nodeSelector stays.
		Expect(sts.Spec.Template.Spec.Affinity).To(BeNil())
		Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("walg.io/local-nvme", "true"))
	})

	It("adds a zone nodeAffinity when placement.zone is set", func() {
		var got walgv1.WalReceiver
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		got.Spec.Placement.Zone = "us-west-2b"
		Expect(k8sClient.Update(ctx, &got)).To(Succeed())

		reconcileUntilStable(r, key)

		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid, Namespace: ns,
		}, &sts)).To(Succeed())

		// nodeSelector still present (both constraints must hold).
		Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("walg.io/local-nvme", "true"))

		Expect(sts.Spec.Template.Spec.Affinity).NotTo(BeNil())
		na := sts.Spec.Template.Spec.Affinity.NodeAffinity
		Expect(na).NotTo(BeNil())
		Expect(na.RequiredDuringSchedulingIgnoredDuringExecution).NotTo(BeNil())
		terms := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		Expect(terms).To(HaveLen(1))
		Expect(terms[0].MatchExpressions).To(HaveLen(1))
		expr := terms[0].MatchExpressions[0]
		Expect(expr.Key).To(Equal("topology.kubernetes.io/zone"))
		Expect(expr.Operator).To(Equal(corev1.NodeSelectorOpIn))
		Expect(expr.Values).To(Equal([]string{"us-west-2b"}))
	})

	It("sets owner references for cascade delete", func() {
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid, Namespace: ns,
		}, &sts)).To(Succeed())
		Expect(sts.OwnerReferences).To(HaveLen(1))
		Expect(sts.OwnerReferences[0].Kind).To(Equal("WalReceiver"))
		Expect(*sts.OwnerReferences[0].Controller).To(BeTrue())

		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: "walg-recv-" + testUbid + "-config", Namespace: ns,
		}, &cm)).To(Succeed())
		Expect(cm.OwnerReferences).To(HaveLen(1))
		Expect(cm.OwnerReferences[0].Kind).To(Equal("WalReceiver"))
	})

	It("tears down StatefulSet and ConfigMap via finalizer on delete", func() {
		var got walgv1.WalReceiver
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(got.Finalizers).To(ContainElement(finalizer))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
		for i := 0; i < 5; i++ {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}

		var sts appsv1.StatefulSet
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "walg-recv-" + testUbid, Namespace: ns}, &sts)
		Expect(apierrors.IsNotFound(err) || !sts.DeletionTimestamp.IsZero()).To(BeTrue())

		var cm corev1.ConfigMap
		err = k8sClient.Get(ctx, types.NamespacedName{Name: "walg-recv-" + testUbid + "-config", Namespace: ns}, &cm)
		Expect(apierrors.IsNotFound(err) || !cm.DeletionTimestamp.IsZero()).To(BeTrue())

		// CR finalizer removed -> CR gone.
		err = k8sClient.Get(ctx, key, &got)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})
})

var _ = Describe("sizing", func() {
	It("applies the flat default envelope regardless of primaryTier", func() {
		for _, tier := range []string{"m8gd.large", "m8gd.16xlarge", "totally-bogus", ""} {
			wr := sampleWR("default")
			wr.Spec.PrimaryTier = tier
			res := resourcesForSpec(wr)
			Expect(res.Requests.Cpu().String()).To(Equal("100m"))
			Expect(res.Limits.Cpu().String()).To(Equal("2"))
			Expect(res.Requests.Memory().String()).To(Equal("256Mi"))
			Expect(res.Limits.Memory().String()).To(Equal("512Mi"))
		}
	})

	It("lets resourcesOverride win over the flat default", func() {
		wr := sampleWR("default")
		wr.Spec.PrimaryTier = "m8gd.large"
		wr.Spec.ResourcesOverride = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("750m")},
		}
		res := resourcesForSpec(wr)
		Expect(res.Requests.Cpu().String()).To(Equal("750m"))
	})
})
