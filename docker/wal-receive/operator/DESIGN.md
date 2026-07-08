# wal-receive operator — design

Companion to `PLAN.md` (which is the phased build schedule). This doc
describes **what the code looks like**: the CRD types, the reconciler
shape, the objects it produces, and the decisions baked into each.

The operator's one job: turn a `WalReceiver` custom resource into a
running receiver pod (`wal-g wal-receive`) plus its ConfigMap, mounting
an externally-managed Secret. Ubicloud (or any caller) writes the CR;
the operator owns everything downstream.

```
            writes CR                  reconciles
  Ubicloud ───────────▶  WalReceiver  ───────────▶  ConfigMap
  control            (walg.io/v1)                    StatefulSet ──▶ Pod (wal-g wal-receive)
  plane                                              (mounts Secret, hostPath NVMe)
                       Secret  ◀── Ubicloud creates it directly (mTLS + SSH key)
```

---

## 1. Scope & non-goals

**In scope (operator owns):**
- Reconcile `WalReceiver` → ConfigMap + StatefulSet.
- Pick container resources from a static per-primary-tier table.
- Restart the pod when env-affecting spec fields change (failover ⇒
  new `primary.host`).
- Report status (`Pending`/`Running`/`Degraded`) + conditions.
- Clean teardown via a finalizer (STS + ConfigMap deleted, Secret left).

**Out of scope (someone else owns):**
- **Secret creation** — Ubicloud issues the mTLS cert/key + standby SSH
  key and `kubectl create secret`s it. The operator only references it
  via `spec.credentialsSecretRef` and mounts it. Rationale in
  `walg-operator-decisions` memory: secret issuance stays with whoever
  owns the PKI.
- **Replication slot creation** on the primary — control-plane concern.
- **Failover orchestration** — the control plane decides who's primary
  and rewrites the CR; the operator only reacts to the spec change.
- **Autoscaling** — sizing is a static lookup, not HPA/VPA.

---

## 2. Project layout (kubebuilder v4)

Scaffolded with `kubebuilder init --domain walg.io --repo
github.com/<fork>/wal-g/docker/wal-receive/operator` then
`kubebuilder create api --group walg --version v1 --kind WalReceiver`.

```
docker/wal-receive/operator/
  go.mod                      # own module, sibling to the wal-g build
  PROJECT
  Makefile                    # manifests, generate, test, build, docker-build
  cmd/main.go                 # manager entrypoint
  api/v1/
    walreceiver_types.go      # Spec + Status (§3)
    groupversion_info.go
    zz_generated.deepcopy.go  # generated
  internal/controller/
    walreceiver_controller.go # Reconcile (§4)
    configmap.go              # builds the ConfigMap (§5)
    statefulset.go            # builds the StatefulSet (§5)
    sizing.go                 # per-tier resource table (§6)
    status.go                 # phase/conditions (§7)
    suite_test.go             # envtest bootstrap
    walreceiver_controller_test.go
  config/                     # CRD, RBAC, manager Deployment, samples (kustomize)
```

Its own `go.mod` keeps controller-runtime's dependency tree out of the
main wal-g build, and vice-versa.

---

## 3. CRD types — `api/v1/walreceiver_types.go`

The Spec is a typed mirror of the parameter contract in
`k8s/configmap.template.yaml` + `k8s/statefulset.template.yaml`, so the
control plane sets fields instead of rendering YAML.

```go
package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// WalReceiverSpec is the desired state of one per-tenant receiver.
type WalReceiverSpec struct {
	// PostgresUbid is the Ubicloud Postgres resource UBID. It pins
	// every derived object name: walg-recv-<ubid>, ...-config, etc.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]{26}$`
	PostgresUbid string `json:"postgresUbid"`

	// TenantName is human-readable; used in logs and push labels.
	TenantName string `json:"tenantName"`

	Primary PrimarySpec `json:"primary"`
	Standby StandbySpec `json:"standby"`

	// PrimaryTier selects the resource sizing row (§6). Unknown tier
	// ⇒ defaults + a Degraded condition rather than a hard failure.
	// +kubebuilder:validation:Enum=m8gd.large;m8gd.4xlarge;m8gd.8xlarge;m8gd.16xlarge
	PrimaryTier string `json:"primaryTier"`

	// CredentialsSecretRef names the externally-managed Secret holding
	// client.crt/client.key/server-ca.crt + the standby SSH key.
	// The operator MOUNTS it; it never creates or mutates it.
	CredentialsSecretRef string `json:"credentialsSecretRef"`

	// Image is the wal-receive image. Operator fills a default if empty.
	// +optional
	Image string `json:"image,omitempty"`
	// +kubebuilder:default=IfNotPresent
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Storage selects where the WAL partial dir lives (§5.3).
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// ResourcesOverride is an escape hatch that wins over the tier table.
	// +optional
	ResourcesOverride *corev1.ResourceRequirements `json:"resourcesOverride,omitempty"`
}

type PrimarySpec struct {
	Host string `json:"host"`
	// +kubebuilder:default=5432
	Port int32 `json:"port,omitempty"`
	// e.g. "ubi_replication"
	User string `json:"user"`
	// Physical replication slot the receiver attaches to, e.g. "walg_sync".
	SlotName string `json:"slotName"`
	// +kubebuilder:default=walg_sync
	ApplicationName string `json:"applicationName,omitempty"`
}

type StandbySpec struct {
	// SSH target for the primary-loss tail push (design §4 Option 4c).
	SSHHost   string `json:"sshHost"`
	SSHUser   string `json:"sshUser"`
	TargetDir string `json:"targetDir"` // e.g. /var/lib/postgresql/walg-tail
}

type StorageSpec struct {
	// Mode: "hostPathNVMe" (PoC EKS, /mnt/nvme), "emptyDir", or "pvc".
	// +kubebuilder:default=hostPathNVMe
	Mode string `json:"mode,omitempty"`
	// +kubebuilder:default="20Gi"
	SizeLimit string `json:"sizeLimit,omitempty"`
	// HostPathBase is the node mount for hostPathNVMe mode.
	// +kubebuilder:default="/mnt/nvme/walg-partials"
	HostPathBase string `json:"hostPathBase,omitempty"`
}

// WalReceiverStatus is the observed state.
type WalReceiverStatus struct {
	// +kubebuilder:validation:Enum=Pending;Reconciling;Running;Degraded;Terminating
	Phase string `json:"phase,omitempty"`
	PodName string `json:"podName,omitempty"`
	// LastFsyncLSN, populated if the pod exposes it; best-effort.
	LastFsyncLSN string `json:"lastFsyncLSN,omitempty"`
	// ObservedGeneration lets us tell a stale status from a fresh one.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Primary,type=string,JSONPath=`.spec.primary.host`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`
type WalReceiver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec   WalReceiverSpec   `json:"spec,omitempty"`
	Status WalReceiverStatus `json:"status,omitempty"`
}
```

`PostgresUbid` is the single source of deterministic naming — the
control plane can address `walg-recv-<ubid>` without listing by labels.

---

## 4. Reconciler — `internal/controller/walreceiver_controller.go`

Standard controller-runtime reconcile loop. Idempotent: it computes the
desired ConfigMap + StatefulSet and `CreateOrUpdate`s them; unchanged
specs produce no writes (no rolling restart).

```go
func (r *WalReceiverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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
		return ctrl.Result{}, r.Update(ctx, &wr)
	}

	// --- desired downstream objects ---
	cm := r.desiredConfigMap(&wr)              // §5.1
	sts, sizingDegraded := r.desiredStatefulSet(&wr) // §5.2 + §6

	for _, obj := range []client.Object{cm, sts} {
		// OwnerRef so CR delete cascades; SetControllerReference also
		// lets us adopt on restart.
		if err := controllerutil.SetControllerReference(&wr, obj, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.applyConfigMap(ctx, cm); err != nil {        // create-or-update
		return ctrl.Result{}, err
	}
	if err := r.applyStatefulSet(ctx, sts); err != nil {     // create-or-update
		return ctrl.Result{}, err
	}

	// --- status (§7) ---
	return r.updateStatus(ctx, &wr, sizingDegraded)
}

func (r *WalReceiverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&walgv1.WalReceiver{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.StatefulSet{}).   // pod-state changes re-trigger Reconcile
		Complete(r)
}
```

**Why `CreateOrUpdate` and not server-side apply for the PoC:** simpler
diffing semantics, and the test matrix in PLAN phase 2 asserts "no churn
on identical spec," which `CreateOrUpdate`'s mutate-fn pattern gives
directly.

### 4.1 Failover reaction (no special code path)

Design §7: on promotion the control plane rewrites `spec.primary.host`
(and possibly `spec.primary.slotName`). Because the ConfigMap is derived
purely from spec, the changed `WALG_PRIMARY_HOST` makes the ConfigMap
content differ, so `applyConfigMap` patches it. The StatefulSet's pod
template carries a **config hash annotation** (sha256 of the rendered
env), so changing the ConfigMap rolls the pod:

```go
sts.Spec.Template.Annotations["walg.io/config-hash"] = hashEnv(cm.Data)
```

That single annotation is the whole failover mechanism on the operator
side — change primary ⇒ env hash changes ⇒ StatefulSet rolls the pod ⇒
`wal-g wal-receive` reconnects to the new primary's `IDENTIFY_SYSTEM`
and picks up the new timeline. Inert edits (labels, annotations) don't
touch the env hash, so they don't restart the pod.

---

## 5. Downstream object builders

### 5.1 ConfigMap (`configmap.go`)

One-to-one with `k8s/configmap.template.yaml`. Pure function of spec:

```go
func (r *WalReceiverReconciler) desiredConfigMap(wr *walgv1.WalReceiver) *corev1.ConfigMap {
	s := wr.Spec
	return &corev1.ConfigMap{
		ObjectMeta: meta(wr, name(wr)+"-config"),
		Data: map[string]string{
			"WALG_PRIMARY_HOST":      s.Primary.Host,
			"WALG_PRIMARY_PORT":      itoa(orDefault(s.Primary.Port, 5432)),
			"WALG_PRIMARY_USER":      s.Primary.User,
			"WALG_PRIMARY_DB":        "postgres",
			"WALG_APPLICATION_NAME":  orDefault(s.Primary.ApplicationName, "walg_sync"),
			"WALG_SLOT_NAME":         s.Primary.SlotName,
			"WALG_TENANT_NAME":       s.TenantName,
			"WALG_STANDBY_SSH_HOST":   s.Standby.SSHHost,
			"WALG_STANDBY_SSH_USER":   s.Standby.SSHUser,
			"WALG_STANDBY_TARGET_DIR": s.Standby.TargetDir,
			"WALG_REMOTE_WALG_PATH":   "/usr/bin/wal-g",
			"WALG_PARTIAL_DIR":        "/var/lib/walg/partials",
			// tuning — image-baked defaults, overridable later
			"WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS": "30",
			"WALG_LOG_LEVEL":                            "NORMAL",
		},
	}
}
```

### 5.2 StatefulSet (`statefulset.go`)

Mirror of `k8s/statefulset.template.yaml`: 1 replica, headless service
name, `envFrom` the ConfigMap, mounts the Secret read-only at
`/etc/walg/tls` + `/etc/walg/ssh`, partials volume per §5.3, resources
from §6, the NVMe `nodeSelector`, the liveness probe on partial-dir
freshness, and `securityContext` uid/gid 10001.

### 5.3 Partial-dir storage — the NVMe decision

The receiver fsyncs WAL to local NVMe; that IOPS profile is the entire
point (`doc/walg-sync-standby-stress-test-results.md`). Three modes via
`spec.storage.mode`:

| mode | volume | when |
|---|---|---|
| `hostPathNVMe` (PoC default) | `hostPath: /mnt/nvme/walg-partials/<ubid>` | our EKS PoC nodegroup formats+mounts the instance store at `/mnt/nvme` (see `eks-walg-poc.yaml` preBootstrapCommands); a hostPath there is an explicit guarantee the partials land on NVMe |
| `emptyDir` | `emptyDir{}` on instance-store-backed kubelet root | only correct if the node's kubelet/containerd root is itself on NVMe; brittle, kept for parity with the original template |
| `pvc` | `volumeClaimTemplate` → local-volume-provisioner StorageClass | production; survives pod restarts on the same node |

`nodeSelector: walg.io/local-nvme: "true"` (the label the PoC nodegroup
sets) pins receiver pods to NVMe nodes regardless of mode.

---

## 6. Sizing — `sizing.go`

Flat default envelope for every receiver pod. No HPA/VPA — a receiver is
single-threaded and its ceiling is disk fsync rate, which scales with the
node class, not pod CPU. The receiver is disk-bound, not CPU-bound: ~11
receivers fit on one `m7gd.large` (NVMe-bound density), so packing is
governed by disk, and CPU just needs a small request with room to burst.

```go
type rp struct{ cpuReq, cpuLim, memReq, memLim string }

// Every receiver, regardless of spec.primaryTier:
var flatDefaultResources = rp{"100m", "2", "256Mi", "512Mi"}
//   CPU:    request 100m (10% of a vCPU), limit 2 (burst/auto-grow)
//   Memory: request 256Mi, limit 512Mi
```

The earlier per-primary-tier lookup table was **retired** in favor of this
flat baseline plus an override. Sizing no longer depends on `primaryTier`
(which stays in the spec as advisory metadata only), so there is no longer
an "unknown tier ⇒ Degraded" path. `spec.resourcesOverride` remains the
escape hatch and wins over the flat default.

> Note: with drain-batching default ON (10.9× IOPS reduction, stress doc
> §"Drain-batched") the receiver's real footprint is small; the flat
> `100m`→`2` CPU / `256Mi`–`512Mi` envelope is the safe baseline until we
> re-measure and tighten it.

---

## 7. Status & conditions — `status.go`

Reads the StatefulSet's pod, maps to a phase, sets standard conditions
(`Ready`, `Progressing`, `Degraded`) and `observedGeneration`.

```
Pod not yet created / 0 ready     -> Pending
Pod Ready                         -> Running
Pod CrashLoopBackOff > 3 min      -> Degraded (reason from container state)
Spec changed, pod rolling         -> Reconciling
DeletionTimestamp set             -> Terminating
```

`lastFsyncLSN` is best-effort: if the receiver writes a marker file the
operator can read (deferred — needs a pod exec or a shared annotation),
populate it; otherwise leave empty. Not load-bearing for the PoC.

---

## 8. Finalizer & teardown

Finalizer `walg.io/wal-receiver-cleanup`. On CR delete: delete the
StatefulSet (grace 30s lets `wal-g` close its replication slot cleanly),
delete the ConfigMap, **leave the Secret** (externally managed), then
remove the finalizer. OwnerRefs would cascade anyway; the finalizer
exists only to (a) guarantee delete *ordering* and (b) surface a
`Degraded` reason if teardown wedges instead of silently looping.

---

## 9. RBAC, manager, image

- **RBAC** (kubebuilder markers on the reconciler): full verbs on
  `walg.io/walreceivers` (+ `/status`, `/finalizers`), and on core
  `configmaps`, `apps/statefulsets`, plus `get;list;watch` on `pods`
  for status. No Secret write verbs — read-only `get` to validate the
  referenced Secret exists.
- **Manager** (`cmd/main.go`): leader election on, health/ready probes,
  metrics on :8080, watches all namespaces (receivers live in
  `walg-receivers`).
- **Image**: `Makefile` `docker-build` builds the operator (distroless
  static, arm64) → pushed to the same ECR registry as the receiver
  image (`794075227955.dkr.ecr.us-west-2.amazonaws.com/wal-g-operator`).
  CRD group is `walg.io/v1` (decoupled from Ubicloud) per the locked
  decision.

---

## 10. Mapping to PLAN.md phases

| PLAN phase | This doc |
|---|---|
| 1 Scaffold + CRD types | §2, §3 |
| 2 Core reconciler | §4, §5 |
| 3 Status reporting | §7 |
| 4 Topology-change reaction | §4.1 (config-hash roll) |
| 5 Per-tier sizing | §6 |
| 6 Finalizers + teardown | §8 |
| 7 kind smoke test | (superseded — we use the real EKS PoC cluster) |
| 8 Real-PG E2E | end-to-end on the EKS PoC + an Ubicloud sync_single primary |

---

## 11. MVP cut (what we build first)

For "a cluster with the operator running, where applying a CR provisions
a configured receiver," the minimum is **phases 1+2+ the failover-hash
from 4**: CRD + reconciler (ConfigMap + StatefulSet + ownerRefs +
config-hash) + `hostPathNVMe` storage. Status (§7), sizing table (§6),
and finalizer (§8) are fast follow-ons but not blocking the first
`kubectl apply -f walreceiver-sample.yaml` → pod demo.

Sample CR for the demo:

```yaml
apiVersion: walg.io/v1
kind: WalReceiver
metadata:
  name: walg-recv-demo
  namespace: walg-receivers
spec:
  postgresUbid: pvda4whfnm2y2gp0tkt8e2w5rs
  tenantName: async-ha-demo
  primaryTier: m8gd.4xlarge
  credentialsSecretRef: walg-recv-demo-secrets
  primary:
    host: 10.0.1.20
    user: ubi_replication
    slotName: walg_sync
  standby:
    sshHost: 10.0.1.21
    sshUser: ubi
    targetDir: /var/lib/postgresql/walg-tail
```
```
