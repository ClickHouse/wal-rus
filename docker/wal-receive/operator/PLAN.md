# wal-receive operator — build plan

A Kubernetes operator that reconciles `WalReceiver` custom resources
to the manifest set we already have (ConfigMap + Secret + StatefulSet).
The operator owns sizing, topology-change reaction, and status
reporting. Ubicloud (or any other caller) just writes the CR.

## Choices already made

- **Framework: kubebuilder (Go).** Most production-grade scaffolding,
  single Go binary, single image to ship.
- **Code location: `docker/wal-receive/operator/`.** Subdirectory of
  the wal-g repo so the image, manifests, and operator release
  together. Its own Go module (`go.mod` at this level) so it doesn't
  drag the wal-g build into its dependency tree.
- **Smoke-test cluster: [kind](https://kind.sigs.k8s.io/).** Runs in
  Docker, ~5 min to bring up, multi-node trivial, reproducible from
  a config YAML.
- **E2E primary: Ubicloud Postgres provisioned per run** via the
  scripts under `.devcontainer/scripts/walg-test/` we already use.

## Time estimate

Six discrete phases. Each is independently testable and small enough
to fit in one focused work session.

| Phase | What | Effort | Cumulative | Status |
|---|---|---:|---:|---|
| 1 | Scaffold + CRD types | 0.5 d | 0.5 d | ✅ Done |
| 2 | Core reconciler (CM + STS) | 1.5 d | 2.0 d | ✅ Done |
| 3 | Status reporting | 0.5 d | 2.5 d | ✅ Done (code); no status-specific envtests, LastFsyncLSN scrape deferred |
| 4 | Topology-change reaction | 0.5 d | 3.0 d | ✅ Done (via config-hash rolling update, not pod-delete) |
| 5 | Per-tier sizing table | 0.5 d | 3.5 d | 🔄 Superseded — per-tier table retired for flat default + `resourcesOverride` |
| 6 | Finalizers + clean teardown | 0.5 d | 4.0 d | ✅ Done (minor: wedged-teardown Degraded reason not surfaced) |
| 7 | kind smoke test in CI | 0.5 d | 4.5 d | ❌ Not started |
| 8 | Real-PG E2E test | 1.0 d | 5.5 d | ❌ Not started |

> **Implementation status (audited against the tree on the operator branch).**
> Phases 1–6 are implemented (5 reshaped — see below); phases 7–8 (kind smoke
> CI, real-PG E2E) are not started: no `test/` dir, no operator CI workflow.
> Beyond the original plan, the operator also gained the **Option B control API
> LoadBalancer** (control Service + mTLS NLB + `status.controlEndpoint`), plus
> `Storage`/`Placement`/`Control` spec sections and a root-chown initContainer.

**Total: ~5–6 focused engineering-days.** Expect 6–8 conversation
sessions with you driving review between phases. Calendar time
depends on review cadence — tight feedback loops finish it in a
working week.

## Cross-phase invariants

Each phase ships:
- **Deliverable** — what's added or changed in the tree.
- **Exit criteria** — concrete, observable conditions for "done."
- **Automated test** — runs unattended, must be green to merge.

A phase is not done until its automated test passes locally **and**
in CI (kind cluster spun up in GitHub Actions on the wal-g fork).

CI configuration is added incrementally — phase 1 sets up the Go
toolchain and the lint/unit-test job; phase 7 adds the kind smoke
job; phase 8 adds the nightly real-PG job.

---

## Phase 1 — Scaffold + CRD types (0.5 d) — ✅ Done

> Implemented: `go.mod`/`Makefile`/`PROJECT`, `api/v1/walreceiver_types.go`
> (schema is richer than planned — adds `Control`, `Storage`, `Placement`),
> generated CRD YAML with the `primaryTier` enum, sample CR, and an envtest
> `suite_test.go` that loads the CRD.

### Deliverable

- `docker/wal-receive/operator/` initialized via `kubebuilder init`:
  - `go.mod` with kubebuilder + controller-runtime
  - `Makefile` with `manifests`, `generate`, `test`, `build`, `image`
  - `PROJECT` config file
- `api/v1/walreceiver_types.go` with the CRD schema:
  ```go
  type WalReceiverSpec struct {
    PostgresUbid          string         // pinned, validates against /^pv[a-z0-9]+$/
    Primary               PrimarySpec    // host, port, user, slot
    // (StandbySpec removed: the DR-push target is HTTP/mTLS and is supplied
    //  at push-time via the control API, not pinned in the CR.)
    PrimaryTier           string         // enum: m8gd.large | m8gd.4xlarge | ...
    CredentialsSecretRef  string         // mTLS material, externally managed
    Image                 string         // operator picks default if empty
  }
  type WalReceiverStatus struct {
    Phase         string   // Pending | Reconciling | Running | Degraded | Terminating
    PodName       string
    LastFsyncLSN  string   // optional; populated if pod exposes it
    Conditions    []metav1.Condition
  }
  ```
- Auto-generated CRD YAML under `config/crd/bases/`.
- Sample CR under `config/samples/`.

### Exit criteria

1. `make manifests generate` regenerates CRD + DeepCopy without errors.
2. `kubectl --dry-run=server apply -f config/crd/bases/...` against any
   K8s cluster succeeds.
3. Sample CR passes server-side validation (required fields enforced).
4. CRD schema rejects an invalid `primaryTier` value at apply time.

### Automated test

- `go vet ./...` exits 0.
- `make manifests` exits 0 and produces no diff vs committed YAML
  (CI fails if generated artifacts are stale).
- A minimal `controllers/suite_test.go` boots envtest, applies CRD,
  verifies a valid CR is accepted and an invalid one is rejected.

### Rollback

Delete the operator subdirectory. No external coupling yet.

---

## Phase 2 — Core reconciler (1.5 d) — ✅ Done

> Implemented in `walreceiver_controller.go` (+ `configmap.go`, `statefulset.go`,
> `diff.go`): create-or-update ConfigMap + StatefulSet, owner refs for cascade,
> idempotent applies (no churn on unchanged spec), Secret only mounted not
> created. All five planned envtests present in `walreceiver_controller_test.go`.

### Deliverable

- `internal/controller/walreceiver_controller.go` with a `Reconcile()`
  that, on CR create or update, produces:
  - A ConfigMap named `walg-recv-<UBID>-config` with all env vars
    derived from `.spec`.
  - A StatefulSet named `walg-recv-<UBID>` matching the existing
    template in `docker/wal-receive/k8s/statefulset.template.yaml`,
    parameterized from `.spec`.
- Owner references on both downstream objects so CR delete cascades.
- **Does not create the Secret** — that's externally managed (Ubicloud,
  External Secrets Operator, manual `kubectl create secret`, etc.).
  Operator only mounts the Secret referenced by
  `.spec.credentialsSecretRef`.

### Exit criteria

1. Applying a sample CR creates the expected ConfigMap and StatefulSet
   with the correct names and labels.
2. Reconcile is idempotent: applying the same CR twice produces no
   churn (no rolling restart, no spurious patch).
3. Pod that the STS spawns starts (no operator-side schema bugs that
   block scheduling). For this phase the wal-g process can fail at
   libpq connect — we're only testing that the reconciler produces a
   *schedulable* Pod.
4. Deleting the CR cascades to delete ConfigMap + STS via owner refs.

### Automated test

Envtest integration tests in `internal/controller/walreceiver_controller_test.go`:

| Test | Assertion |
|---|---|
| `creates a ConfigMap with all expected env keys` | All `WALG_*` keys present |
| `creates a StatefulSet with correct labels and selector` | Selector matches Pod template labels |
| `propagates spec.primary.host into ConfigMap env` | `WALG_PRIMARY_HOST` matches spec |
| `is idempotent when reconciling unchanged spec` | No update calls on second reconcile |
| `cascades delete via owner references` | Deleting CR deletes downstream objects |

All run in `make test` against envtest in < 60 s. No real K8s required.

### Rollback

Revert the controller file — CRD remains usable for further work.

---

## Phase 3 — Status reporting (0.5 d) — ✅ Done (code); tests/LSN deferred

> Implemented in `status.go`: `phase` (Pending/Reconciling/Running/Degraded/
> Terminating), `podName`, `Ready`/`Progressing`/`Degraded` conditions,
> CrashLoopBackOff > 3 min ⇒ Degraded, plus `controlEndpoint` (Option B).
> **Gaps:** no dedicated status-phase envtests (the planned Pending/Running/
> Degraded/Events tests), and the optional `lastFsyncLSN` marker-file scrape is
> not wired (field exists, never populated) — the plan allowed deferring it.

### Deliverable

- Reconciler updates `status.phase`, `status.podName`,
  `status.conditions[]` based on observed Pod state.
- Conditions follow standard K8s patterns: `Ready`, `Progressing`,
  `Degraded` (mirrors how kubebuilder handles these).
- Optional: scrape `lastFsyncLSN` by reading a marker file the
  receiver writes (defer if the pod can't write one cleanly).

### Exit criteria

1. `status.phase = Pending` while Pod is starting.
2. `status.phase = Running` when Pod reports Ready.
3. `status.phase = Degraded` after Pod is in CrashLoopBackOff for > 3 min.
4. `kubectl describe walreceiver <name>` shows phase, conditions, and
   downstream pod name.

### Automated test

Envtest:

| Test | Assertion |
|---|---|
| `phase=Pending when Pod is starting` | Initial reconcile, no Pod yet |
| `phase=Running when Pod is Ready` | Inject Ready Pod status, assert phase |
| `phase=Degraded after CrashLoopBackOff` | Inject ContainerStatuses[].Waiting.Reason, advance fake clock |
| `condition transitions emit Events` | Watch for `WalReceiverReconciled` events |

All in `make test`, no real cluster.

### Rollback

Revert to phase 2; CR still works without rich status.

---

## Phase 4 — Topology-change reaction (0.5 d) — ✅ Done (different mechanism)

> Implemented via a **config-hash pod-template annotation** (`hashEnv` over the
> ConfigMap data) that drives a StatefulSet rolling update on env-affecting
> changes — not the planned explicit Pod delete. Inert changes don't roll.
> Note: `WALG_STANDBY_*` keys are deliberately **excluded** from the hash so a
> standby change mid-failover updates the ConfigMap without rolling the pod
> (the DR-push target is delivered at push-time via the control API); primary
> changes still roll. Covered by `statefulset_hash_test.go` + the idempotency /
> "changes the config-hash when primary.host changes" controller tests.

### Deliverable

- When `.spec.primary.host`, `.spec.primary.port`, `.spec.standby.*`,
  or any other env-affecting field changes:
  - Update ConfigMap with new values.
  - Delete the Pod (StatefulSet recreates with new env).
  - Surface a `Restarting` condition until Pod is Ready again.
- Inert changes (labels, annotations) do NOT trigger restart.

### Exit criteria

1. Patching `spec.primary.host` triggers a Pod restart within one
   reconcile loop (< 30 s in envtest, < 60 s on real K8s).
2. After restart, `status.phase` returns to `Running`.
3. Patching `metadata.labels` does not restart the Pod.
4. Restart is idempotent — no infinite delete loop if reconcile fires
   mid-restart.

### Automated test

Envtest:

| Test | Assertion |
|---|---|
| `restarts Pod on primary.host change` | Pod has DeletionTimestamp within N reconciles |
| `does not restart Pod on label change` | No DeletionTimestamp after metadata-only edit |
| `recovers to Running after restart` | Phase returns to Running |
| `restart is idempotent` | Second reconcile during restart is a no-op |

### Rollback

Revert reaction logic; CR will still produce the initial Pod from
phase 2 but won't auto-update on topology changes.

---

## Phase 5 — Per-primary-tier sizing (0.5 d) — 🔄 Superseded

> The per-tier `primaryTierResources` table below was **retired** (see
> `sizing.go` / DESIGN §6): receivers are disk-bound not CPU-bound (~11 fit on
> an m7gd.large), so every pod now gets one **flat default envelope**
> (`100m/2` CPU, `256Mi/512Mi` mem) plus the `spec.resourcesOverride` escape
> hatch. `primaryTier` is still a validated enum on the CR but no longer drives
> sizing, so the "unknown tier ⇒ Degraded fallback" exit criterion is moot.
> Tests: "applies the flat default envelope regardless of primaryTier" and
> "lets resourcesOverride win over the flat default".

### Deliverable

- A `sizing.go` table:
  ```go
  var primaryTierResources = map[string]ResourcesPair{
    "m8gd.large":     {CPU: "100m/500m", Mem: "128Mi/384Mi"},
    "m8gd.4xlarge":   {CPU: "250m/1",    Mem: "192Mi/384Mi"},
    "m8gd.8xlarge":   {CPU: "500m/2",    Mem: "256Mi/512Mi"},
    "m8gd.16xlarge":  {CPU: "1/3",       Mem: "256Mi/512Mi"},
  }
  ```
- Reconciler applies these to the StatefulSet's container resources.
- Optional `.spec.resourcesOverride` for escape hatch.
- Unknown tier falls back to default with a Degraded condition explaining.

### Exit criteria

1. Each known `primaryTier` value yields the expected resources block.
2. Changing tier triggers a Pod restart with updated resources.
3. Unknown tier sets a `Degraded` condition and falls back to defaults.
4. `spec.resourcesOverride` (if set) wins over the table.

### Automated test

Envtest table-driven test parameterized over each tier. For each:
- Create CR with that tier
- Assert STS container resources match expected
- Mutate tier, assert restart + new resources

### Rollback

Revert sizing logic; reconciler falls back to hard-coded defaults.

---

## Phase 6 — Finalizers + clean teardown (0.5 d) — ✅ Done

> Implemented: finalizer `walg.io/wal-receiver-cleanup` added on create;
> `reconcileDelete` does ordered teardown (StatefulSet → ConfigMap → control
> Service), leaves the Secret, sets phase `Terminating`, then removes the
> finalizer. Covered by "tears down StatefulSet and ConfigMap via finalizer on
> delete". **Minor gap:** a wedged teardown requeues on error but does not set
> the planned `Degraded` condition with a reason.

### Deliverable

- Finalizer `walg.io/wal-receiver-cleanup` added to CR on create.
- On CR delete:
  - Delete StatefulSet first (Pod terminates with grace period 30 s).
  - Delete ConfigMap.
  - Leave Secret alone (externally managed).
  - Remove finalizer, allowing CR delete to complete.
- If any teardown step wedges, surface a Degraded condition with the
  reason.

### Exit criteria

1. `kubectl delete walreceiver <name>` triggers clean teardown in order.
2. CR is removed only after downstream resources are gone.
3. Secret persists after CR deletion.
4. If STS deletion is blocked (e.g., webhook denies), CR shows
   `Degraded` with a clear reason; doesn't loop silently.

### Automated test

Envtest:

| Test | Assertion |
|---|---|
| `deletes downstream resources on CR delete` | STS, CM gone within 30 s |
| `preserves Secret on CR delete` | Secret still exists |
| `removes finalizer after cleanup` | CR fully deleted |
| `surfaces Degraded if STS delete blocked` | Inject finalizer on STS, observe CR condition |

### Rollback

Drop the finalizer; falls back to K8s default cascade (which works
but doesn't give us the ordering guarantees).

---

## Phase 7 — kind smoke test in CI (0.5 d) — ❌ Not started

> No `test/kind/` (cluster.yaml, up.sh, down.sh), no `make smoke-kind` target,
> no operator CI workflow under `.github/workflows/`. (PoC validation has been
> done manually on the EKS `walg-operator-poc` cluster instead.)

### Deliverable

- `test/kind/cluster.yaml` — kind config with two worker nodes and
  the `nodeSelector` value our template expects (we'll fake this with
  a node label rather than real instance-store).
- `test/kind/up.sh`, `test/kind/down.sh`.
- `make smoke-kind` target that:
  1. Creates kind cluster
  2. Builds + loads operator image
  3. Applies CRD + operator deployment
  4. Creates a stub Secret (dummy cert + key files)
  5. Applies a sample CR pointing at a stub primary
  6. Polls until `status.phase=Running`
  7. Tears down the cluster
- GitHub Actions job (in the wal-g fork's CI) that runs `make smoke-kind`
  on every push to the operator branch.

### Exit criteria

1. `make smoke-kind` completes end-to-end in < 5 min.
2. The smoke test catches a deliberate regression — break the
   reconciler so it produces a malformed STS, re-run, observe failure.
3. CI workflow passes on push.
4. Cluster always tears down, even on test failure.

### Automated test

The smoke test **is** the automated test. For its inverse — verifying
it catches breakage — we'll keep a `test/kind/regression.sh` that
patches the controller to misbehave and asserts smoke-kind exits
non-zero. This regression test runs once during phase 7 development;
it doesn't need to live in steady-state CI.

For this phase, the wal-g process inside the Pod is replaced with a
small `sh -c 'sleep infinity'` shim — we're testing reconciler
correctness, not WAL streaming. Phase 8 tests with the real binary.

### Rollback

Operator still works against any K8s cluster, just no automated
push-button validation.

---

## Phase 8 — Real-PG E2E test (1.0 d) — ❌ Not started

> No `test/e2e/run.sh`, no `make e2e-real`, no nightly workflow. (End-to-end
> failover/RPO validation has been driven manually via the
> `.devcontainer/scripts/walg-test/` helpers against live Ubicloud PG.)

### Deliverable

- `test/e2e/run.sh`:
  1. Provision Ubicloud PG `op-e2e-pg` (sync_single, m8gd.4xlarge) via
     the existing `.devcontainer/scripts/walg-test/` helpers.
  2. Wait for cluster to reach `running`.
  3. Issue mTLS cert + SSH key for the receiver; create K8s Secret.
  4. Apply a `WalReceiver` CR pointing at the new primary.
  5. Wait for `status.phase=Running`.
  6. Verify primary's `pg_stat_replication` shows `walg_sync` with
     `sync_state=quorum`.
  7. Run a 60 s pgbench against the primary; record TPS + replay lag.
  8. Patch CR with a new (fake) `primary.host`, verify pod restarts.
  9. Delete CR, verify Pod and ConfigMap are gone, Secret persists.
  10. Destroy PG cluster.
- `make e2e-real` target.
- Nightly GitHub Actions job (separate from per-push CI because it
  costs real money).

### Exit criteria

1. End-to-end runs unattended in < 30 min including provision/destroy.
2. Operator-managed pod participates in `ANY 1 (standby, walg_sync)`
   quorum on the live primary.
3. Replay lag stays under 1 MB at steady state during pgbench.
4. Commit-latency p95 during the 60 s pgbench does not regress vs the
   bare-VM receiver from prior stress tests (i.e., < 2 ms at the same
   workload).
5. Operator-driven Pod restart during traffic causes < 5 s of lag bump
   and < 2 s of primary commit-latency spike.
6. Teardown is clean — no orphaned K8s objects, no stuck primary
   replication slot, no leaked Ubicloud resources.

### Automated test

The whole script is the test. Pass/fail asserted by the exit code of
the script — internal checks emit clear log markers and exit
non-zero on first failure.

Runs nightly. Failures alert via GitHub Actions notification (or
whatever channel you prefer once we get there).

### Rollback

E2E test removal doesn't affect operator function; just loses the
real-PG validation.

---

## Definition of "shipped"

All phases complete and green in CI. We have:
- A CRD anyone can install from a single YAML.
- An operator image anyone can pull from ghcr.io.
- A worked sample CR that runs end-to-end on kind.
- A nightly real-PG test backing claims about steady-state behavior.

At that point the operator is the public API. Ubicloud (or any other
tool) just writes the CR.

## Open questions — resolved

1. **API group** → ✅ `walg.io/v1` (decoupled, generic wal-g operator).
2. **CRD versioning** → ✅ v1 only for the PoC.
3. **Image registry** → ✅ Amazon ECR
   (`794075227955.dkr.ecr.us-west-2.amazonaws.com/wal-g-operator`, current tag
   `dev-optionb-budget`), not ghcr.
4. **Smoke-test cluster** → 🔄 kind never adopted; PoC runs on the EKS
   `walg-operator-poc` cluster (phase 7 kind CI remains unbuilt).
