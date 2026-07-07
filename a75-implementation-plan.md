# gRFC A75 Implementation Plan for grpc-go

This document outlines the design, impacted features, and task breakdown for implementing **[gRFC A75: xDS Aggregate Cluster Behavior Fixes](https://github.com/grpc/proposal/blob/master/A75-xds-aggregate-cluster-behavior-fixes.md)** in `grpc-go`.

---

## 1. Overview

In the original xDS aggregate cluster design (gRFC A37), gRPC assumed that aggregate clusters concatenated the priority lists of all underlying (leaf) clusters and configured a single LB policy at the aggregate cluster level. This led to per-priority outlier detection and `cluster_impl` instances, preventing features like Outlier Detection from working across all priorities in a cluster.

gRFC A75 refactors the LB policy hierarchy for both single and aggregate clusters:

### Non-Aggregate (Single EDS/DNS) Cluster Architecture
- **Old (pre-A75)**: `cds` → `priority` → `outlier_detection` → `cluster_impl` → `xdsLBPolicy` (`wrr_locality` / `weighted_target` / `pick_first`)
- **New (A75)**: `cds` → `outlier_detection` → `cluster_impl` → `priority` → `xdsLBPolicy`

### Aggregate Cluster Architecture
- **Old (pre-A75)**: `cds` (aggregate) concatenated all leaf cluster priorities into a single `priority` balancer, using the aggregate cluster's LB policy.
- **New (A75)**: An aggregate cluster is represented as an instance of a `priority` LB policy where each child is a `cds_experimental` LB policy for each underlying leaf cluster:
  - **Root**: `priority` (handles failover between child clusters $C_1, C_2, \dots$)
    - **Child $C_1$**: `cds_experimental` (for $C_1$) → `outlier_detection` → `cluster_impl` → `priority` → `xdsLBPolicy`
    - **Child $C_2$**: `cds_experimental` (for $C_2$) → `outlier_detection` → `cluster_impl` → `priority` → `xdsLBPolicy`

---

## 2. Summary of Impacted Features

| Feature | Pre-A75 Behavior | Post-A75 Behavior |
| :--- | :--- | :--- |
| **Outlier Detection (`outlier_detection`)** | Instantiated per-priority under `priority`. Ejection & health tracking were isolated within each priority. | Instantiated above `priority`. Ejection and host health tracking operate across all priorities ($P_0, P_1, \dots$) in a cluster. |
| **Cluster Features (`xds_cluster_impl`)** | Instantiated per-priority under `priority`. Load reporting, RPC drops, and circuit-breaking were per-priority. | Instantiated above `priority`. RPC drops, circuit-breaking (`max_requests`), LRS load reporting, and telemetry labels (`grpc.lb.backend_service`) operate per-cluster. |
| **Aggregate Cluster LB Policy** | Used the LB policy configured on the aggregate cluster across all leaf cluster priorities. | Ignores the aggregate cluster's LB policy. Each leaf cluster uses its own configured LB policy. |
| **Aggregate Cluster Failover** | Handled by merging priorities of leaf clusters into a single `priority` policy. | Handled by a top-level `priority` policy where each child is a `cds_experimental` balancer for a leaf cluster. |
| **Priority Child Naming ([#9013](https://github.com/grpc/grpc-go/issues/9013))** | Required `priority-<seq>-<id>` prefixes to avoid name collisions between leaf cluster priorities in the single `priority` policy. | Collision-free because each leaf cluster has its own `priority` policy instance. Global sequence prefixes can be safely removed. |
| **Stateful Session Affinity (SSA)** | N/A (Not yet implemented in `grpc-go`). | Out of scope. Future work (parsing CDS `idle_timeout` and `xds_override_host` subchannel management will be added when SSA is brought to `grpc-go`). |

---

## 3. Scope & Out-of-Scope

### In-Scope
- LB policy tree refactoring for single (non-aggregate) clusters (`cds` → `outlier_detection` → `cluster_impl` → `priority` → `xdsLBPolicy`).
- LB policy tree refactoring for aggregate clusters (top-level `priority` policy containing `cds_experimental` child balancers for each leaf cluster).
- Outlier detection and cluster-level load reporting / circuit breaking across all priorities in a cluster.
- Removal of global sequence ID prefixes in priority child names ([#9013](https://github.com/grpc/grpc-go/issues/9013)).
- E2E and unit testing for aggregate cluster failover and observability.

### Out-of-Scope: Stateful Session Affinity (SSA) Changes
- **CDS `idle_timeout` Parsing & `xds_override_host` Subchannel Management**: gRFC A75 includes specifications for reading the `idle_timeout` field from CDS `upstream_config` and adding subchannel ownership / idle timer sweeps to the `xds_override_host` LB policy.
- **Reason for Exclusion**: `grpc-go` currently does not implement Stateful Session Affinity (gRFCs A55 & A60) or the `xds_override_host` LB policy. Per gRFC A75's implementation guidance (*"The stateful session affinity changes will be implemented in those languages if/when we need to support stateful session affinity in those languages"*), these changes are deferred until SSA support is brought to `grpc-go`.

---

## 4. Task List & Story Point Estimates

Each task represents an independent, deliverable PR:

* **Task 1: Non-Aggregate Cluster LB Policy Tree Refactoring (PR 1)**
  * **Files**: `internal/xds/balancer/cdsbalancer/configbuilder.go`, `internal/xds/balancer/cdsbalancer/cdsbalancer.go`, `internal/xds/balancer/cdsbalancer/cdsbalancer_test.go`
  * **Description**: Refactor single-cluster (EDS / Logical DNS) LB policy topology from `cds` → `priority` → `outlier_detection` → `cluster_impl` → `xdsLBPolicy` to `cds` → `outlier_detection` → `cluster_impl` → `priority` → `xdsLBPolicy`. Includes updating `cdsBalancer.handleClusterUpdate()`, updating existing JSON assertion tests, and adding unit tests for outlier detection across priorities.
  * **Story Points**: **8**

* **Task 2: Aggregate Cluster LB Policy Tree Refactoring (PR 2)**
  * **Files**: `internal/xds/balancer/cdsbalancer/cdsbalancer.go`, `internal/xds/balancer/cdsbalancer/configbuilder_test.go`
  * **Description**: Represent aggregate clusters as a top-level `priority` LB policy whose children are `cds_experimental` instances for each leaf cluster. Ignore LB policy on aggregate cluster. Configure `IgnoreReresolutionRequests` per child depending on leaf cluster type (EDS vs DNS), and add unit tests for aggregate cluster config generation.
  * **Story Points**: **5**

* **Task 3: E2E Test Suite & Integration Verification (PR 3)**
  * **Files**: `internal/xds/balancer/cdsbalancer/e2e_test/aggregate_cluster_test.go`, `internal/xds/balancer/cdsbalancer/e2e_test/balancer_test.go`
  * **Description**: Add comprehensive E2E tests for aggregate cluster failover (primary to secondary leaf cluster), LRS load reporting, and telemetry label (`grpc.lb.backend_service`) propagation under aggregate clusters.
  * **Story Points**: **5**

* **Task 4: Priority Child Name Cleanup (PR 4 / Standalone Task)**
  * **Files**: `internal/xds/balancer/cdsbalancer/configbuilder_childname.go`, `internal/xds/balancer/cdsbalancer/configbuilder_childname_test.go`
  * **Description**: Remove `prefix` parameter and `childNameGeneratorSeqID` from `configbuilder_childname.go` (resolves [#9013](https://github.com/grpc/grpc-go/issues/9013)). Generate clean child names without global sequence prefixes and update corresponding unit tests.
  * **Story Points**: **1**

---

## 5. Summary of Tasks & Estimates

| Task / PR | Summary | Target Feature / Issue | Story Points |
| :--- | :--- | :--- | :--- |
| **Task 1 (PR 1)** | Non-Aggregate Cluster LB Policy Tree Refactoring | Non-aggregate A75 | **8 pts** |
| **Task 2 (PR 2)** | Aggregate Cluster LB Policy Tree Refactoring | Aggregate A75 | **5 pts** |
| **Task 3 (PR 3)** | E2E Test Suite & Integration Verification | A75 E2E / Observability | **5 pts** |
| **Task 4 (PR 4)** | Priority Child Name Cleanup | [Issue #9013](https://github.com/grpc/grpc-go/issues/9013) | **1 pt** |
