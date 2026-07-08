package controller

import "testing"

// The receiver carries NO standby target in its env (the DR-push target is
// delivered at push-time via the control API). So a failover never changes
// this ConfigMap and never rolls the pod. A primary change MUST still roll it
// (re-point after failover completes).
func TestHashEnv_PrimaryRolls(t *testing.T) {
	base := map[string]string{
		"WALG_PRIMARY_HOST": "10.0.0.1",
		"WALG_SLOT_NAME":    "walg_sync",
	}

	// Identical data hashes identically (no spurious roll).
	same := map[string]string{
		"WALG_PRIMARY_HOST": "10.0.0.1",
		"WALG_SLOT_NAME":    "walg_sync",
	}
	if hashEnv(base) != hashEnv(same) {
		t.Fatalf("hash changed for identical env; would roll the pod spuriously")
	}

	// Changing the primary MUST change the hash (re-point/roll after failover).
	primaryChanged := map[string]string{
		"WALG_PRIMARY_HOST": "10.0.0.5", // promoted primary
		"WALG_SLOT_NAME":    "walg_sync",
	}
	if hashEnv(base) == hashEnv(primaryChanged) {
		t.Fatalf("hash unchanged when WALG_PRIMARY_HOST changed; the pod would not re-point after failover")
	}
}
