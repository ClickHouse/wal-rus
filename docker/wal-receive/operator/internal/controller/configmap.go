package controller

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

// primaryHostConfigKey is the ConfigMap key carrying the streaming primary's
// host. It is deliberately EXCLUDED from the StatefulSet roll-hash (see
// configHash) so a failover re-points it without restarting the receiver pod;
// the live re-target goes through the failover-primary control API instead.
const primaryHostConfigKey = "WALG_PRIMARY_HOST"

// desiredConfigMap renders the per-tenant ConfigMap. It is a pure
// function of spec, mirroring k8s/configmap.template.yaml one-to-one.
func (r *WalReceiverReconciler) desiredConfigMap(wr *walgv1.WalReceiver) *corev1.ConfigMap {
	s := wr.Spec
	data := map[string]string{
		// --- Primary connection ---
		primaryHostConfigKey:    s.Primary.Host,
		"WALG_PRIMARY_PORT":     strconv.Itoa(int(orDefaultInt32(s.Primary.Port, 5432))),
		"WALG_PRIMARY_USER":     s.Primary.User,
		"WALG_PRIMARY_DB":       "postgres",
		"WALG_APPLICATION_NAME": orDefaultString(s.Primary.ApplicationName, "walg_sync"),

		// --- Replication slot ---
		"WALG_SLOT_NAME": s.Primary.SlotName,

		// --- Tenant identity ---
		"WALG_TENANT_NAME": s.TenantName,

		// --- Local storage path (matches the volumeMount in the StatefulSet) ---
		// Emit the canonical name the wal-g binary actually reads
		// (PartialDirEnv = WALG_WAL_RECEIVE_PARTIAL_DIR) so the configmap and the
		// code agree directly, instead of relying on entrypoint.sh to remap a
		// differently-named WALG_PARTIAL_DIR. The entrypoint keeps a backward-
		// compat fallback to the old name for any externally-set override.
		"WALG_WAL_RECEIVE_PARTIAL_DIR": "/var/lib/walg/partials",

		// --- Tuning (image-baked defaults; override per-tenant later) ---
		"WALG_WAL_RECEIVE_JANITOR_INTERVAL_SECONDS": "30",
		"WALG_LOG_LEVEL":                            "NORMAL",
	}

	// --- Option B: control-plane-orchestrated failover control API ---
	// Setting CONTROL_LISTEN runs the receiver's control HTTP/mTLS server and
	// disables the autonomous push-on-primary-loss (the control plane drives
	// catch-up via /v1/dr-catchup). Cert paths point at the mounted resource
	// certs; the same client identity is reused for the dr-catchup push.
	if !s.Control.Disabled {
		data["WALG_WAL_RECEIVE_CONTROL_LISTEN"] = ":" + strconv.Itoa(int(controlPort(wr)))
		// Control server identity = the resource server cert (server-CA-signed,
		// so the CP verifies it against the server CA); client-CA = the resource
		// client CA, so the receiver verifies the CP's client-CA-signed cert.
		data["WALG_WAL_RECEIVE_CONTROL_TLS_CERT"] = "/etc/walg/tls/control-server.crt"
		data["WALG_WAL_RECEIVE_CONTROL_TLS_KEY"] = "/etc/walg/tls/control-server.key"
		data["WALG_WAL_RECEIVE_CONTROL_CLIENT_CA"] = "/etc/walg/tls/client-ca.crt"
		// dr-catchup push: the receiver is the CLIENT to the standby's serve
		// (client-CA-signed cert, server CA to verify the standby server cert).
		data["WALG_WAL_RECEIVE_PUSH_CLIENT_CERT"] = "/etc/walg/tls/client.crt"
		data["WALG_WAL_RECEIVE_PUSH_CLIENT_KEY"] = "/etc/walg/tls/client.key"
		data["WALG_WAL_RECEIVE_PUSH_SERVER_CA"] = "/etc/walg/tls/server-ca.crt"
	}

	// S3 DR-tail delivery (alternative to the direct push; doc/walg-receiver-
	// s3-dr-delivery.md): dr-catchup uploads the tail to <WALG_S3_PREFIX>/dr-tail
	// and the candidate wal-fetches it, instead of pushing to the standby.
	if s.Control.DRDeliveryS3 {
		data["WALG_WAL_RECEIVE_DR_S3"] = "true"
		// Object-storage destination for the dr-tail upload. Non-secret config
		// here; the access key id/secret are injected as env from the Secret (see
		// statefulset.go). PoC: static per-timeline keys.
		if s.S3.Prefix != "" {
			data["WALG_S3_PREFIX"] = s.S3.Prefix
			data["AWS_REGION"] = s.S3.Region
			data["AWS_ENDPOINT"] = s.S3.Endpoint
			if s.S3.ForcePathStyle {
				data["AWS_S3_FORCE_PATH_STYLE"] = "true"
			}
		}
	}

	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(wr, configMapName(wr)),
		Data:       data,
	}
}

func orDefaultInt32(v, def int32) int32 {
	if v == 0 {
		return def
	}
	return v
}

func orDefaultString(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
