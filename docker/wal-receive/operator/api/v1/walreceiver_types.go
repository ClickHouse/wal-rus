package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WalReceiverSpec is the desired state of one per-tenant receiver.
type WalReceiverSpec struct {
	// PostgresUbid is the Ubicloud Postgres resource UBID. It pins
	// every derived object name: walg-recv-<ubid>, ...-config, etc.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]{26}$`
	PostgresUbid string `json:"postgresUbid"`

	// TenantName is human-readable; used in logs and push labels.
	TenantName string `json:"tenantName"`

	Primary PrimarySpec `json:"primary"`

	// Control configures the receiver's control HTTP/mTLS API for
	// control-plane-orchestrated failover (Option B).
	// +optional
	Control ControlSpec `json:"control,omitempty"`

	// S3 gives the receiver object-storage access so the S3 DR-tail delivery
	// (control.drDeliveryS3) can upload the failover tail to the tenant bucket.
	// Only needed when drDeliveryS3 is set. The access key id/secret are read
	// from the credentials Secret (keys aws-access-key-id / aws-secret-access-key).
	// PoC: static per-timeline keys; see doc/walg-receiver-s3-dr-delivery.md.
	// +optional
	S3 S3Spec `json:"s3,omitempty"`

	// PrimaryTier is informational — the primary's instance size. Sizing no
	// longer depends on it (flat default envelope), so any value is accepted
	// (the old m8gd-only Enum hard-failed i8g/other families at admission).
	PrimaryTier string `json:"primaryTier"`

	// CredentialsSecretRef names the externally-managed Secret holding the
	// receiver's mTLS material (client.crt/client.key/server-ca.crt and the
	// Option B control-server cert/key + client-ca.crt).
	// The operator MOUNTS it; it never creates or mutates it.
	CredentialsSecretRef string `json:"credentialsSecretRef"`

	// Image is the wal-receive image. Operator fills a default if empty.
	// +optional
	Image string `json:"image,omitempty"`

	// InitChownImage is the tiny image used by the partials-dir chown
	// initContainer (needs only a shell + chown). Operator defaults it to
	// a busybox image if empty.
	// +optional
	InitChownImage string `json:"initChownImage,omitempty"`

	// ImagePullPolicy for the receiver container.
	// +kubebuilder:default=IfNotPresent
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Storage selects where the WAL partial dir lives (§5.3).
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Placement controls where the receiver pod is scheduled.
	// +optional
	Placement PlacementSpec `json:"placement,omitempty"`

	// ResourcesOverride is an escape hatch that wins over the tier table.
	// +optional
	ResourcesOverride *corev1.ResourceRequirements `json:"resourcesOverride,omitempty"`
}

// PlacementSpec controls pod scheduling for the receiver.
type PlacementSpec struct {
	// Zone pins the pod to a topology.kubernetes.io/zone (AWS AZ),
	// e.g. "us-west-2b". Empty = scheduler chooses any NVMe node.
	// +optional
	Zone string `json:"zone,omitempty"`
}

// PrimarySpec describes the primary the receiver streams from.
type PrimarySpec struct {
	Host string `json:"host"`
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`
	// User, e.g. "ubi_replication".
	User string `json:"user"`
	// SlotName is the physical replication slot the receiver attaches to, e.g. "walg_sync".
	SlotName string `json:"slotName"`
	// +kubebuilder:default=walg_sync
	// +optional
	ApplicationName string `json:"applicationName,omitempty"`
}

// ControlSpec configures the receiver's control HTTP/mTLS API (Option B,
// doc/walg-receiver-control-channel-design.md). The control plane calls
// GET /v1/status and POST /v1/dr-catchup on it during failover. The operator
// runs the server on Port, exposes it via a TCP LoadBalancer (NLB, which
// preserves the receiver's mTLS by passing TCP straight through), and reports
// the reachable address in status.controlEndpoint.
type ControlSpec struct {
	// Port the receiver's control server listens on.
	// +kubebuilder:default=8444
	// +optional
	Port int32 `json:"port,omitempty"`

	// Disabled turns the control API + its LoadBalancer off (the receiver then
	// falls back to the autonomous push-on-primary-loss). Default false.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// (S3Spec moved out of ControlSpec — see WalReceiverSpec.S3.)

	// DRDeliveryS3 makes /v1/dr-catchup upload the retained tail to the S3
	// DR-tail prefix (WALG_WAL_RECEIVE_DR_S3) instead of pushing it to the
	// standby. Flag-gated alternative to the direct push; default false.
	// See doc/walg-receiver-s3-dr-delivery.md.
	// +optional
	DRDeliveryS3 bool `json:"drDeliveryS3,omitempty"`
}

// S3Spec is the receiver's object-storage destination for S3 DR-tail delivery.
// Non-secret fields only; the access key id/secret come from the credentials
// Secret (aws-access-key-id / aws-secret-access-key).
type S3Spec struct {
	// Prefix is the tenant WAL prefix, e.g. s3://<timeline-ubid>. The dr-tail
	// objects go under <prefix>/dr-tail/wal_<v>/.
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// Region, e.g. us-west-2.
	// +optional
	Region string `json:"region,omitempty"`
	// Endpoint, e.g. https://s3.us-west-2.amazonaws.com.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// ForcePathStyle sets AWS_S3_FORCE_PATH_STYLE.
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
}

// StorageSpec selects where the WAL partial dir is backed.
type StorageSpec struct {
	// Mode: "hostPathNVMe" (PoC EKS, /mnt/nvme), "emptyDir", or "pvc".
	// +kubebuilder:validation:Enum=hostPathNVMe;emptyDir;pvc
	// +kubebuilder:default=hostPathNVMe
	// +optional
	Mode string `json:"mode,omitempty"`
	// +kubebuilder:default="20Gi"
	// +optional
	SizeLimit string `json:"sizeLimit,omitempty"`
	// HostPathBase is the node mount for hostPathNVMe mode.
	// +kubebuilder:default="/mnt/nvme/walg-partials"
	// +optional
	HostPathBase string `json:"hostPathBase,omitempty"`
}

// WalReceiverStatus is the observed state.
type WalReceiverStatus struct {
	// +kubebuilder:validation:Enum=Pending;Reconciling;Running;Degraded;Terminating
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// ControlEndpoint is the reachable "host:port" of the receiver's control API
	// (the LoadBalancer address), for the control plane to call. Empty until the
	// LoadBalancer has an address.
	// +optional
	ControlEndpoint string `json:"controlEndpoint,omitempty"`
	// LastFsyncLSN, populated if the pod exposes it; best-effort.
	// +optional
	LastFsyncLSN string `json:"lastFsyncLSN,omitempty"`
	// ObservedGeneration lets us tell a stale status from a fresh one.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Primary,type=string,JSONPath=`.spec.primary.host`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// WalReceiver is the Schema for the walreceivers API.
type WalReceiver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WalReceiverSpec   `json:"spec,omitempty"`
	Status WalReceiverStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WalReceiverList contains a list of WalReceiver.
type WalReceiverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WalReceiver `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WalReceiver{}, &WalReceiverList{})
}
