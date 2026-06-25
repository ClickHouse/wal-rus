package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

const (
	// configHashAnnotation rolls the pod when the rendered env changes.
	configHashAnnotation = "walg.io/config-hash"

	// The Rust walrus receiver image. The control plane overrides this per-CR
	// via spec.image (Config.walg_operator_receiver_image), so this is just the
	// standalone-deploy default.
	defaultImage = "794075227955.dkr.ecr.us-west-2.amazonaws.com/walg-receive:walrus"

	// initChownImage is a tiny image used by the partials-dir chown
	// initContainer. It only needs a shell + chown, so busybox is plenty.
	// Overridable via spec.initChownImage; defaulted here.
	defaultInitChownImage = "public.ecr.aws/docker/library/busybox:latest"

	partialDirMountPath = "/var/lib/walg/partials"

	uidGid int64 = 10001
)

// name returns the deterministic StatefulSet name walg-recv-<ubid>.
func stsName(wr *walgv1.WalReceiver) string {
	return "walg-recv-" + wr.Spec.PostgresUbid
}

func configMapName(wr *walgv1.WalReceiver) string {
	return stsName(wr) + "-config"
}

// selectorLabels are the immutable labels used for the STS selector and
// pod identity. The selector must never change after creation.
func selectorLabels(wr *walgv1.WalReceiver) map[string]string {
	return map[string]string{
		"ubicloud.io/postgres-resource-ubid": wr.Spec.PostgresUbid,
	}
}

// commonLabels are applied to all downstream objects.
func commonLabels(wr *walgv1.WalReceiver) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":             "wal-g-receive",
		"app.kubernetes.io/instance":         stsName(wr),
		"ubicloud.io/postgres-resource-ubid": wr.Spec.PostgresUbid,
	}
}

func objectMeta(wr *walgv1.WalReceiver, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: wr.Namespace,
		Labels:    commonLabels(wr),
	}
}

// hashEnv returns a stable sha256 over the ConfigMap data so inert edits
// (labels, annotations) don't roll the pod but env changes do.
//
// The receiver carries NO standby target in its env: the DR-push target is
// delivered at push-time via the control API (POST /v1/dr-catchup carries
// targetStandby.host). So a failover (new standby elected/recycled) changes
// nothing in this ConfigMap and never rolls the pod — exactly right, since
// failover is the moment the receiver must stay up to serve the gap. The pod
// is re-pointed (and may roll) only on a primary change.
func hashEnv(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		// The primary host is re-targeted LIVE via the receiver's failover-primary
		// control API (which rewrites PGHOST in the running process and preserves
		// its fsync frontier). Excluding it from the roll-hash means a failover
		// updates the ConfigMap (the declarative cold-start value, kept converged
		// by the control plane) WITHOUT rolling the pod -- a pod roll would reset
		// HighestFsyncdLSN to 0/0 and wedge the promote fastpath. See the
		// failover-primary design (doc/walg-failover-primary.md).
		if k == primaryHostConfigKey {
			continue
		}
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(data[k]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// desiredStatefulSet renders the per-tenant StatefulSet. configHash is
// the hash of the rendered ConfigMap data; it is stamped on the pod
// template so a primary.host change rolls the pod.
func (r *WalReceiverReconciler) desiredStatefulSet(wr *walgv1.WalReceiver, configHash string) *appsv1.StatefulSet {
	resources := resourcesForSpec(wr)

	image := wr.Spec.Image
	if image == "" {
		image = defaultImage
	}
	// Default Always: the receiver image uses a MUTABLE dev tag (e.g. "dev"),
	// so IfNotPresent would let a node serve a stale cached image after a
	// rebuild+retag. Always re-pulls the current digest. Override per-CR if a
	// pinned/immutable tag is ever used.
	pullPolicy := corev1.PullPolicy(orDefaultString(wr.Spec.ImagePullPolicy, string(corev1.PullAlways)))

	initChownImage := orDefaultString(wr.Spec.InitChownImage, defaultInitChownImage)

	secretName := wr.Spec.CredentialsSecretRef
	podLabels := selectorLabels(wr)
	for k, v := range commonLabels(wr) {
		podLabels[k] = v
	}

	partialsVolume := partialsVolume(wr)

	// S3 DR-tail (PoC static keys): inject the tenant S3 access key/secret as env
	// from the credentials Secret (keys optional so push-mode receivers are
	// unaffected). The non-secret S3 config lives in the ConfigMap.
	var awsEnv []corev1.EnvVar
	if wr.Spec.Control.DRDeliveryS3 && wr.Spec.S3.Prefix != "" {
		optional := true
		awsEnv = []corev1.EnvVar{
			{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: "aws-access-key-id", Optional: &optional}}},
			{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: "aws-secret-access-key", Optional: &optional}}},
		}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: objectMeta(wr, stsName(wr)),
		Spec: appsv1.StatefulSetSpec{
			ServiceName: stsName(wr),
			Replicas:    ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(wr),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
					Annotations: map[string]string{
						configHashAnnotation: configHash,
					},
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"walg.io/local-nvme": "true",
					},
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  ptr.To(uidGid),
						RunAsGroup: ptr.To(uidGid),
						FSGroup:    ptr.To(uidGid),
					},
					TerminationGracePeriodSeconds: ptr.To(int64(30)),
					// The partials volume is a kubelet-created hostPath
					// (default root:root 0755) or an emptyDir, so the main
					// container (uid/gid 10001) cannot write to it. Run a
					// tiny root initContainer first to chown/chmod the
					// mount to 10001:10001. For pvc mode FSGroup already
					// handles this, but the initContainer is harmless.
					InitContainers: []corev1.Container{{
						Name:            "init-chown-partials",
						Image:           initChownImage,
						ImagePullPolicy: pullPolicy,
						Command: []string{
							"sh",
							"-c",
							"chown -R 10001:10001 " + partialDirMountPath + " && chmod 0700 " + partialDirMountPath,
						},
						SecurityContext: &corev1.SecurityContext{
							RunAsUser:  ptr.To(int64(0)),
							RunAsGroup: ptr.To(int64(0)),
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "partials", MountPath: partialDirMountPath},
						},
					}},
					Containers: []corev1.Container{{
						Name:            "wal-receive",
						Image:           image,
						ImagePullPolicy: pullPolicy,
						EnvFrom: []corev1.EnvFromSource{{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: configMapName(wr),
								},
							},
						}},
						Env: awsEnv,
						Ports: []corev1.ContainerPort{
							// Option B control API; targeted by the control LB Service.
							{Name: "control", ContainerPort: controlPort(wr), Protocol: corev1.ProtocolTCP},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "tls", MountPath: "/etc/walg/tls", ReadOnly: true},
							{Name: "partials", MountPath: partialDirMountPath},
						},
						Resources: resources,
						// No liveness probe: wal-g is the container's main process
						// and exits on fatal errors, so the StatefulSet restarts it
						// on real failures. A file-mtime "progress" probe falsely
						// kills an idle-but-healthy receiver during quiet write
						// periods (no new partials != hung).
					}},
					Volumes: []corev1.Volume{
						{
							Name: "tls",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: secretName,
									Items: []corev1.KeyToPath{
										{Key: "client.crt", Path: "client.crt"},
										{Key: "client.key", Path: "client.key"},
										{Key: "server-ca.crt", Path: "server-ca.crt"},
										// Option B control server identity + client CA (mTLS).
										{Key: "control-server.crt", Path: "control-server.crt"},
										{Key: "control-server.key", Path: "control-server.key"},
										{Key: "client-ca.crt", Path: "client-ca.crt"},
									},
								},
							},
						},
						partialsVolume,
					},
				},
			},
		},
	}

	// Optional AZ pin: a hard nodeAffinity on the zone topology label,
	// in addition to the walg.io/local-nvme nodeSelector (both must
	// hold). The affinity lives in the pod template, so changing the
	// zone updates the StatefulSet template and rolls the pod via the
	// normal STS rollout (it does not go through the config-hash, which
	// only tracks ConfigMap env).
	if zone := wr.Spec.Placement.Zone; zone != "" {
		sts.Spec.Template.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "topology.kubernetes.io/zone",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{zone},
						}},
					}},
				},
			},
		}
	}

	// pvc mode uses a volumeClaimTemplate instead of a pod-level volume.
	if wr.Spec.Storage.Mode == "pvc" {
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvcTemplate(wr)}
	}

	return sts
}

// partialsVolume builds the pod-level partials volume for the chosen
// storage mode. For "pvc" mode the volume is supplied by the
// volumeClaimTemplate, so this returns an empty placeholder that is not
// appended.
func partialsVolume(wr *walgv1.WalReceiver) corev1.Volume {
	sizeLimit := orDefaultString(wr.Spec.Storage.SizeLimit, "20Gi")
	switch wr.Spec.Storage.Mode {
	case "emptyDir":
		q := resource.MustParse(sizeLimit)
		return corev1.Volume{
			Name: "partials",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &q},
			},
		}
	case "pvc":
		// Placeholder; replaced by volumeClaimTemplate named "partials".
		return corev1.Volume{Name: "partials"}
	default: // hostPathNVMe
		base := orDefaultString(wr.Spec.Storage.HostPathBase, "/mnt/nvme/walg-partials")
		return corev1.Volume{
			Name: "partials",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: base + "/" + wr.Spec.PostgresUbid,
					Type: ptr.To(corev1.HostPathDirectoryOrCreate),
				},
			},
		}
	}
}

func pvcTemplate(wr *walgv1.WalReceiver) corev1.PersistentVolumeClaim {
	sizeLimit := orDefaultString(wr.Spec.Storage.SizeLimit, "20Gi")
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "partials"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(sizeLimit),
				},
			},
		},
	}
}
