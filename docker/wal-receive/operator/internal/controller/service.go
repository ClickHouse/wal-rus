package controller

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	walgv1 "github.com/wal-g/wal-receive-operator/api/v1"
)

const defaultControlPort int32 = 8444

// controlServiceName is the per-tenant control LoadBalancer Service name.
func controlServiceName(wr *walgv1.WalReceiver) string {
	return stsName(wr) + "-control"
}

// controlPort is the receiver control API port (spec or default 8444).
func controlPort(wr *walgv1.WalReceiver) int32 {
	return orDefaultInt32(wr.Spec.Control.Port, defaultControlPort)
}

// desiredControlService renders the LoadBalancer Service that exposes the
// receiver's control API (Option B) to the control plane. It is a TCP NLB so
// the receiver's mTLS passes straight through to the pod — the LB never
// terminates TLS, preserving the client-cert auth the control plane relies on.
// Returns nil when the control API is disabled.
//
// PoC: internet-facing so the control plane (which reaches the PG VMs over the
// public network today) can call it with its resource-signed client cert.
// Production should make this internal and reach it over the peered VNet (see
// the VPC-peering work) instead of the public internet.
func (r *WalReceiverReconciler) desiredControlService(wr *walgv1.WalReceiver) *corev1.Service {
	if wr.Spec.Control.Disabled {
		return nil
	}
	port := controlPort(wr)
	meta := objectMeta(wr, controlServiceName(wr))
	// AWS load-balancer-controller hints; harmless on clusters without it
	// (the Service stays Pending until some LB provisioner fulfils it).
	meta.Annotations = map[string]string{
		"service.beta.kubernetes.io/aws-load-balancer-type":            "nlb",
		"service.beta.kubernetes.io/aws-load-balancer-scheme":          "internet-facing",
		"service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "ip",
	}
	return &corev1.Service{
		ObjectMeta: meta,
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
			Selector:              selectorLabels(wr),
			Ports: []corev1.ServicePort{{
				Name:       "control",
				Port:       port,
				TargetPort: intstr.FromInt(int(port)),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

func servicePortsEqual(a, b []corev1.ServicePort) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Port != b[i].Port ||
			a[i].Protocol != b[i].Protocol || a[i].TargetPort != b[i].TargetPort {
			return false
		}
	}
	return true
}

// controlEndpointFromService returns "host:port" from a provisioned
// LoadBalancer Service, or "" if it has no address yet.
func controlEndpointFromService(svc *corev1.Service) string {
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		host := ing.Hostname
		if host == "" {
			host = ing.IP
		}
		if host == "" {
			continue
		}
		port := defaultControlPort
		if len(svc.Spec.Ports) > 0 {
			port = svc.Spec.Ports[0].Port
		}
		return host + ":" + strconv.Itoa(int(port))
	}
	return ""
}
