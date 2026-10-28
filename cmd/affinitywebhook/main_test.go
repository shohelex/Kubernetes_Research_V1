package main

import (
	"encoding/json"
	"testing"

	"github.com/shohelex/Kubernetes_Research_V1/pkg/policy"
	"github.com/shohelex/Kubernetes_Research_V1/pkg/types"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

func reviewOf(t *testing.T, pod *corev1.Pod) *admissionv1.AdmissionResponse {
	t.Helper()
	raw, _ := json.Marshal(pod)
	in, _ := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request:  &admissionv1.AdmissionRequest{UID: k8stypes.UID("u1"), Object: runtime.RawExtension{Raw: raw}},
	})
	out, _, err := review(in)
	if err != nil {
		t.Fatal(err)
	}
	var ar admissionv1.AdmissionReview
	if err := json.Unmarshal(out, &ar); err != nil || ar.Response == nil {
		t.Fatalf("bad response %s: %v", out, err)
	}
	if ar.Response.UID != "u1" || !ar.Response.Allowed {
		t.Fatalf("the webhook must allow the pod and echo its UID: %+v", ar.Response)
	}
	return ar.Response
}

func TestWebhookAffinity(t *testing.T) {
	req := types.Pod{GDPRRequired: true, IsolationRequired: true}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Annotations: policy.Annotations(req)}}
	resp := reviewOf(t, pod)
	if resp.PatchType == nil || *resp.PatchType != admissionv1.PatchTypeJSONPatch {
		t.Fatal("a pod with requirements must be patched")
	}
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value corev1.Affinity `json:"value"`
	}
	if err := json.Unmarshal(resp.Patch, &ops); err != nil || len(ops) != 1 || ops[0].Op != "add" || ops[0].Path != "/spec/affinity" {
		t.Fatalf("patch %s: %v", resp.Patch, err)
	}
	want, _ := json.Marshal(policy.PrivacyAffinity(req, false))
	got, _ := json.Marshal(ops[0].Value)
	if string(want) != string(got) {
		t.Fatalf("injected %s, want %s", got, want)
	}

	if resp := reviewOf(t, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "q"}}); resp.Patch != nil {
		t.Fatal("a pod without requirements must not be patched")
	}
	own := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "r", Annotations: policy.Annotations(req)},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{}}}
	if resp := reviewOf(t, own); resp.Patch != nil {
		t.Fatal("a pod with its own affinity must be left alone")
	}
}

func TestAnnotationsRoundTrip(t *testing.T) {
	for _, req := range []types.Pod{{GDPRRequired: true}, {IsolationRequired: true, EncryptionRequired: true}, {GDPRRequired: true, IsolationRequired: true, EncryptionRequired: true}} {
		if got := policy.FromAnnotations(policy.Annotations(req)); got.GDPRRequired != req.GDPRRequired ||
			got.IsolationRequired != req.IsolationRequired || got.EncryptionRequired != req.EncryptionRequired {
			t.Fatalf("%+v came back as %+v", req, got)
		}
	}
	if policy.Annotations(types.Pod{}) != nil {
		t.Fatal("a pod without requirements has no requirement annotations")
	}
}
