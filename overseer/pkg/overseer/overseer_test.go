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

package overseer

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	sandboxapi "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	overseerv1alpha1 "github.com/gke-labs/gemini-for-kubernetes-development/overseer/pkg/api/v1alpha1"
)

func setupTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = overseerv1alpha1.AddToScheme(s)
	_ = sandboxapi.AddToScheme(s)
	return s
}

func TestReconcileOverseer_CreateSandbox(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	overseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-agent",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "2m",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(overseer).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, overseer); err != nil {
		t.Fatalf("unexpected error from ReconcileOverseer: %v", err)
	}

	if overseer.Status.OverseerStatus != "Active" {
		t.Errorf("expected OverseerStatus to be 'Active', got: %q", overseer.Status.OverseerStatus)
	}

	sandbox := &sandboxapi.Sandbox{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-test-agent", Namespace: "overseer-test-agent"}, sandbox)
	if err != nil {
		t.Fatalf("expected sandbox to be created, got error: %v", err)
	}

	// Verify owner reference
	ownerRefs := sandbox.GetOwnerReferences()
	if len(ownerRefs) != 1 {
		t.Fatalf("expected 1 owner reference on sandbox, got: %d", len(ownerRefs))
	}
	if ownerRefs[0].Name != "test-agent" || ownerRefs[0].Kind != "Overseer" {
		t.Errorf("unexpected owner reference: %+v", ownerRefs[0])
	}
}

func TestReconcileOverseer_CreateSandbox_WithTokenScript(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	overseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-ts",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL: "https://github.com/example/repo",
		},
	}

	tokenScriptSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tokenscript",
			Namespace: "overseer-agent-ts",
		},
		Data: map[string][]byte{
			"tokenscript.sh": []byte("#!/bin/bash"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(overseer, tokenScriptSecret).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, overseer); err != nil {
		t.Fatalf("unexpected error from ReconcileOverseer: %v", err)
	}

	sandbox := &sandboxapi.Sandbox{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-agent-ts", Namespace: "overseer-agent-ts"}, sandbox)
	if err != nil {
		t.Fatalf("failed to get created sandbox: %v", err)
	}

	// Verify tokenscript volume is present
	volumes := sandbox.Spec.PodTemplate.Spec.Volumes
	if len(volumes) == 0 {
		t.Fatalf("expected volumes in podSpec when tokenscript is present")
	}

	hasTSVol := false
	for _, v := range volumes {
		if v.Name == "tokenscript-vol" {
			hasTSVol = true
			break
		}
	}
	if !hasTSVol {
		t.Errorf("expected tokenscript-vol volume in sandbox podSpec")
	}
}

func TestReconcileOverseer_SpecUnchanged_NoUpdateNoDelete(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	overseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-unchanged",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "5m",
		},
	}

	// Pre-create the Sandbox matching desired spec
	sandbox := newOverseerSandboxFromOverseer(overseer, "overseer-agent-unchanged", "overseer-agent-unchanged", false)

	// Pre-create the Sandbox pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "overseer-agent-unchanged",
			Namespace: "overseer-agent-unchanged",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "overseer", Image: "test-image"},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(overseer, sandbox, pod).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, overseer); err != nil {
		t.Fatalf("unexpected error from ReconcileOverseer: %v", err)
	}

	if overseer.Status.OverseerStatus != "Active" {
		t.Errorf("expected status to be Active, got: %s", overseer.Status.OverseerStatus)
	}

	// Verify pod is still present and was NOT deleted
	currentPod := &corev1.Pod{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-agent-unchanged", Namespace: "overseer-agent-unchanged"}, currentPod)
	if err != nil {
		t.Fatalf("expected pod to remain untouched, got error: %v", err)
	}
}

func TestReconcileOverseer_SpecChanged_UpdatesSandboxAndDeletesPod(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	oldOverseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-update",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "1m",
		},
	}

	// Pre-create existing sandbox with old spec (PollInterval: 1m)
	existingSandbox := newOverseerSandboxFromOverseer(oldOverseer, "overseer-agent-update", "overseer-agent-update", false)

	// Pre-create the running sandbox pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "overseer-agent-update",
			Namespace: "overseer-agent-update",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "overseer", Image: "test-image"},
			},
		},
	}

	// Overseer with new spec (PollInterval: 10m)
	updatedOverseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-update",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "10m",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(updatedOverseer, existingSandbox, pod).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, updatedOverseer); err != nil {
		t.Fatalf("unexpected error from ReconcileOverseer: %v", err)
	}

	if updatedOverseer.Status.OverseerStatus != "Active" {
		t.Errorf("expected status to be Active, got: %s", updatedOverseer.Status.OverseerStatus)
	}

	// 1. Verify sandbox spec was updated
	updatedSandbox := &sandboxapi.Sandbox{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-agent-update", Namespace: "overseer-agent-update"}, updatedSandbox); err != nil {
		t.Fatalf("failed to get updated sandbox: %v", err)
	}

	// 2. Verify sandbox pod was deleted to force restart
	checkPod := &corev1.Pod{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-agent-update", Namespace: "overseer-agent-update"}, checkPod)
	if err == nil {
		t.Errorf("expected sandbox pod to be deleted, but it still exists: %+v", checkPod)
	} else if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound error for pod, got: %v", err)
	}
}

func TestReconcileOverseer_SpecChanged_PodNotFound_Succeeds(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	oldOverseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-no-pod",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "1m",
		},
	}

	// Pre-create existing sandbox with old spec, but no pod
	existingSandbox := newOverseerSandboxFromOverseer(oldOverseer, "overseer-agent-no-pod", "overseer-agent-no-pod", false)

	updatedOverseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-no-pod",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:      "https://github.com/example/repo",
			PollInterval: "10m",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(updatedOverseer, existingSandbox).
		Build()

	// Should succeed without error even if the pod was not found
	if err := ReconcileOverseer(ctx, k8sClient, updatedOverseer); err != nil {
		t.Fatalf("expected ReconcileOverseer to succeed when pod is not found, got error: %v", err)
	}

	if updatedOverseer.Status.OverseerStatus != "Active" {
		t.Errorf("expected status to be Active, got: %s", updatedOverseer.Status.OverseerStatus)
	}
}

func TestReconcileOverseer_LongNameTruncation(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	// Name that exceeds 63 characters with prefix
	longName := "very-long-name-that-when-prefixed-with-overseer-will-exceed-the-sixty-three-char-limit"
	overseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: longName,
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL: "https://github.com/example/repo",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(overseer).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, overseer); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedName := "overseer-" + longName
	if len(expectedName) > 63 {
		expectedName = expectedName[:63]
	}

	sandbox := &sandboxapi.Sandbox{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: expectedName, Namespace: expectedName}, sandbox); err != nil {
		t.Fatalf("expected sandbox with truncated name %q to exist, got error: %v", expectedName, err)
	}
}

func TestReconcileOverseer_CreateSandbox_CustomResourceQuantities(t *testing.T) {
	scheme := setupTestScheme()
	ctx := context.Background()

	workspaceDiskSize := resource.MustParse("50Gi")
	ephemeralStorage := resource.MustParse("20Gi")
	sandboxCPURequest := resource.MustParse("4000m")
	sandboxCPULimit := resource.MustParse("8000m")
	sandboxMemoryRequest := resource.MustParse("8Gi")
	sandboxMemoryLimit := resource.MustParse("16Gi")

	overseer := &overseerv1alpha1.Overseer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-resources",
		},
		Spec: overseerv1alpha1.OverseerSpec{
			RepoURL:              "https://github.com/example/repo",
			WorkspaceDiskSize:    workspaceDiskSize,
			EphemeralStorage:     ephemeralStorage,
			SandboxCPURequest:    sandboxCPURequest,
			SandboxCPULimit:      sandboxCPULimit,
			SandboxMemoryRequest: sandboxMemoryRequest,
			SandboxMemoryLimit:   sandboxMemoryLimit,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(overseer).
		Build()

	if err := ReconcileOverseer(ctx, k8sClient, overseer); err != nil {
		t.Fatalf("unexpected error from ReconcileOverseer: %v", err)
	}

	sandbox := &sandboxapi.Sandbox{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "overseer-custom-resources", Namespace: "overseer-custom-resources"}, sandbox)
	if err != nil {
		t.Fatalf("expected sandbox to be created, got error: %v", err)
	}

	// Verify ephemeral storage on container resources
	containers := sandbox.Spec.PodTemplate.Spec.Containers
	if len(containers) == 0 {
		t.Fatalf("expected at least one container in sandbox")
	}
	mainContainer := containers[0]
	if req := mainContainer.Resources.Requests[corev1.ResourceEphemeralStorage]; req.Cmp(ephemeralStorage) != 0 {
		t.Errorf("expected ephemeral storage request %s, got %s", ephemeralStorage.String(), req.String())
	}
	if lim := mainContainer.Resources.Limits[corev1.ResourceEphemeralStorage]; lim.Cmp(ephemeralStorage) != 0 {
		t.Errorf("expected ephemeral storage limit %s, got %s", ephemeralStorage.String(), lim.String())
	}

	// Verify workspace disk size on PVC template
	pvcs := sandbox.Spec.VolumeClaimTemplates
	if len(pvcs) == 0 {
		t.Fatalf("expected at least one volume claim template")
	}
	if req := pvcs[0].Spec.Resources.Requests[corev1.ResourceStorage]; req.Cmp(workspaceDiskSize) != 0 {
		t.Errorf("expected storage request %s, got %s", workspaceDiskSize.String(), req.String())
	}

	// Verify environment variables
	expectedEnv := map[string]string{
		"WORKSPACE_DISK_SIZE":    workspaceDiskSize.String(),
		"EPHEMERAL_STORAGE":      ephemeralStorage.String(),
		"SANDBOX_CPU_REQUEST":    sandboxCPURequest.String(),
		"SANDBOX_CPU_LIMIT":      sandboxCPULimit.String(),
		"SANDBOX_MEMORY_REQUEST": sandboxMemoryRequest.String(),
		"SANDBOX_MEMORY_LIMIT":   sandboxMemoryLimit.String(),
	}

	envMap := make(map[string]string)
	for _, envVar := range mainContainer.Env {
		envMap[envVar.Name] = envVar.Value
	}

	for k, expectedVal := range expectedEnv {
		if val, ok := envMap[k]; !ok || val != expectedVal {
			t.Errorf("expected env var %s to be %q, got %q (present: %v)", k, expectedVal, val, ok)
		}
	}
}
