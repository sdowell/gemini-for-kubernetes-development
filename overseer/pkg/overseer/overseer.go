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
	"encoding/json"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	sandboxapi "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	overseerv1alpha1 "github.com/gke-labs/gemini-for-kubernetes-development/overseer/pkg/api/v1alpha1"
)

// collectorURL is the token-usage collector endpoint injected into overseer
// sandboxes; the factory binary reports per-task gemini token usage there.
// Overridable on the controller deployment; empty disables reporting.
func collectorURL() string {
	if v, ok := os.LookupEnv("COLLECTOR_URL"); ok {
		return v
	}
	return "http://token-usage.overseer-system.svc.cluster.local:8080"
}

// ReconcileOverseer ensures the Overseer sandbox is running for the given Overseer.
func ReconcileOverseer(ctx context.Context, c client.Client, o *overseerv1alpha1.Overseer) error {
	log := log.FromContext(ctx)

	overseerName := fmt.Sprintf("overseer-%s", o.Name)
	if len(overseerName) > 63 {
		overseerName = overseerName[:63]
	}
	namespace := fmt.Sprintf("overseer-%s", o.Name)
	if len(namespace) > 63 {
		namespace = namespace[:63]
	}

	hasTokenScript := false
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: "tokenscript", Namespace: namespace}, secret); err == nil {
		hasTokenScript = true
	} else if !errors.IsNotFound(err) {
		return err
	}

	sandbox := &sandboxapi.Sandbox{}
	err := c.Get(ctx, types.NamespacedName{Name: overseerName, Namespace: namespace}, sandbox)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create
			log.Info("Creating Overseer sandbox", "name", overseerName, "namespace", namespace)
			newSandbox := newOverseerSandboxFromOverseer(o, overseerName, namespace, hasTokenScript)
			if err := controllerutil.SetControllerReference(o, newSandbox, c.Scheme()); err != nil {
				return err
			}
			if err := c.Create(ctx, newSandbox); err != nil {
				return err
			}
			o.Status.OverseerStatus = "Active"
			return nil
		}
		return err
	}

	// If exists, compare spec and update if changed
	desiredSandbox := newOverseerSandboxFromOverseer(o, overseerName, namespace, hasTokenScript)

	if !apiequality.Semantic.DeepEqual(sandbox.Spec, desiredSandbox.Spec) {
		log.Info("Updating Overseer sandbox spec", "name", overseerName, "namespace", namespace)
		sandbox.Spec = desiredSandbox.Spec
		if err := c.Update(ctx, sandbox); err != nil {
			return fmt.Errorf("updating sandbox spec: %w", err)
		}

		// Delete the sandbox pod to force a restart/recreation with the new spec
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      overseerName,
				Namespace: namespace,
			},
		}
		log.Info("Deleting Overseer sandbox pod to force restart", "name", overseerName, "namespace", namespace)
		if err := c.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete sandbox pod to force restart", "name", overseerName, "namespace", namespace)
		}
	}

	o.Status.OverseerStatus = "Active"
	return nil
}

func newOverseerSandboxFromOverseer(o *overseerv1alpha1.Overseer, name, namespace string, hasTokenScript bool) *sandboxapi.Sandbox {
	image := os.Getenv("OVERSEER_IMAGE")

	secretName := "factory-user"
	if roleSpec, ok := o.Spec.Roles["watcher"]; ok && len(roleSpec.Users) > 0 && roleSpec.Users[0] != "" {
		secretName = fmt.Sprintf("user-%s", roleSpec.Users[0])
	}

	env := []corev1.EnvVar{
		{
			Name: "GITHUB_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
					Key:      "GITHUB_TOKEN",
					Optional: ptr.To(true),
				},
			},
		},
		{
			Name: "GITHUB_LOGIN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
					Key:      "GITHUB_LOGIN",
					Optional: ptr.To(true),
				},
			},
		},
		{
			Name: "GITHUB_EMAIL",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
					Key:      "GITHUB_EMAIL",
					Optional: ptr.To(true),
				},
			},
		},
		{
			Name: "GEMINI_API_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
					Key:      "GEMINI_API_KEY",
					Optional: ptr.To(true),
				},
			},
		},
		{
			Name:  "GITHUB_API_URL",
			Value: "http://github-portal.overseer-system.svc.cluster.local/",
		},
		{
			Name:  "GEMINI_CLI_TRUST_WORKSPACE",
			Value: "true",
		},
		{
			Name:  "REPO_URL",
			Value: o.Spec.RepoURL,
		},
		{
			Name:  "OVERSEER_NAME",
			Value: o.Name,
		},
		{
			Name:  "NAMESPACE",
			Value: namespace,
		},
		{
			Name:  "HOME",
			Value: "/workspaces/.home",
		},
		{
			Name:  "POLL_INTERVAL",
			Value: o.Spec.PollInterval,
		},
		{
			Name:  "ALLOW_GEMINI_ORCHESTRATION",
			Value: fmt.Sprintf("%t", o.Spec.EnableGeminiOrchestrator),
		},
		{
			Name:  "COLLECTOR_URL",
			Value: collectorURL(),
		},
	}

	rolesJSON, _ := json.Marshal(o.Spec.Roles)
	env = append(env, corev1.EnvVar{
		Name:  "FACTORY_ROLES",
		Value: string(rolesJSON),
	})

	if o.Spec.Chores != nil && o.Spec.Chores.Mode != "" {
		env = append(env, corev1.EnvVar{
			Name:  "CHORES_MODE",
			Value: o.Spec.Chores.Mode,
		})
	}

	if o.Spec.Repo != nil {
		if o.Spec.Repo.ReviewMode != "" {
			env = append(env, corev1.EnvVar{
				Name:  "REVIEW_MODE",
				Value: o.Spec.Repo.ReviewMode,
			})
		}
		if o.Spec.Repo.PRMode != "" {
			env = append(env, corev1.EnvVar{
				Name:  "PR_MODE",
				Value: o.Spec.Repo.PRMode,
			})
		}
		if o.Spec.Repo.IssueMode != "" {
			env = append(env, corev1.EnvVar{
				Name:  "ISSUE_MODE",
				Value: o.Spec.Repo.IssueMode,
			})
		}
	}

	if o.Spec.MaxActiveReviews != nil {
		env = append(env, corev1.EnvVar{
			Name:  "MAX_ACTIVE_REVIEWS",
			Value: fmt.Sprintf("%d", *o.Spec.MaxActiveReviews),
		})
	}
	if o.Spec.MaxActiveIssues != nil {
		env = append(env, corev1.EnvVar{
			Name:  "MAX_ACTIVE_ISSUES",
			Value: fmt.Sprintf("%d", *o.Spec.MaxActiveIssues),
		})
	}
	if o.Spec.SandboxEvictionAge != "" {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_EVICTION_AGE",
			Value: o.Spec.SandboxEvictionAge,
		})
	}
	if o.Spec.SandboxIdleTimeout != "" {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_IDLE_TIMEOUT",
			Value: o.Spec.SandboxIdleTimeout,
		})
	}
	if o.Spec.PRInactivityTimeout != nil {
		env = append(env, corev1.EnvVar{
			Name:  "PR_INACTIVITY_TIMEOUT",
			Value: o.Spec.PRInactivityTimeout.Duration.String(),
		})
	}
	if o.Spec.MinNumber != nil {
		env = append(env, corev1.EnvVar{
			Name:  "MIN_NUMBER",
			Value: fmt.Sprintf("%d", *o.Spec.MinNumber),
		})
	}
	if !o.Spec.WorkspaceDiskSize.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "WORKSPACE_DISK_SIZE",
			Value: o.Spec.WorkspaceDiskSize.String(),
		})
	}
	if !o.Spec.EphemeralStorage.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "EPHEMERAL_STORAGE",
			Value: o.Spec.EphemeralStorage.String(),
		})
	}
	if !o.Spec.SandboxCPURequest.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_CPU_REQUEST",
			Value: o.Spec.SandboxCPURequest.String(),
		})
	}
	if !o.Spec.SandboxCPULimit.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_CPU_LIMIT",
			Value: o.Spec.SandboxCPULimit.String(),
		})
	}
	if !o.Spec.SandboxMemoryRequest.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_MEMORY_REQUEST",
			Value: o.Spec.SandboxMemoryRequest.String(),
		})
	}
	if !o.Spec.SandboxMemoryLimit.IsZero() {
		env = append(env, corev1.EnvVar{
			Name:  "SANDBOX_MEMORY_LIMIT",
			Value: o.Spec.SandboxMemoryLimit.String(),
		})
	}
	if o.Spec.Image != "" {
		env = append(env, corev1.EnvVar{
			Name:  "FACTORY_IMAGE",
			Value: o.Spec.Image,
		})
	}
	if len(o.Spec.Secrets) > 0 {
		secretsJSON, err := json.Marshal(o.Spec.Secrets)
		if err == nil {
			env = append(env, corev1.EnvVar{
				Name:  "FACTORY_SECRETS",
				Value: string(secretsJSON),
			})
		}
	}
	if len(o.Spec.Env) > 0 {
		envJSON, err := json.Marshal(o.Spec.Env)
		if err == nil {
			env = append(env, corev1.EnvVar{
				Name:  "FACTORY_ENV",
				Value: string(envJSON),
			})
		}
	}

	ephemeralQty := resource.MustParse("10Gi")
	if !o.Spec.EphemeralStorage.IsZero() {
		ephemeralQty = o.Spec.EphemeralStorage
	}

	diskQty := resource.MustParse("10Gi")
	if !o.Spec.WorkspaceDiskSize.IsZero() {
		diskQty = o.Spec.WorkspaceDiskSize
	}

	volumes := []corev1.Volume{}
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "workspaces-pvc",
			MountPath: "/workspaces",
		},
	}

	if hasTokenScript {
		mode := int32(0755)
		volumes = append(volumes, corev1.Volume{
			Name: "tokenscript-vol",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  "tokenscript",
					DefaultMode: &mode,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "tokenscript-vol",
			MountPath: "/etc/tokenscript",
		})
		env = append(env, corev1.EnvVar{
			Name:  "TOKENSCRIPT_DIR",
			Value: "/etc/tokenscript",
		})
	}

	// Inject CA cert volume
	volumes = append(volumes, corev1.Volume{
		Name: "ca-cert",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "github-portal-ca",
				Optional:   ptr.To(true),
			},
		},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "ca-cert",
		MountPath: "/etc/github-portal/ca",
		ReadOnly:  true,
	})

	return &sandboxapi.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxapi.GroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"sandbox-type":                        "agent",
				"overseer.gemini.google.com/overseer": o.Name,
			},
		},
		Spec: sandboxapi.SandboxSpec{
			Replicas: ptr.To(int32(1)),
			PodTemplate: sandboxapi.PodTemplate{
				ObjectMeta: sandboxapi.PodMetadata{
					Labels: map[string]string{
						"sandbox":      name,
						"sandbox-type": "agent",
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "overseer",
					Containers: []corev1.Container{
						{
							Name:    "overseer",
							Image:   image,
							Command: []string{"/app/bootstrap.sh"},
							Env:     env,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:              resource.MustParse("1000m"),
									corev1.ResourceMemory:           resource.MustParse("1Gi"),
									corev1.ResourceEphemeralStorage: ephemeralQty,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:              resource.MustParse("2000m"),
									corev1.ResourceMemory:           resource.MustParse("2Gi"),
									corev1.ResourceEphemeralStorage: ephemeralQty,
								},
							},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
			VolumeClaimTemplates: []sandboxapi.PersistentVolumeClaimTemplate{
				{
					EmbeddedObjectMetadata: sandboxapi.EmbeddedObjectMetadata{
						Name: "workspaces-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: diskQty,
							},
						},
					},
				},
			},
		},
	}
}
