package k8s

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBootstrapNamespace_DoesNotBindClusterRoleBindings(t *testing.T) {
	ctx := context.Background()

	reviewCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "review-sandbox"},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "review-sandbox",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	issueCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "issue-sandbox"},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "issue-sandbox",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	ghSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GithubSecretName,
			Namespace: SystemNamespace,
		},
		Data: map[string][]byte{"pat": []byte("default-token")},
	}

	clientset := fake.NewSimpleClientset(reviewCRB, issueCRB, ghSecret)

	tenantNS := "tenant-a"
	if err := BootstrapNamespace(ctx, clientset, tenantNS); err != nil {
		t.Fatalf("BootstrapNamespace(%q) failed: %v", tenantNS, err)
	}

	ns, err := clientset.CoreV1().Namespaces().Get(ctx, tenantNS, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected namespace %q to exist: %v", tenantNS, err)
	}
	if got := ns.Labels["review.gemini.google.com/tenant"]; got != tenantNS {
		t.Errorf("namespace tenant label = %q, want %q", got, tenantNS)
	}

	if _, err := clientset.CoreV1().Secrets(tenantNS).Get(ctx, GithubSecretName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected secret %q to be copied to %q: %v", GithubSecretName, tenantNS, err)
	}

	crbs, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing ClusterRoleBindings: %v", err)
	}
	for _, crb := range crbs.Items {
		if len(crb.Subjects) != 0 {
			t.Errorf("ClusterRoleBinding %q should have no tenant subjects added, got: %+v", crb.Name, crb.Subjects)
		}
	}
}
