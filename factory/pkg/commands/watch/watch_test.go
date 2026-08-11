package watch

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
)

func TestNewWatchCommand(t *testing.T) {
	ctx := context.Background()
	resolveRoot := func(cmd *cobra.Command) (*RootOptions, error) {
		return &RootOptions{
			Namespace:  "default",
			Image:      "test-image",
			SecretName: "test-secret",
		}, nil
	}

	cmd := NewWatchCommand(ctx, resolveRoot)
	if cmd == nil {
		t.Fatal("expected NewWatchCommand to return non-nil *cobra.Command")
	}

	if cmd.Use != "watch" {
		t.Errorf("expected command Use to be 'watch', got %q", cmd.Use)
	}

	if cmd.Flag("repo") == nil {
		t.Errorf("expected --repo flag to be registered")
	}
	if cmd.Flag("mode") == nil {
		t.Errorf("expected --mode flag to be registered")
	}
	if cmd.Flag("queue-dir") == nil {
		t.Errorf("expected --queue-dir flag to be registered")
	}
}
