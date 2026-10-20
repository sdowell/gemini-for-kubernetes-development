package issues

import (
	"context"

	githubv39 "github.com/google/go-github/v39/github"
	"k8s.io/klog/v2"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/commands/watch/conventions"
)

// routePipelineParent hands an issue pipeline parent - an issue whose body
// has a factory-pipeline block - to the pipeline reconciler
// (design/issue-pipelines.md §7.1): it is labelled '<trigger>/pipeline',
// which is how the reconciler lists parents, and is never queued as a task
// or treated as a workflow. Its steps' children are ordinary issues, which
// this scanner picks up as they are opened.
func (s *Scanner) routePipelineParent(ctx context.Context, issue *githubv39.Issue) {
	num := issue.GetNumber()
	// A fix queued before the block was added must not run.
	_ = s.queue.RemovePendingTasksForNumber(num)
	if !s.cfg.LabelPipelines {
		klog.Infof("Skipping issue #%d because it is a pipeline parent (pipelines are disabled in this watcher)", num)
		return
	}
	if conventions.HasPipelineLabel(issue.Labels, s.cfg.TriggerLabel) {
		klog.V(2).Infof("Skipping issue #%d because it is a pipeline parent", num)
		return
	}
	label := conventions.PipelineLabel(s.cfg.TriggerLabel)
	if s.cfg.DryRun {
		klog.Infof("[DRYRUN] Would label pipeline parent #%d '%s'", num, label)
		return
	}
	klog.Infof("Labelling pipeline parent #%d '%s' for the pipeline reconciler", num, label)
	if err := s.gh.AddLabels(ctx, num, []string{label}); err != nil {
		klog.Errorf("Failed to label pipeline parent #%d '%s': %v", num, label, err)
		return
	}
	issue.Labels = append(issue.Labels, &githubv39.Label{Name: githubv39.String(label)})
}
