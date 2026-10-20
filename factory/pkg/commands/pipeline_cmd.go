package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/config"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/github"
	"github.com/gke-labs/gemini-for-kubernetes-development/factory/pkg/pipeline"
)

// NewPipelineCommand is `factory pipeline`: deterministic multi-step issue
// pipelines (design/issue-pipelines.md).
func NewPipelineCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Validate, reconcile and adopt issues into deterministic multi-step issue pipelines",
		Long: `An IssuePipeline is an ordered list of steps, each a templated GitHub issue,
defined in the watched repository under ` + pipeline.Dir + `/<name>.yaml.

A parent issue starts one with a fenced block in its body:

  ` + "```" + `factory-pipeline
  pipeline: <name>
  params:
    <param>: <value>
  ` + "```" + `

The reconciler opens step N as a sub-issue of the parent, waits for it to
close, opens step N+1, and closes the parent after the last step. factory
watch runs it with --pipeline-mode enabled. See factory/design/issue-pipelines.md.`,
	}
	cmd.AddCommand(newPipelineValidateCommand())
	cmd.AddCommand(newPipelineReconcileCommand(ctx))
	cmd.AddCommand(newPipelineAdoptCommand(ctx))
	return cmd
}

func newPipelineValidateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <file or directory>...",
		Short: "Check pipeline definitions statically; writes nothing",
		Long: `Check pipeline definitions statically, as CI should: the schema is strict
(no unknown fields), step ids are unique, every param is constrained by an
anchored pattern or an enum, every default is on an optional param and
satisfies its constraint, and templates parse and reference only earlier
steps. Every step is then rendered with each param's example, and again with
every optional param omitted. A directory is checked file by file (*.yaml,
*.yml); a file's name must match its metadata.name.`,
		Example: `  factory pipeline validate .agents/pipelines/
  factory pipeline validate .agents/pipelines/kcc-greenfield.yaml`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			files, err := pipelineFiles(args)
			if err != nil {
				return err
			}
			out := c.OutOrStdout()
			failed := 0
			for _, f := range files {
				if err := validatePipelineFile(f); err != nil {
					failed++
					fmt.Fprintf(out, "FAIL %s\n", f)
					for _, line := range strings.Split(err.Error(), "\n") {
						fmt.Fprintf(out, "  %s\n", line)
					}
					continue
				}
				fmt.Fprintf(out, "ok   %s\n", f)
			}
			if failed > 0 {
				return fmt.Errorf("%d of %d pipeline definitions failed validation", failed, len(files))
			}
			return nil
		},
	}
}

// pipelineFiles expands directories into the definitions they hold.
func pipelineFiles(args []string) ([]string, error) {
	var files []string
	for _, a := range args {
		st, err := os.Stat(a)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			files = append(files, a)
			continue
		}
		entries, err := os.ReadDir(a)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() && (strings.HasSuffix(e.Name(), ".yaml") || strings.HasSuffix(e.Name(), ".yml")) {
				files = append(files, filepath.Join(a, e.Name()))
			}
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no pipeline definitions in %s", strings.Join(args, ", "))
	}
	return files, nil
}

func validatePipelineFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	d, err := pipeline.Parse(data)
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".yaml"), ".yml")
	if base != d.Metadata.Name {
		return fmt.Errorf("metadata.name is %q, but the file is %s: a parent names the pipeline by its file, %s", d.Metadata.Name, filepath.Base(path), pipeline.Path(d.Metadata.Name))
	}
	return nil
}

// pipelineClientFlags are the flags reconcile and adopt share.
type pipelineClientFlags struct {
	url      string
	trigger  string
	assignee string
	dryRun   bool
}

func (f *pipelineClientFlags) add(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.url, "url", "", "the parent issue's URL")
	cmd.Flags().StringVar(&f.trigger, "trigger-label", "", "the label that hands a child to the coder bots (default: the factory config's triggerLabel, as watch uses)")
	cmd.Flags().StringVar(&f.assignee, "assignee", "", "who children are assigned to when their step names nobody (default: the account you run as)")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "write nothing; print the status and the actions a real run would apply")
	_ = cmd.MarkFlagRequired("url")
}

// resolve parses the URL and builds the client and options.
func (f *pipelineClientFlags) resolve(ctx context.Context, out func(format string, args ...any)) (*github.Client, int, pipeline.Options, error) {
	item, err := parseGitHubItemURL(f.url)
	if err != nil {
		return nil, 0, pipeline.Options{}, err
	}
	if item.IsPR {
		return nil, 0, pipeline.Options{}, fmt.Errorf("%s is a pull request; a pipeline parent is an issue", f.url)
	}
	// A dry run reads through a client that refuses every write, so a
	// preview cannot change the repository.
	newClient := github.NewClient
	if f.dryRun {
		newClient = github.NewReadOnlyClient
	}
	rc, err := newClient(ctx)
	if err != nil {
		return nil, 0, pipeline.Options{}, err
	}
	gh := github.ForRepo(rc, item.Owner, item.Repo)
	login, err := gh.AuthenticatedLogin(ctx)
	if err != nil {
		return nil, 0, pipeline.Options{}, err
	}
	trigger := f.trigger
	if trigger == "" {
		trigger = watchTriggerLabel()
	}
	assignee := f.assignee
	if assignee == "" {
		assignee = login
	}
	opts := pipeline.Options{
		Repo:           item.Owner + "/" + item.Repo,
		TriggerLabel:   trigger,
		BotLogin:       login,
		BotLogins:      configuredBotLogins(),
		TargetAssignee: assignee,
		DryRun:         f.dryRun,
		Logf:           out,
	}
	return gh, item.Number, opts, nil
}

// configuredBotLogins are the factory config's role accounts, whose merged
// PRs complete a child.
func configuredBotLogins() []string {
	cfg, err := config.LoadConfig()
	if err != nil || cfg == nil {
		return nil
	}
	var out []string
	for _, r := range cfg.Roles {
		for _, u := range r.Users {
			if u != "" && !slices.Contains(out, u) {
				out = append(out, u)
			}
		}
	}
	sort.Strings(out)
	return out
}

func newPipelineReconcileCommand(ctx context.Context) *cobra.Command {
	var f pipelineClientFlags
	cmd := &cobra.Command{
		Use:   "reconcile --url <parent> [--dry-run]",
		Short: "Run one reconcile pass over a pipeline parent",
		Long: `Run one reconcile pass over a pipeline parent, as factory watch does for every
parent labelled <trigger>/pipeline: observe the parent and its children on
GitHub, decide, and apply. A pass opens at most one child, repairs unlinked
children and duplicates, re-applies missing labels and assignees, closes a
child whose bot PR merged, closes the parent after the last step, and keeps
the status comment up to date.

With --dry-run it runs the same observe and reconcile code with a read-only
client, then prints the status and the actions a real run would apply.

The reconciler writes as the account you run as; use --dry-run, or a bot
token, for local runs.`,
		Example: `  factory pipeline reconcile --url https://github.com/owner/repo/issues/123 --dry-run
  factory pipeline reconcile --url https://github.com/owner/repo/issues/123`,
		Args: cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			out := c.OutOrStdout()
			logf := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
			gh, parent, opts, err := f.resolve(ctx, logf)
			if err != nil {
				return err
			}
			res, err := pipeline.Sync(ctx, gh, parent, opts)
			if err != nil {
				return err
			}
			if res.Phase == pipeline.PhaseInvalid {
				return fmt.Errorf("the pipeline is invalid: %s", res.Reason)
			}
			return nil
		},
	}
	f.add(cmd)
	return cmd
}

func newPipelineAdoptCommand(ctx context.Context) *cobra.Command {
	var f pipelineClientFlags
	var step string
	var issue int
	cmd := &cobra.Command{
		Use:   "adopt --url <parent> --step <id> --issue <n> [--dry-run]",
		Short: "Bind an existing issue to a step of a pipeline parent",
		Long: `Bind an existing issue to a step of a pipeline parent: add the step's hidden
marker to the issue's body and link it as a sub-issue of the parent. The
next reconcile pass treats it as the step's child. Existing issues are bound
to steps only this way, never from an issue body.`,
		Example: `  factory pipeline adopt --url https://github.com/owner/repo/issues/123 --step gen-types --issue 456 --dry-run`,
		Args:    cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			out := c.OutOrStdout()
			logf := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
			gh, parent, opts, err := f.resolve(ctx, logf)
			if err != nil {
				return err
			}
			return pipeline.Adopt(ctx, gh, parent, step, issue, opts)
		},
	}
	f.add(cmd)
	cmd.Flags().StringVar(&step, "step", "", "the step's id")
	cmd.Flags().IntVar(&issue, "issue", 0, "the number of the issue to adopt")
	_ = cmd.MarkFlagRequired("step")
	_ = cmd.MarkFlagRequired("issue")
	return cmd
}
