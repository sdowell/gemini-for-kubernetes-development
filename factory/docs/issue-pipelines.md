# Issue pipelines

An issue pipeline runs a fixed sequence of steps against a GitHub repository,
one sub-issue at a time. Each step is an ordinary issue that the coder bots
pick up, fix and open a PR for, as they would any other issue. A
deterministic reconciler in `factory watch` sequences the steps. No LLM or
sandbox is involved, and GitHub is the only state store. For the design, see
[design/issue-pipelines.md](../design/issue-pipelines.md).

## 1. Define the pipeline

Commit `.agents/pipelines/<name>.yaml` to the repository the pipeline runs
against. The file's name must match `metadata.name`.

```yaml
apiVersion: factory.gemini.google.com/v1alpha1
kind: IssuePipeline
metadata:
  name: kcc-greenfield
spec:
  params:
    - name: kind                       # {{ .Params.kind }}
      pattern: '^[A-Z][A-Za-z0-9]{1,62}$'
      example: SpannerInstanceConfig
    - name: version
      required: false
      enum: [v1alpha1, v1beta1]
      default: v1alpha1
  defaults:
    labels: [greenfield]               # the trigger label is always added
  steps:
    - id: gen-types
      title: 'Greenfield: types for {{ .Params.kind }}'
      labels: [step/gen-types]
      review: |                        # optional: labels the child <trigger>/review
        Check that the types follow the KRM API conventions.
      body: |
        Generate the types for {{ .Params.kind }} at {{ .Params.version }}.
    - id: controller
      title: 'Greenfield: controller for {{ .Params.kind }}'
      body: |
        The types were added in #{{ (step "gen-types").Issue }}.
```

| Field | Notes |
| :--- | :--- |
| `params[]` | Each param needs an anchored `pattern` (`^…$`), an `enum`, or both. Params are required by default. A `default` is only allowed on an optional param (`required: false`). An omitted optional param with no default is `""`. |
| `steps[].id` | Lowercase, unique and stable. Children are matched to steps by `id`, so renaming one orphans its child. |
| `title`, `body`, `review` | Go templates with `missingkey=error`. They can use `.Params.<name>`, `step "<id>"` (an earlier step's `.Issue` and `.URL`), and these helpers: `lower`, `upper`, `trim`, `trimPrefix`, `trimSuffix`, `replace`, `hasPrefix`, `hasSuffix`, `contains`, `kebab` and `snake`. |
| `labels`, `assignees` | Labels are added to `defaults.labels` and the trigger label. Assignees default to the watch's target assignee, and are only re-applied while a child has no assignee at all. |
| `completion.closeOnLinkedPRMerge` | On by default. The open child is closed as completed when a bot PR that references it merges. |
| `dependsOn` | Reserved; it is rejected in v1. |

Check definitions in CI:

```sh
factory pipeline validate .agents/pipelines/
```

## 2. Start a pipeline

File a parent issue that the watch acts on (trigger-labelled, or assigned to
a bot) and give it exactly one block:

````markdown
Onboard SpannerInstanceConfig as a greenfield resource.

```factory-pipeline
pipeline: kcc-greenfield
params:
  kind: SpannerInstanceConfig
```
````

The issue scanner labels the parent `<trigger>/pipeline` and never queues it
as a task. The reconciler then:

1. Creates the first step's child, links it as a sub-issue, and labels and
   assigns it.
2. Waits for that child to close. Closed as completed means `Succeeded`;
   closed as not planned or as a duplicate means `Skipped`.
3. Creates the next step's child, and so on.
4. Closes the parent as completed after the last step.

A status comment on the parent shows the phase and one row per step, and is
edited in place.

## 3. Operate

| You want to | Do |
| :--- | :--- |
| Pause | Add the stop label (`<trigger>/stop` or `overseer/stop`) to the parent. |
| Skip a step | Close its child as *not planned*. |
| Redo a step | Reopen its child. Its step becomes `Active` again. |
| Cancel | Close the parent yourself. Open children are left open. |
| Change params | You can't. Editing the block blocks the pipeline until it is restored; file a new parent instead. |
| Bind an existing issue to a step | `factory pipeline adopt --url <parent> --step <id> --issue <n>` |
| Preview what a pass would do | `factory pipeline reconcile --url <parent> --dry-run` |

If the parent is closed by a commit or PR (a stray `Fixes #<parent>`) while
steps remain, the reconciler reopens it with a comment. When the definition
or the params don't validate, the parent is labelled
`<trigger>/pipeline-invalid`, the status comment says why, and nothing is
created until it is fixed.

## 4. Enable it in `factory watch`

Pipelines are off by default while they roll out:

```sh
factory watch --repo owner/repo --pipeline-mode dryrun   # log what it would do
factory watch --repo owner/repo --pipeline-mode enabled
```

You can also set the `PIPELINE_MODE` environment variable or, for an Overseer,
`spec.repo.pipelineMode`. Pipelines need issue scanning on too. The
reconciler runs at startup, every 5 minutes, and as soon as a child's closing
is reported. It writes as the watch's GitHub identity.

> [!NOTE]
> Only one watcher per repository should run with `--pipeline-mode
> enabled`. Run any others with `dryrun`.
