#!/bin/bash
set -e
set -o pipefail


# It expects the following environment variables to be set:
# - GEMINI_API_KEY
# - GITHUB_USER_TOKEN
# - REPO_NAME
# - CLONE_URL
# - PROMPT_FILE
# - GITHUB_USER_ID
# - GITHUB_USER_EMAIL
# - GITHUB_USER_NAME
# - PR_NUMBER
# - MODELS

export GITHUB_USER_TOKEN="${GITHUB_USER_TOKEN:-${GITHUB_TOKEN}}"
if [ -z "$GITHUB_USER_TOKEN" ]; then
    # Try other common names
    GITHUB_USER_TOKEN="${MANUAL_PAT:-${OAUTH_PAT}}"
fi

if [ -n "${GITHUB_BOT_LOGIN}" ]; then
    if [ -n "${GITHUB_BOT_TOKEN}" ] || [ -n "${GITHUB_BOT_OAUTH_PAT}" ] || [ -n "${GITHUB_BOT_MANUAL_PAT}" ]; then
        GITHUB_USER_TOKEN="${GITHUB_BOT_TOKEN:-${GITHUB_BOT_MANUAL_PAT:-${GITHUB_BOT_OAUTH_PAT}}}"
    fi
fi

function publishReviewOutput() {
    local output_name="$1"
    local task_dir
    task_dir="$(dirname "${PROMPT_FILE}")"
    local review_file="${task_dir}/${output_name}"

    if [ "${PUBLISH_POLICY}" != "yes" ] && [ "${PUBLISH_POLICY}" != "draft" ]; then
        return 0
    fi

    if [ ! -s "${review_file}" ]; then
        echo "Error: review output file ${review_file} is empty or missing" >&2
        return 1
    fi

    echo "Publishing review to GitHub from sandbox (policy=${PUBLISH_POLICY})..."
    if command -v factory >/dev/null 2>&1 && factory pr publish-review --help >/dev/null 2>&1; then
        GITHUB_TOKEN="${GITHUB_USER_TOKEN}" factory pr publish-review \
            --pr-url "${PR_URL}" \
            --input "${review_file}" \
            --publish "${PUBLISH_POLICY}"
        touch "${task_dir}/review-published"
        return 0
    fi

    # Fallback for sandbox images built before `factory pr publish-review` was added.
    local payload_file="${task_dir}/review-payload.json"
    python3 - "${review_file}" "${PUBLISH_POLICY}" "${payload_file}" << 'PYEOF'
import json, sys, yaml

raw = open(sys.argv[1]).read().strip()
policy = sys.argv[2].strip().lower()
out_path = sys.argv[3]

lines = [l for l in raw.splitlines() if not l.strip().startswith("```")]
cleaned = []
found = False
for l in lines:
    if not found and l.strip().startswith("review:"):
        found = True
    if found:
        cleaned.append(l)
text = "\n".join(cleaned if found else lines)

data = yaml.safe_load(text)
if not isinstance(data, dict) or "review" not in data or not isinstance(data["review"], dict):
    raise SystemExit("parsed review output missing 'review' key")

rev = data["review"]
payload = {}
if rev.get("body"):
    payload["body"] = str(rev["body"])
if policy != "draft":
    payload["event"] = "COMMENT"

comments = []
for c in (rev.get("comments") or []):
    if not isinstance(c, dict):
        continue
    item = {}
    if c.get("path") and str(c["path"]).strip():
        item["path"] = str(c["path"]).strip()
    if c.get("position") is not None:
        item["position"] = int(c["position"])
    if c.get("body") is not None:
        item["body"] = str(c["body"])
    if c.get("line") is not None:
        item["line"] = int(c["line"])
    side = str(c.get("side") or "").strip().upper()
    if side in ("LEFT", "RIGHT"):
        item["side"] = side
    if c.get("start_line") is not None:
        item["start_line"] = int(c["start_line"])
    start_side = str(c.get("start_side") or "").strip().upper()
    if start_side in ("LEFT", "RIGHT"):
        item["start_side"] = start_side
    comments.append(item)
if comments:
    payload["comments"] = comments

with open(out_path, "w") as f:
    json.dump(payload, f)
PYEOF

    local owner="${REPO_OWNER}"
    if [ -z "${owner}" ] && [ -n "${PR_URL}" ]; then
        owner="$(echo "${PR_URL}" | awk -F'/' '{print $(NF-3)}')"
    fi
    GH_TOKEN="${GITHUB_USER_TOKEN}" gh api \
        --method POST \
        "repos/${owner}/${REPO_NAME}/pulls/${PR_NUMBER}/reviews" \
        --input "${payload_file}" >/dev/null
    touch "${task_dir}/review-published"
}

# Main execution
setupGit
setupGitRepos
# HACK: Avoid git lock issues
sleep 5
checkoutPRBranch
configureGemini
runEngine review-output.txt
publishReviewOutput review-output.txt
