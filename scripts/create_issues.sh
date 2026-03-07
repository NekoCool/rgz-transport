#!/usr/bin/env bash
set -euo pipefail

PROJECT_TITLE="rgz-transport Restart 2026"
DRY_RUN=0
REPO=""
SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LABELS_FILE="${SCRIPT_DIR}/issues/labels.yaml"
ISSUES_FILE="${SCRIPT_DIR}/issues/issues.yaml"

usage() {
  cat <<USAGE
Create the issues via GitHub CLI.

Usage:
  ./$SCRIPT_NAME [--repo owner/name] [--project "Project Title"] [--labels-file path] [--issues-file path] [--dry-run]

Options:
  --repo       Target repository (default: current gh repo)
  --project    GitHub Project title (default: rgz-transport Restart 2026)
  --labels-file YAML file containing labels (default: ./issues/labels.yaml)
  --issues-file YAML file containing issues (default: ./issues/issues.yaml)
  --dry-run    Print commands instead of creating issues
  -h, --help   Show this help
USAGE
}

while (($#)); do
  case "$1" in
    --repo)
      REPO="${2:-}"
      shift 2
      ;;
    --project)
      PROJECT_TITLE="${2:-}"
      shift 2
      ;;
    --labels-file)
      LABELS_FILE="${2:-}"
      shift 2
      ;;
    --issues-file)
      ISSUES_FILE="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v gh >/dev/null 2>&1; then
  echo "Error: gh command is required." >&2
  exit 1
fi

if ! command -v ruby >/dev/null 2>&1; then
  echo "Error: ruby command is required for YAML parsing." >&2
  exit 1
fi

if [[ ! -f "$LABELS_FILE" ]]; then
  echo "Error: labels file not found: $LABELS_FILE" >&2
  exit 1
fi

if [[ ! -f "$ISSUES_FILE" ]]; then
  echo "Error: issues file not found: $ISSUES_FILE" >&2
  exit 1
fi

if [[ -z "$REPO" ]]; then
  REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
fi

run_or_echo() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] %q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

ensure_label() {
  local name="$1"
  local color="$2"
  local description="$3"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    run_or_echo gh label create "$name" \
      --repo "$REPO" \
      --color "$color" \
      --description "$description"
    return 0
  fi

  # Try create first; if it already exists, update it.
  if ! gh label create "$name" \
    --repo "$REPO" \
    --color "$color" \
    --description "$description" >/dev/null 2>&1; then
    gh label edit "$name" \
      --repo "$REPO" \
      --color "$color" \
      --description "$description" >/dev/null
  fi
}

create_issue() {
  local title="$1"
  local body="$2"
  shift 2
  local labels=("$@")
  local cmd=(
    gh issue create
    --repo "$REPO"
    --title "$title"
    --body "$body"
    --project "$PROJECT_TITLE"
  )
  local label

  for label in "${labels[@]}"; do
    cmd+=(--label "$label")
  done

  run_or_echo "${cmd[@]}"
}

emit_yaml_labels() {
  ruby - "$LABELS_FILE" <<'RUBY'
require 'yaml'

path = ARGV[0]
data = YAML.load_file(path)
labels = data.is_a?(Hash) ? data['labels'] : data
abort("Error: labels YAML must be an array or contain top-level 'labels' array.") unless labels.is_a?(Array)

labels.each_with_index do |label, idx|
  abort("Error: labels[#{idx}] must be an object.") unless label.is_a?(Hash)
  name = label['name']
  color = label['color']
  description = label['description']
  abort("Error: labels[#{idx}] requires name/color/description.") if [name, color, description].any? { |v| v.nil? || v.to_s.empty? }
  print name.to_s, "\0", color.to_s, "\0", description.to_s, "\0"
end
RUBY
}

emit_yaml_issues() {
  ruby - "$ISSUES_FILE" <<'RUBY'
require 'yaml'

path = ARGV[0]
data = YAML.load_file(path)
issues = data.is_a?(Hash) ? data['issues'] : data
abort("Error: issues YAML must be an array or contain top-level 'issues' array.") unless issues.is_a?(Array)

issues.each_with_index do |issue, idx|
  abort("Error: issues[#{idx}] must be an object.") unless issue.is_a?(Hash)
  title = issue['title']
  body = issue['body']
  labels = issue['labels']
  abort("Error: issues[#{idx}] requires title/body/labels.") if title.to_s.empty? || body.to_s.empty? || !labels.is_a?(Array) || labels.empty?
  print title.to_s, "\0", body.to_s, "\0", labels.join(","), "\0"
end
RUBY
}

validate_yaml_files() {
  emit_yaml_labels >/dev/null
  emit_yaml_issues >/dev/null
}

ensure_labels_from_yaml() {
  local name color description

  while IFS= read -r -d '' name && IFS= read -r -d '' color && IFS= read -r -d '' description; do
    ensure_label "$name" "$color" "$description"
  done < <(emit_yaml_labels)
}

create_issues_from_yaml() {
  local title body labels_csv
  local labels=()
  local old_ifs

  while IFS= read -r -d '' title && IFS= read -r -d '' body && IFS= read -r -d '' labels_csv; do
    old_ifs="$IFS"
    IFS=',' read -r -a labels <<<"$labels_csv"
    IFS="$old_ifs"

    create_issue "$title" "$body" "${labels[@]}"
  done < <(emit_yaml_issues)
}

validate_yaml_files

echo "Ensuring labels exist in ${REPO}..."
ensure_labels_from_yaml

echo "Creating issues in ${REPO}..."
create_issues_from_yaml

echo "Issue and label setup completed for repo: ${REPO}"
