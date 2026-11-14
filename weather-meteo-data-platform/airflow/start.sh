#!/usr/bin/env bash
set -euo pipefail

: "${_AIRFLOW_WWW_USER_USERNAME:?Set the Airflow admin username}"

airflow db migrate

users=$(airflow users list --output json)
admin_exists=$(printf '%s' "$users" | python3 -c '
import json, sys
username = sys.argv[1]
print(any(user["username"] == username for user in json.load(sys.stdin)))
' "$_AIRFLOW_WWW_USER_USERNAME")
if [[ "$admin_exists" == False ]]; then
    : "${_AIRFLOW_WWW_USER_PASSWORD:?Set the Airflow admin password}"
    airflow users create --username "$_AIRFLOW_WWW_USER_USERNAME" \
        --password "$_AIRFLOW_WWW_USER_PASSWORD" --firstname Airflow --lastname Admin \
        --role Admin --email admin@example.invalid
fi

pids=()
cleanup() {
    if (( ${#pids[@]} > 0 )); then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

for component in scheduler triggerer dag-processor api-server; do
    airflow "$component" &
    pids+=("$!")
done

# These are long-running services. Treat any premature exit as a failed container.
status=0
wait -n "${pids[@]}" || status=$?
echo "An Airflow component exited (status: $status)." >&2
exit "$((status == 0 ? 1 : status))"
