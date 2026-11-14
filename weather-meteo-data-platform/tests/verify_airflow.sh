#!/usr/bin/env bash
set -euo pipefail

platform_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
airflow_image="${1:?Usage: bash tests/verify_airflow.sh <AIRFLOW_IMAGE> [POSTGRES_IMAGE]}"
postgres_image="${2:-postgres:16.16-alpine}"
prefix="meteo-airflow-test-$$-${RANDOM}"
network="$prefix-net"
database="$prefix-db"
airflow="$prefix-app"

docker network create --internal "$network" >/dev/null
cleanup() {
    docker rm -f -v "$airflow" "$database" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Include reserved URL characters in the password to verify SQLAlchemy URL construction.
fixture_password='fixture@with:/space and punctuation'
docker run -d --name "$database" --network "$network" --network-alias postgres_db \
    -e POSTGRES_USER=airflow_smoke -e "POSTGRES_PASSWORD=$fixture_password" -e POSTGRES_DB=airflow_smoke \
    "$postgres_image" >/dev/null
for ((attempt=0; attempt<60; attempt++)); do
    if docker exec "$database" pg_isready -h 127.0.0.1 -U airflow_smoke -d airflow_smoke >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
if (( attempt == 60 )); then
    docker logs "$database"
    exit 1
fi

docker run -d --name "$airflow" --network "$network" \
    --mount "type=bind,src=$platform_dir/airflow/dags,dst=/opt/airflow/dags,readonly" \
    -e AIRFLOW_DB_USER=airflow_smoke -e "AIRFLOW_DB_PASSWORD=$fixture_password" -e AIRFLOW_DB_NAME=airflow_smoke \
    -e 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD=python3 /opt/airflow/metadata_url.py' \
    -e '_AIRFLOW_WWW_USER_USERNAME=meteo_test' -e '_AIRFLOW_WWW_USER_PASSWORD=fixture pass' \
    -e 'AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager' \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=False -e AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True \
    -e AIRFLOW__API__SECRET_KEY=fixture-airflow-api-secret-key \
    -e AIRFLOW__API_AUTH__JWT_SECRET=fixture-airflow-jwt-signing-key-for-isolated-smoke-checks-only-ci \
    "$airflow_image" bash /opt/airflow/start.sh >/dev/null

for ((attempt=0; attempt<45; attempt++)); do
    if [[ "$(docker inspect -f '{{.State.Status}}' "$airflow")" != running ]]; then
        break
    fi
    if docker exec "$airflow" python3 -c '
import json, urllib.request
with urllib.request.urlopen("http://127.0.0.1:8080/api/v2/monitor/health", timeout=3) as response:
    data = json.load(response)
assert all(data.get(c, {}).get("status") == "healthy"
           for c in ("metadatabase", "scheduler", "triggerer", "dag_processor")), data
print(data)
' 2>/dev/null; then
        docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$airflow" airflow dags list-import-errors -o json |
            python3 -c 'import json, sys; assert not json.load(sys.stdin), "Airflow reported DAG import errors"'
        # A healthy processor can report its first heartbeat before it registers the DAG.
        if ! docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$airflow" airflow dags list -o json |
            python3 -c 'import json, sys; sys.exit(not any(row["dag_id"] == "open_meteo_api_pipeline_orchestrator" and str(row["is_paused"]).lower() == "true" for row in json.load(sys.stdin)))'; then
            sleep 2
            continue
        fi
        docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$airflow" airflow users list -o json |
            python3 -c 'import json, sys; assert any(row["username"] == "meteo_test" for row in json.load(sys.stdin)), "FAB admin is missing"'
        exit 0
    fi
    sleep 2
done
docker logs "$airflow"
exit 1
