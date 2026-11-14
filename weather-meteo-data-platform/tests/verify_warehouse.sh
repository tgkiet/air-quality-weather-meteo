#!/usr/bin/env bash
set -euo pipefail

platform_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
airflow_image="${1:?Usage: bash tests/verify_warehouse.sh <AIRFLOW_IMAGE> [POSTGRES_IMAGE]}"
postgres_image="${2:-postgres:16-alpine}"
app_password="fixture'with-quote"
prefix="meteo-warehouse-test-$$-${RANDOM}"
network="$prefix-net"
database="$prefix-db"

docker network create --internal "$network" >/dev/null
cleanup() {
    docker rm -f -v "$database" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

init_mounts=(--mount "type=bind,src=$platform_dir/src/scripts/init_dbs.sh,dst=/docker-entrypoint-initdb.d/init_dbs.sh,readonly")
shopt -s nullglob
for migration in "$platform_dir"/src/scripts/migrations/*.sql; do
    init_mounts+=(--mount "type=bind,src=$migration,dst=/docker-entrypoint-initdb.d/migration_${migration##*/},readonly")
done

docker run -d --name "$database" --network "$network" --network-alias postgres_db \
    "${init_mounts[@]}" \
    -e POSTGRES_USER=meteo_test -e POSTGRES_PASSWORD=fixture -e POSTGRES_DB=air_quality_db \
    -e AIRFLOW_DB_USER=airflow_test -e "AIRFLOW_DB_PASSWORD=$app_password" -e AIRFLOW_DB_NAME=airflow_test \
    -e SUPERSET_DB_USER=superset_test -e "SUPERSET_DB_PASSWORD=$app_password" -e SUPERSET_DB_NAME=superset_test \
    "$postgres_image" >/dev/null

# TCP only becomes available after the init scripts exit and the temporary socket server stops.
for ((attempt=0; attempt<60; attempt++)); do
    if docker exec "$database" pg_isready -h 127.0.0.1 -U meteo_test -d air_quality_db >/dev/null 2>&1; then
        break
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "$database")" == 'false' ]]; then
        docker logs "$database"
        exit 1
    fi
    sleep 1
done
if (( attempt == 60 )); then
    docker logs "$database"
    exit 1
fi

for service in airflow superset; do
    docker exec -e "PGPASSWORD=$app_password" "$database" psql -X -h 127.0.0.1 \
        -U "${service}_test" -d "${service}_test" -v ON_ERROR_STOP=1 \
        -c 'SELECT current_user' >/dev/null
done

docker run --rm --network "$network" \
    --mount "type=bind,src=$platform_dir,dst=/workspace,readonly" \
    --workdir /workspace --entrypoint python \
    -e POSTGRES_HOST=postgres_db -e POSTGRES_PORT=5432 \
    -e POSTGRES_USER=meteo_test -e POSTGRES_PASSWORD=fixture -e POSTGRES_DB=air_quality_db \
    -e WAREHOUSE_TEST_DISPOSABLE_DB=yes -e RUNNING_IN_DOCKER=true -e PYTHONDONTWRITEBYTECODE=1 \
    "$airflow_image" -m tests.verify_warehouse
