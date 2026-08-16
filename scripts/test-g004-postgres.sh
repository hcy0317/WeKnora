#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
weknora_path="${1:-${WEKNORA_PATH:-${script_dir}/..}}"
if ! resolved_weknora="$(realpath "${weknora_path}" 2>/dev/null)"; then
  echo "WeKnora source path does not exist: ${weknora_path}; when running from the overlay, pass the source path as the first argument or set WEKNORA_PATH" >&2
  exit 2
fi
if [ ! -f "${resolved_weknora}/go.mod" ]; then
  echo "WeKnora source path is missing go.mod: ${resolved_weknora}; when running from the overlay, pass the source path as the first argument or set WEKNORA_PATH" >&2
  exit 2
fi

suffix="$$"
network="weknora-g004-test-${suffix}"
postgres_container="weknora-g004-pg-${suffix}"

cleanup() {
  docker rm -f -v "${postgres_container}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "${network}" >/dev/null
docker run -d --name "${postgres_container}" --network "${network}" \
  -e POSTGRES_USER=g004 -e POSTGRES_PASSWORD=g004test -e POSTGRES_DB=g004 \
  paradedb/paradedb:v0.22.2-pg17 >/dev/null

consecutive_ready=0
for _ in $(seq 1 60); do
  if docker exec -e PGPASSWORD=g004test "${postgres_container}" \
    psql -h 127.0.0.1 -U g004 -d g004 -Atqc 'SELECT 1' >/dev/null 2>&1; then
    consecutive_ready=$((consecutive_ready + 1))
  else
    consecutive_ready=0
  fi
  if [ "${consecutive_ready}" -ge 3 ]; then
    break
  fi
  sleep 1
done
docker exec -e PGPASSWORD=g004test "${postgres_container}" \
  psql -h 127.0.0.1 -U g004 -d g004 -Atqc 'SELECT 1' >/dev/null

docker run --rm --network "${network}" \
  -e WEKNORA_TEST_POSTGRES_EPHEMERAL=1 \
  -e "WEKNORA_TEST_POSTGRES_DSN=postgres://g004:g004test@${postgres_container}:5432/g004?sslmode=disable" \
  -v "${resolved_weknora}:/workspace" -v weknora-go-mod-cache:/go/pkg/mod \
  -v weknora-go-build-cache:/root/.cache/go-build -w /workspace \
  golang:1.26-bookworm go test \
  ./internal/application/repository ./internal/database \
  -run 'Test(KnowledgeSpanRepo_Postgres|PostgresMigration|PostgresFresh)' -count=1 -v

docker run --rm --network "${network}" \
  -e WEKNORA_MIGRATE_ONLY=true -e AUTO_RECOVER_DIRTY=false \
  -e DB_DRIVER=postgres -e DB_USER=g004 -e DB_PASSWORD=g004test -e DB_NAME=g004 \
  -e DB_PORT=5432 -e "DB_HOST=${postgres_container}" -e RETRIEVE_DRIVER=postgres \
  -v "${resolved_weknora}:/workspace" -v weknora-go-mod-cache:/go/pkg/mod \
  -v weknora-go-build-cache:/root/.cache/go-build -w /workspace \
  golang:1.26-bookworm go run ./cmd/server
