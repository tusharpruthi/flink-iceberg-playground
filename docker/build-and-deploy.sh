#!/bin/bash
# Builds the job JAR and copies it into the running Flink jobmanager container as job.jar.
# Resolves the JAR name dynamically (excluding the shaded plugin's original-*.jar) so it
# survives artifactId renames instead of silently deploying a stale build.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBMANAGER_CONTAINER="flink-iceberg-playground-jobmanager"

cd "${PROJECT_ROOT}"

echo "[build-and-deploy] mvn package..."
mvn package -DskipTests -q

JAR_PATH="$(find target -maxdepth 1 -name '*.jar' ! -name 'original-*.jar' -print -quit)"
if [ -z "${JAR_PATH}" ]; then
  echo "[build-and-deploy] no jar found in target/ after build" >&2
  exit 1
fi

echo "[build-and-deploy] deploying ${JAR_PATH} -> ${JOBMANAGER_CONTAINER}:/opt/flink/job.jar"
docker cp "${JAR_PATH}" "${JOBMANAGER_CONTAINER}:/opt/flink/job.jar"

echo "[build-and-deploy] done. Run it with:"
echo "  docker exec -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 -e MINIO_ENDPOINT=http://minio:9000 -e ICEBERG_REST_URI=http://iceberg-rest:8181 ${JOBMANAGER_CONTAINER} flink run -c com.hevo.icebergplayground.job.IcebergWalSyncJob /opt/flink/job.jar local"
