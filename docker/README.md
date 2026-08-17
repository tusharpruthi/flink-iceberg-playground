# Local playground stack

Brings up Postgres (with `wal2json` logical decoding), MinIO, an Iceberg REST catalog server, and
a small Flink cluster. Isolated via its own Compose project name (`flink-iceberg-playground`), network
(`flink-iceberg-playground-net`), and non-default host ports so it won't collide with other local Docker
projects.

| Service        | Host port(s) |
|----------------|--------------|
| Postgres       | 55432        |
| MinIO API      | 59000        |
| MinIO console  | 59001        |
| Iceberg REST   | 58181        |
| Flink Web UI   | 58082        |

Apache Polaris was tried first for the REST catalog and dropped: its server-side S3 client
resolves MinIO paths in virtual-hosted style (`bucket.host`) regardless of path-style config,
which MinIO rejects — an interop gap in Polaris's MinIO support, not something fixable from this
app's side. `tabulario/iceberg-rest` (Iceberg's own reference REST catalog server) works with
MinIO out of the box and is what most Iceberg+MinIO tutorials use locally. The app only sees
`catalog.type: rest`, so swapping the catalog server back to Polaris later — or pointing at AWS
Glue in prod — is a config change, not a code change.

## Bring the stack up

```bash
cd docker
docker compose up -d
docker compose ps        # wait until postgres/minio/iceberg-rest report healthy
```

`minio-init` creates the warehouse bucket and exits; check its logs if the job later fails to
reach storage:

```bash
docker compose logs minio-init
```

## Run the job once against this stack

This is a real Flink job, so it needs to go through `flink run` against the Dockerized cluster —
running it as a bare Java process depends on JVM `--add-opens` flags for Flink's Kryo serializer
on Java 17+, and a `mvn exec:java`-launched JVM has its own classloader isolation issues with
Flink's MiniCluster. Submitting to the real cluster avoids both:

```bash
mvn package -DskipTests
docker cp target/flink-iceberg-playground-1.0-SNAPSHOT.jar flink-iceberg-playground-jobmanager:/opt/flink/job.jar

# hostnames below are the in-network service names, not the host-mapped ports from the table above
docker exec \
  -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e ICEBERG_REST_URI=http://iceberg-rest:8181 \
  flink-iceberg-playground-jobmanager \
  flink run -c com.hevo.icebergplayground.job.IcebergWalSyncJob /opt/flink/job.jar local
```

A run with no captured changes exits quickly without submitting any Flink job at all (there's
nothing to write) — that's expected, not a failure.

## Data generator

`pgbench-loadgen` bootstraps the standard `pgbench` schema (`pgbench_accounts`,
`pgbench_branches`, `pgbench_tellers`, `pgbench_history`) once via `pgbench -i -s 1`, then runs a
steady trickle of 5 pgbench transactions every 60s (`docker/loadgen/entrypoint.sh`) — a few
`UPDATE`s plus one `INSERT`, so there's always fresh WAL activity for the job to sync without
needing a bulk/burst load. The one-time bootstrap is skipped on subsequent restarts as long as the
`postgres-data` volume persists.

`pgbench_history` has no primary key, so it's excluded from `source.tables` in
`application-local.yml` — `TableSchemaResolver` requires one on every synced table so CDC
updates/deletes can be applied as Iceberg upserts/equality-deletes.

```bash
docker compose logs -f pgbench-loadgen   # watch the trickle
```

## Try a full cycle

```bash
docker exec -it flink-iceberg-playground-postgres psql -U playground -d playground \
  -c "UPDATE pgbench_accounts SET abalance = abalance + 100 WHERE aid = 1;"

# run the job (see above), then:
docker exec -it flink-iceberg-playground-postgres psql -U playground -d playground \
  -c "UPDATE pgbench_accounts SET abalance = abalance + 50 WHERE aid = 1;"

# run the job again — only the second update should be picked up this time, and applied as an
# upsert (aid=1's balance reflects the latest value, no duplicate row).
```

Rows that existed in Postgres *before* the replication slot was created are never captured —
there's no initial-snapshot/backfill step in this job, only ongoing WAL changes from slot creation
onward. That's expected for a from-scratch table; there's nothing to reconcile against.

## Tear down

```bash
docker compose down -v
```
