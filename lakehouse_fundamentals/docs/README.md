# Lakehouse Sandbox — Murphy

A hands-on Iceberg + Spark + MinIO lakehouse running in Docker on Murphy
(`192.168.0.16`). This is the live counterpart to the four-layer model in
[Lakehouse.md](Lakehouse.md) — each conceptual layer is a real container you can poke at.

Compose file: [`../docker/docker-compose.yml`](../docker/docker-compose.yml)
(also deployed on Murphy at `C:\lakehouse\docker-compose.yml`).

## What's running

| Layer | Container | Access (from your LAN) |
|---|---|---|
| Engine (Spark + Jupyter) | `spark-iceberg` | Notebooks: http://192.168.0.16:8888 · Spark UI: http://192.168.0.16:8082 |
| Table format (Iceberg catalog) | `iceberg-rest` | http://192.168.0.16:8181 |
| Object store (MinIO) | `minio` | Console: http://192.168.0.16:9001 — `admin` / `password` |
| Bucket bootstrap | `mc` | created the `warehouse` bucket, now idle |

All four return HTTP 200, and the real proof: Spark created `demo.smoke.t`, wrote Parquet
to MinIO via the REST catalog, and read the two rows back. The four-layer `Lakehouse.md`
diagram is now a running thing you can poke at.

## How the layers map (live)

```
spark-iceberg   Engine        Spark SQL / Jupyter  -> reads & writes
   ↑
iceberg-rest    Table format  Iceberg REST catalog -> "where is table X, what's its schema"
   ↑
(Parquet)       File format   written by Spark into MinIO (no container)
   ↑
minio           Object store  S3-compatible, bucket s3://warehouse/
```

The Spark catalog is named **`demo`** and is backed by `iceberg-rest`, whose warehouse is
`s3://warehouse/` on MinIO. So a table `demo.smoke.t` is Iceberg metadata in the REST
catalog plus Parquet files under `warehouse/` in MinIO.

## First thing to try (Jupyter)

Open http://192.168.0.16:8888, start a notebook, and run:

```python
spark.sql("SELECT * FROM demo.smoke.t").show()
```

That's the smoke-test table, read straight off MinIO. Then peek at MinIO
(http://192.168.0.16:9001) and you'll see the Parquet + metadata files under
`warehouse/smoke/t/`.

Time-travel (Iceberg's killer feature) — every write is a snapshot:

```python
spark.sql("SELECT * FROM demo.smoke.t.snapshots").show(truncate=False)   # the snapshot log
spark.sql("SELECT * FROM demo.smoke.t.history").show(truncate=False)     # commits over time
```

## Daily operations

These run fine over plain SSH (`ssh jianm@192.168.0.16`) — no registry credentials needed:

```cmd
docker ps                                              :: what's up
docker compose -f C:\lakehouse\docker-compose.yml stop :: pause the stack (keeps data)
docker compose -f C:\lakehouse\docker-compose.yml start:: resume
docker compose -f C:\lakehouse\docker-compose.yml down :: remove containers (volumes kept)
docker exec -it spark-iceberg spark-sql                :: a SQL shell in the engine
```

Run a SQL file end-to-end (the smoke test lives at `C:\lakehouse\smoke.sql`):

```cmd
docker cp C:\lakehouse\smoke.sql spark-iceberg:/tmp/smoke.sql
docker exec spark-iceberg spark-sql -f /tmp/smoke.sql
```

## Gotcha: pulling images on Murphy (one-time, when first deploying or upgrading)

`docker pull` / `docker compose up` that needs to **download images** fails over plain SSH:

```
error getting credentials ... "A specified logon session does not exist."
```

This is Docker Desktop on Windows: the credential helper needs the interactive user's
Credential vault, but SSH runs in session 0 (network logon). **No `config.json` edit fixes
it.** Pulls must run in the **interactive console session** (where you're logged in).
The working pattern is a one-shot Scheduled Task:

```cmd
:: C:\lakehouse\up.bat already contains: docker compose up -d > C:\lakehouse\up.log 2>&1
schtasks /create /tn lakehouse_up /tr "C:\lakehouse\up.bat" /sc ONCE /st 23:59 /it /ru jianm /f
schtasks /run /tn lakehouse_up
:: then watch C:\lakehouse\up.log ; Docker resumes partial layers if a pull stalls
```

Once images are pulled, everything else (`start`/`stop`/`exec`/`up` on cached images)
works over normal SSH.

## Resource notes (shared host)

- Murphy has 16 GB RAM. With this stack up, **free RAM is ~2.3 GB** — Spark's JVM is the
  hungry one. Fine for learning; heavy Spark jobs are the ceiling.
- **DataHub and the df360 sidecars are stopped** to make room. They're paused, not deleted
  — `docker start` the `datahub-*` / `df360-*` containers to bring them back. Run one stack
  at a time.

## Notebook persistence (important)

Jupyter's home root (`/home/iceberg/notebooks/`, where the example `Iceberg - *.ipynb`
live) is **inside the container** — notebooks saved there survive `stop`/`start` but are
**lost on `docker compose down` or a container recreate/upgrade**.

Only the **`notebooks/` subfolder** is bind-mounted to Murphy's disk
(`C:\lakehouse\notebooks`). **Save your work there.** For version control, copy finished
notebooks back into the repo at `../notebooks/`:

```
scp jianm@192.168.0.16:C:/lakehouse/notebooks/<name>.ipynb ../notebooks/<name>.ipynb
```

## The stack vs. DataHub

This sandbox is a **separate learning track** from the DataHub/CardCompass work in the
parent repo. Spark UI is mapped to host port **8082** (not 8080) specifically so it never
collides with DataHub GMS (8080) if both are ever up. Normally, run one or the other.
