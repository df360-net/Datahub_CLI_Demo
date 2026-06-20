# Understanding S3 / Object Storage

The bottom layer of the lakehouse. The single most important idea:

> **Object storage is not a filesystem — it's a flat key-value store of bytes, exposed through
> the S3 API. The API is a contract in the middle, and both the clients above it and the
> storage implementations below it are swappable.**

---

## 1. What object storage is — and isn't

**Object storage manages data as discrete, self-contained units called *objects*, kept in a flat
namespace, each addressed by a unique key and accessed over an API (HTTP). Each object bundles
three things — the raw bytes + metadata + a unique identifier — and objects are operated on as a
whole (replace, not edit-in-place).**

The crispest way to pin it down is against the **three storage paradigms** — object is one of three:

| | Block storage | File storage | Object storage |
|---|---|---|---|
| Unit | fixed-size **block** | **file** in a directory tree | **object** (whole blob) |
| Namespace | block addresses (offsets) | **hierarchy** (paths/folders) | **flat** (bucket -> key) |
| Access | SCSI/NVMe, by offset | filesystem calls (open/read/seek) | **HTTP API** (PUT/GET/DELETE/LIST) by key |
| Mutability | random, in-place | random, in-place, append | **whole-object replace** (immutable-ish) |
| Metadata | none (just bytes) | limited (inode: perms, times) | **rich, extensible** (custom per object) |
| Latency | lowest (sub-ms) | low | higher (ms) |
| Scale | bounded (a volume) | bounded (a server/array) | **effectively unlimited** |
| Examples | iSCSI, FC, AWS EBS, NVMe | NFS, SMB, ext4, HDFS | **S3, MinIO, Azure Blob, GCS** |
| Built for | DB / VM disks, OS drives | shared drives, app files | backups, **data lakes, the lakehouse** |

"Object storage" means specifically the **third column** (S3 / MinIO) — *not* a filesystem (that's
file storage, like NFS/HDFS) and *not* a raw disk (that's block storage, like iSCSI/EBS).

The defining traits:
- **An "object" is the unit** — a whole blob (a Parquet file, image, backup), addressed by a key.
  You GET or PUT the *whole thing*.
- **Flat namespace** — buckets hold objects; the `/` in keys is cosmetic (no real folders).
- **API access, not a mount** — you talk to it over HTTP (the S3 API), not by mounting a drive.
- **Rich metadata per object** — beyond a filesystem's inode; arbitrary tags per object.
- **Whole-object operations** — no in-place partial writes, no append, no rename (= copy+delete).
  This immutability is *why* it pairs perfectly with Iceberg/Parquet.

Bridge to RDBMS / Hadoop experience:
- **Block** ≈ the raw SAN/disk *under* a Teradata or SQL Server (the DB's engine writes blocks).
- **File** ≈ NFS/SMB network drives, and **HDFS** (a distributed *filesystem*: real dirs, blocks, NameNode).
- **Object** ≈ S3/MinIO — what the lakehouse sits on, deliberately *not* a filesystem.

### It is not a filesystem

It looks like one (`smoke/t/data/file.parquet`), but that path is **not** a directory tree.
The namespace is **flat**: a bucket holds objects, and an object is just:

```
key (a string)  ->  value (bytes)  +  metadata
```

The `/` characters in a key are **just part of the string**. Tools like `mc` *render* them as
folders for your eyes, but there are no real directories, inodes, or block maps underneath.
"Blob storage", "object storage", and "key-value store of bytes" are all the **same thing** —
synonyms for one abstraction, not stacked layers.

## 2. The layered architecture

```
Clients / tools (swappable):   mc · boto3 SDK · web console · Spark S3FileIO · DuckDB · Trino
        ▲                       (every one just issues S3 API calls)
═══════ S3 API (HTTP REST) ═══════  PUT / GET / DELETE / LIST  on buckets & keys   ← THE boundary
        ▼
Object model:                  flat namespace: bucket -> key -> (bytes + metadata)
        │
Storage engine (e.g. MinIO):   erasure coding, healing, spreading shards across drives/nodes
        │                       (this is what provides durability)
Physical:                      actual disks / a local filesystem (XFS/ext4) on the server(s)
```

The bottom is **physical disks**. The storage engine (MinIO, or AWS's internal system) turns raw
disks into a durable object store — typically via **erasure coding**, which splits each object
into data + parity shards spread across drives so it survives disk failures.

## 3. The key insight: the S3 API is a contract, swappable on BOTH sides

An API decouples the two sides of it. As long as the contract holds:

- **Swap the client** (mc -> boto3 -> Spark) — the storage doesn't care.
- **Swap the whole storage backend** (MinIO -> AWS S3 -> Ceph) — the clients don't care.

```
Clients (swappable):       mc · boto3 · Spark S3FileIO · DuckDB · Trino · console
        ▲
════ S3 API ════  the fixed contract
        ▼
Implementations (swappable):  MinIO · AWS S3 · Ceph · Backblaze B2 · GCS(S3 mode) · Wasabi
```

This is why Spark only needed `s3.endpoint = minio:9000` to use MinIO instead of AWS — that
changed the *implementation below* the API; nothing *above* it changed. Point it at real AWS S3
tomorrow and every client keeps working untouched.

**Caveat — "S3-compatible" is a spectrum.** Every store implements the *core* (PUT/GET/DELETE/
LIST, multipart) rock-solid. Advanced features (versioning, object lock, conditional/atomic
writes, exact consistency) vary between implementations, and that occasionally matters at the
table-format level. The mental model holds; just know the edges aren't 100% uniform.

## 4. `mc` is a client, not the storage

`mc` is the **MinIO Client** — a CLI that translates commands into S3 API calls over HTTP.
It is *not* part of the storage server; it's one swappable client among many.

```
mc alias set m http://minio:9000 admin password   # 'm' = server URL + credentials
mc ls -r m/warehouse                               # ListObjects under the 'warehouse' bucket
mc cat m/warehouse/.../file.json                   # GetObject
mc stat m/warehouse/.../file.parquet               # object size / metadata
mc du  m/warehouse/smoke                            # total footprint of a table
mc cp  m/warehouse/.../f.parquet /tmp/             # download an object
```

Decoding `m/warehouse`: `m` is the **alias** (server + creds bundled under a short name),
`warehouse` is the **bucket**.

## 5. It's the grandchild of `hdfs` — same pattern, different storage

If you used Hadoop, `mc` will feel familiar: it's to object storage what `hdfs dfs` / `hadoop fs`
was to HDFS — the CLI shell for the storage layer. The verbs map ~1:1:

| Hadoop (`hdfs dfs -…`) | Lakehouse (`mc …`) |
|---|---|
| `-ls` | `mc ls` |
| `-cat` | `mc cat` |
| `-cp` / `-mv` | `mc cp` / `mc mv` |
| `-put` / `-get` | `mc cp` (local<->store) |
| `-du` | `mc du` |
| `-rm -r` | `mc rm -r` |

Evolution: **GFS -> HDFS -> object storage**. The browse-it-with-a-CLI pattern survived; the
storage model changed underneath:

| | HDFS | Object storage (S3/MinIO) |
|---|---|---|
| Model | true distributed **filesystem** | flat **key-value** object store |
| Structure | blocks, 3x replication, NameNode | objects + prefixes, erasure coding, no NameNode |
| "Folders" | real directories | just key prefixes (faked by tools) |
| Compute | **co-located** (move compute to data) | **decoupled** (compute reaches storage over network) |

**Why this matters for the lakehouse (ties to Iceberg):** because object storage is flat and
listing a prefix is slow / historically inconsistent, the old Hive approach of finding a table's
files by *listing the directory* broke on S3. That is a core reason **Iceberg exists** — it tracks
every data file **explicitly in its manifests**, so it never has to list a directory. The manifest
layer is Iceberg's answer to "object storage isn't HDFS."

## 6. Is S3 as fast as a traditional network drive?

S3 is HTTP REST, so a fair question: is it as efficient as NFS/SMB/iSCSI? **No — per operation,
S3 is slower (higher latency). But that is the wrong comparison: they are built for opposite
workloads, and for analytics S3 actually wins.**

**Where traditional network drives win** (NFS, SMB/CIFS file-level; iSCSI, Fibre Channel block-level):
- **Low per-op latency** — sub-ms on a LAN/SAN.
- **Random access & in-place updates** — seek, overwrite part of a file, partial writes.
- **POSIX semantics** — locking, rename, append.
- Small-file / interactive workloads. A single small read beats a single S3 `GET` every time.

**Where S3 wins.** It pays higher latency per request (each op is an HTTP round-trip with auth
signing — tens of ms on AWS, single-digit ms on LAN MinIO) but gives what a network drive
structurally cannot:
- **Massive parallel throughput** — thousands of concurrent `GET`s, aggregate bandwidth scales
  near-linearly (GB/s–TB/s). A single NFS server / SAN LUN has a hard ceiling.
- **Horizontal scale** — effectively unlimited capacity and request rate.
- **Range reads** — `GET` just the byte range you need (one Parquet row group / column).
- **Durability + cost** — 11-nines via erasure coding, far cheaper per GB than NAS/SAN.
- **Unlimited concurrent readers**, no lock contention.

| | Network drive (NFS/SMB/iSCSI) | S3 / object storage |
|---|---|---|
| Per-op latency | **low** (sub-ms) | higher (ms–tens of ms) |
| Access pattern | random, in-place, partial | whole-object, immutable, range-read |
| Throughput | single-server ceiling | **near-infinite via parallelism** |
| Best for | small/random/mutable, interactive | **large/sequential/immutable, massively parallel** |
| Scale & durability | bounded by the appliance | effectively unlimited, 11-nines |

It is **latency vs throughput**, **random vs sequential**, **single-server vs massively-parallel.**

**Why this is perfect for the lakehouse (not a compromise).** The workload is big columnar files,
read sequentially, by many parallel tasks — which plays to S3's strengths and hides its weakness:
- Per-request latency is negligible when each request streams a large chunk; throughput dominates.
- Spark fans out 1000 tasks reading different objects -> S3's parallel scale shines; a network
  drive would bottleneck on its single server.
- Object **immutability is aligned, not a limitation** — Iceberg/Parquet never update in place; they
  write new files, matching the storage's "no in-place writes."

This also explains two things from elsewhere in the guide:
- **The small-files problem** — you pay per-request latency *per file*, so a million tiny objects is
  death by a thousand round-trips. Fewer, larger files amortize the overhead.
- **Why compaction matters** — it merges small objects into big ones S3 streams efficiently.

**Bridge to MPP/Teradata:** aggregate throughput comes from **fan-out, not single-stream speed** —
the same principle as a shared-nothing system where hundreds of AMPs read in parallel. Don't
optimize the single request; multiply the requests.

So: as a network-drive replacement for interactive/random/small-file work, S3 is worse. As the
storage layer for parallel analytics, it is *better* — the workload scales out exactly the way
object storage was built to serve.

## 7. The access model: location-independent, universal, delegatable

The single most consequential property of object storage:

> **An object isn't tied to a machine or a location — it's a key in a bucket, reachable over HTTP
> by any authorized client, from anywhere.**

This sounds like a network drive, but it is fundamentally different — and the difference is *not*
"reachable from another machine" (network drives are network-accessible too; you can mount an SMB
share from many PCs). The real differences are three:

1. **Universal HTTP reach, no mount.** A network drive must be *mounted* by the OS — a specific
   protocol (SMB/NFS), a driver, usually inside a LAN/VPN. An S3 object is reachable by **any
   HTTP-capable client, any language, any platform, from anywhere** — a browser, `curl`, a phone,
   a Python script, a Spark cluster — all equal clients, no mount, no driver.
2. **Addressed by a global URL/key, not a host path.** A network file is `//serverX/share/file` —
   bound to that server, that export, that network. An object's identity is a **location-independent
   key in a global namespace** served by a distributed system. It's not "a file on a machine"; it's
   "a key in a service."
3. **Delegatable, time-boxed, credential-free access — the presigned URL.** You can grant access to
   **one object, for N hours, to someone with no account**, via a signed URL. **No network drive can
   do this.** A presigned URL is a normal HTTP `GET` with a cryptographic **signature + expiry** baked
   into it — the signature *is* the authorization, the recipient needs no credentials, and access dies
   automatically at expiry. (This is the governed, modern form of "upload a big file at home, download
   it at work" — scoped to one object, revocable by expiry.)

**Why it matters most:** this is the property that made S3 the **connective tissue of the cloud** —
because any service can reach the same bytes over plain HTTP, S3 became the universal integration
point every system plugs into. (Durability, scale, and cost are other pillars, but *this* is what
made it the backbone.)

**The lakehouse payoff:** this exact property is *why* the lakehouse works. Spark, Trino, and DuckDB —
on different machines, from different vendors — all reach the same Iceberg objects over HTTP.
"Engine-agnostic, decoupled storage and compute" is nothing more than "objects are reachable by any
HTTP client from anywhere," scaled up. The casual "S3 as a file-transfer tool" trick and the entire
decoupled lakehouse architecture rest on the **identical** property.

## 8. The bigger pattern: stable contract + swappable implementations

S3 is one instance of the design principle that runs through the entire lakehouse — *a stable
contract in the middle, with swappable pieces on both sides*:

| Contract | Swappable below | Swappable above |
|---|---|---|
| **S3 API** | MinIO / AWS / Ceph | mc / boto3 / Spark |
| **Iceberg table format** | any catalog + object store | Spark / Trino / DuckDB / Flink |
| **SQL** | any engine | any query / BI tool |

This is the **opposite of a tightly-coupled appliance** (e.g. Teradata), where storage, API, and
engine are welded into one box. The lakehouse deliberately *unwelds* them into stacked contracts —
and that decoupling is exactly what buys the swappability.

### RDBMS / Teradata bridge

| Object storage | RDBMS analog |
|---|---|
| S3 API (the contract) | the DB's wire protocol / SQL interface |
| `mc` / boto3 / console | different clients (`psql`, a GUI, a JDBC app) |
| MinIO erasure-coding engine | the database **storage engine** (how bytes hit disk durably) |
| disks | disks |

## 9. Hands-on: exploring the storage from a shell

The containers run in Docker on a host (Murphy). You reach them by exec'ing in:

```
ssh jianm@192.168.0.16             # into the host (Windows shell)
docker exec -it mc sh              # into the mc container (has the mc client)
mc alias set m http://minio:9000 admin password
mc ls -r m/warehouse              # the entire physical footprint of every Iceberg table
```

Also available:
- **MinIO web console:** http://192.168.0.16:9001 (admin / password) — the same objects, graphical.
- **Raw disk view:** `docker exec -it minio sh` then look under `/data` — but MinIO scatters internal
  erasure-coding files there; `mc` shows the clean logical view.
