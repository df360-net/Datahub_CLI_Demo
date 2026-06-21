"""Build and push the Sales360 catalog + lineage to DataHub via direct token auth.

Structure:
- ONE Application entity "SAL360" (top-level, clickable) that owns every asset across all 4
  platforms (mysql/spark/dremio/airflow), via the `applications` aspect.
- A per-platform schema Container (sales_oltp, sales_bronze/silver/gold, sales_curated) that the
  tables nest under.
- Datasets carry NO platformInstance (the Application is the cross-platform home instead).

Per dataset: schemaMetadata + datasetProperties + container + applications. Per edge:
upstreamLineage (table + column level). Plus the sales_daily DataFlow + 9 DataJobs (also owned by
the Application). Idempotent: re-running upserts every aspect.

Run:  python -m sales360
"""
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, add_dataset_to_container, gen_containers
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ApplicationKeyClass,
    ApplicationPropertiesClass,
    ApplicationsClass,
    BooleanTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from sales360.config import Settings
from sales360.model import AIRFLOW, EXPLICIT_COLMAP, LAYER_PLATFORM, LINEAGE, TABLES, datasets
from sales360.urn import app_urn, dataset_urn, field_urn, flow_urn, job_urn, platform_urn

LAYER_DESC = {
    "mysql":  "Raw operational table (MySQL OLTP source of record)",
    "bronze": "Raw typed append-log (Spark -> Iceberg, sales_bronze)",
    "silver": "Deduped-to-current, conformed (Spark -> Iceberg, sales_silver)",
    "gold":   "SCD-2 star schema (Spark -> Iceberg, sales_gold)",
    "dremio": "Curated serving view (Dremio over the gold star)",
}


def field_type(col: str):
    c = col.lower()
    if c.startswith("is_") or c.endswith("_flag"):
        return BooleanTypeClass(), "boolean"
    if c in ("created_at", "updated_at", "order_ts") or c.endswith("_ts") or c.endswith("_at"):
        return TimeTypeClass(), "timestamp"
    if c == "date" or c.endswith("_date") or c in ("valid_from", "valid_to"):
        return DateTypeClass(), "date"
    if (c.endswith(("_id", "_key", "_no")) or c in ("quantity", "year", "quarter", "month", "day",
            "day_of_week", "week_of_year", "date_key", "category_id")
            or any(k in c for k in ("price", "amount", "total", "pct"))):
        return NumberTypeClass(), "number"
    return StringTypeClass(), "string"


def schema_aspect(platform: str, full_name: str, cols):
    fields = []
    for c in cols:
        t, native = field_type(c)
        fields.append(SchemaFieldClass(fieldPath=c, type=SchemaFieldDataTypeClass(type=t),
                                       nativeDataType=native))
    return SchemaMetadataClass(schemaName=full_name, platform=platform_urn(platform), version=0,
                               hash="", platformSchema=OtherSchemaClass(rawSchema=""), fields=fields)


def main():
    s = Settings.load()
    emitter = DatahubRestEmitter(gms_server=s.gms_server, token=s.token)
    emitter.test_connection()
    APP = app_urn(s.app_id)
    print(f"connected to DataHub @ {s.gms_server}  (Application={s.app_id})")

    def dsurn(layer, table):
        platform, ns = LAYER_PLATFORM[layer]
        return dataset_urn(platform, f"{ns}.{table}", s.env)

    def ckey(layer):
        platform, ns = LAYER_PLATFORM[layer]
        return DatabaseKey(platform=platform, instance=None, env=s.env, database=ns)

    def emit(urn, aspect):
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))

    n = 0
    # 0. the SAL360 Application (single, clickable, cross-platform home)
    emit(APP, ApplicationKeyClass(id=s.app_id))
    emit(APP, ApplicationPropertiesClass(
        name=s.app_id,
        description="Sales360 - end-to-end Sales data platform: OLTP -> medallion -> serving/BI"))
    n += 2

    # 1. one schema Container per layer namespace, owned by the Application
    for layer in LAYER_PLATFORM:
        platform, ns = LAYER_PLATFORM[layer]
        for wu in gen_containers(ckey(layer), name=ns, sub_types=["Schema"],
                                 description=f"Sales360 {layer} schema ({platform})"):
            emitter.emit(wu.metadata)
            n += 1
        emit_container_urn = ckey(layer).as_urn()
        emit(emit_container_urn, ApplicationsClass(applications=[APP]))
        n += 1

    # 2. datasets: schema + properties + container + application
    for layer, table, cols in datasets():
        platform, ns = LAYER_PLATFORM[layer]
        urn, full = dsurn(layer, table), f"{ns}.{table}"
        emit(urn, schema_aspect(platform, full, cols))
        emit(urn, DatasetPropertiesClass(
            name=table, qualifiedName=full, description=LAYER_DESC[layer],
            customProperties={"source.app": "Sales360", "layer": layer, "namespace": ns}))
        for wu in add_dataset_to_container(ckey(layer), urn):
            emitter.emit(wu.metadata)
            n += 1
        emit(urn, ApplicationsClass(applications=[APP]))
        n += 3
    print(f"  {sum(len(t) for t in TABLES.values())} datasets (schema + properties + container + app)")

    # 3. table-level + column-level (fineGrained) lineage
    def colmap_for(down, ups):
        if down in EXPLICIT_COLMAP:
            return EXPLICIT_COLMAP[down]
        dl, dt = down
        out = {}
        for col in TABLES[dl][dt]:
            srcs = [(ul, ut, col) for (ul, ut) in ups if col in TABLES[ul][ut]]
            if srcs:
                out[col] = srcs
        return out

    def fine_grained(down, ups):
        dn_urn = dsurn(*down)
        fgls = []
        for dcol, srcs in colmap_for(down, ups).items():
            fgls.append(FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[field_urn(dsurn(ul, ut), uc) for ul, ut, uc in srcs],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[field_urn(dn_urn, dcol)]))
        return fgls

    edges = col_maps = 0
    for (dl, dt), ups in LINEAGE:
        emit(dsurn(dl, dt), UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=dsurn(*u), type=DatasetLineageTypeClass.TRANSFORMED) for u in ups],
            fineGrainedLineages=fine_grained((dl, dt), ups)))
        edges += len(ups)
        col_maps += len(colmap_for((dl, dt), ups))
        n += 1
    print(f"  {len(LINEAGE)} lineage aspects ({edges} table edges, {col_maps} column mappings)")

    # 4. Airflow process lineage, also owned by the Application
    emit(flow_urn(), DataFlowInfoClass(
        name="sales_daily",
        description="End-to-end Sales platform: OLTP day -> feed -> medallion -> DQ -> serving/BI",
        customProperties={"source.app": "Sales360"}))
    emit(flow_urn(), ApplicationsClass(applications=[APP]))
    n += 2
    for job_id, desc, ins, outs in AIRFLOW:
        ju = job_urn(job_id)
        emit(ju, DataJobInfoClass(name=job_id, type="COMMAND", description=desc))
        emit(ju, DataJobInputOutputClass(
            inputDatasets=[dsurn(*x) for x in ins], outputDatasets=[dsurn(*x) for x in outs]))
        emit(ju, ApplicationsClass(applications=[APP]))
        n += 3
    print(f"  1 DataFlow + {len(AIRFLOW)} DataJobs (process lineage)")
    print(f"done: {n} aspects emitted; everything owned by Application '{s.app_id}'")


if __name__ == "__main__":
    main()
