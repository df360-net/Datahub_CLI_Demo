"""Build and push the Sales360 catalog + lineage to DataHub via direct token auth.

Structure mirrors the workplace pattern: a single clickable platformInstance (ENTERPRISE_APP_ID,
e.g. sales360_demo) as the top container ->
  Database  sales_oltp        (MySQL OLTP tables)
  Database  sales_lakehouse    -> Schema sales_bronze / sales_silver / sales_gold (Spark tables)
  Database  sales_curated      (Dremio views)
Everything is on one logical platform `sales360`; the instance rides in every dataset URN.

Per dataset: schemaMetadata + datasetProperties + dataPlatformInstance + container. Per edge:
upstreamLineage (table + column level). Airflow process lineage = DataFlow/DataJob with the
instance in their `cluster`, wired to the datasets. Idempotent.

Run:  python -m sales360
"""
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey, add_dataset_to_container, gen_containers
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DataPlatformInstancePropertiesClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    PlatformTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from sales360.config import Settings
from sales360.model import (
    AIRFLOW,
    DATABASES,
    EXPLICIT_COLMAP,
    LAKEHOUSE_SCHEMAS,
    LAYER_CONTAINER,
    LINEAGE,
    TABLES,
    dataset_name,
    datasets,
)
from sales360.urn import dataset_urn, field_urn, flow_urn, instance_urn, job_urn, platform_urn

LAYER_DESC = {
    "mysql":  "Raw operational table (MySQL OLTP source of record)",
    "bronze": "Raw typed append-log (Spark -> Iceberg)",
    "silver": "Deduped-to-current, conformed (Spark -> Iceberg)",
    "gold":   "SCD-2 star schema (Spark -> Iceberg)",
    "dremio": "Curated serving view (Dremio)",
}
DB_DESC = {
    "sales_oltp": "Sales OLTP source (MySQL)",
    "sales_lakehouse": "Sales lakehouse (Spark + Iceberg): bronze/silver/gold",
    "sales_curated": "Sales curated serving layer (Dremio)",
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
    P, INST, ENV = s.platform, s.instance, s.env
    print(f"connected to DataHub @ {s.gms_server}  (platform={P}, instance={INST})")

    def dsurn(layer, table):
        return dataset_urn(P, dataset_name(layer, table), INST, ENV)

    def db_key(db):
        return DatabaseKey(platform=P, instance=INST, env=ENV, database=db)

    def sch_key(db, schema):
        return SchemaKey(platform=P, instance=INST, env=ENV, database=db, schema=schema)

    def leaf_key(layer):
        db, schema = LAYER_CONTAINER[layer]
        return sch_key(db, schema) if schema else db_key(db)

    def emit(urn, aspect):
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))

    n = 0
    # 0. register the logical platform + the named, clickable platformInstance (top container)
    emit(platform_urn(P), DataPlatformInfoClass(
        name=P, type=PlatformTypeClass.OTHERS, datasetNameDelimiter=".", displayName="Sales360"))
    emit(instance_urn(P, INST), DataPlatformInstancePropertiesClass(
        name=INST, description="Sales360 enterprise application instance"))
    n += 2

    # 1. databases
    for db in DATABASES:
        for wu in gen_containers(db_key(db), name=db, sub_types=["Database"], description=DB_DESC[db]):
            emitter.emit(wu.metadata)
            n += 1
    # 2. schemas under sales_lakehouse
    for schema in LAKEHOUSE_SCHEMAS:
        for wu in gen_containers(sch_key("sales_lakehouse", schema), name=schema, sub_types=["Schema"],
                                 parent_container_key=db_key("sales_lakehouse"),
                                 description=f"Lakehouse {schema.split('_')[-1]} layer"):
            emitter.emit(wu.metadata)
            n += 1
    print(f"  {len(DATABASES)} databases + {len(LAKEHOUSE_SCHEMAS)} schemas")

    # 3. datasets: schema + properties + platformInstance + container
    inst_aspect = DataPlatformInstanceClass(platform=platform_urn(P), instance=instance_urn(P, INST))
    for layer, table, cols in datasets():
        urn, full = dsurn(layer, table), dataset_name(layer, table)
        emit(urn, schema_aspect(P, full, cols))
        emit(urn, DatasetPropertiesClass(
            name=table, qualifiedName=full, description=LAYER_DESC[layer],
            customProperties={"source.app": "Sales360", "layer": layer}))
        emit(urn, inst_aspect)
        for wu in add_dataset_to_container(leaf_key(layer), urn):
            emitter.emit(wu.metadata)
            n += 1
        n += 3
    print(f"  {sum(len(t) for t in TABLES.values())} datasets")

    # 4. table-level + column-level (fineGrained) lineage
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

    # 5. Airflow process lineage (instance in the dataFlow/dataJob cluster)
    emit(flow_urn(INST), DataFlowInfoClass(
        name="sales_daily",
        description="End-to-end Sales platform: OLTP day -> feed -> medallion -> DQ -> serving/BI",
        customProperties={"source.app": "Sales360"}))
    n += 1
    for job_id, desc, ins, outs in AIRFLOW:
        ju = job_urn(job_id, INST)
        emit(ju, DataJobInfoClass(name=job_id, type="COMMAND", description=desc))
        emit(ju, DataJobInputOutputClass(
            inputDatasets=[dsurn(*x) for x in ins], outputDatasets=[dsurn(*x) for x in outs]))
        n += 2
    print(f"  1 DataFlow + {len(AIRFLOW)} DataJobs (cluster={INST})")
    print(f"done: {n} aspects emitted under instance '{INST}'")


if __name__ == "__main__":
    main()
