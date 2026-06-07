from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

from cardcompass import catalog, lineage


def test_field_type_mapping():
    assert isinstance(catalog._field_type("nvarchar(40)").type, StringTypeClass)
    assert isinstance(catalog._field_type("char(3)").type, StringTypeClass)
    assert isinstance(catalog._field_type("decimal(18,2)").type, NumberTypeClass)
    assert isinstance(catalog._field_type("bigint").type, NumberTypeClass)
    assert isinstance(catalog._field_type("int").type, NumberTypeClass)
    assert isinstance(catalog._field_type("bit").type, BooleanTypeClass)
    assert isinstance(catalog._field_type("datetime2(3)").type, TimeTypeClass)
    assert isinstance(catalog._field_type("date").type, DateTypeClass)


def test_catalog_publishes_six_datasets_with_columns():
    assert len(catalog.DATASETS) == 6
    for schema, table, desc, cols in catalog.DATASETS:
        assert desc and cols


def _published_columns():
    return {(s, t): {c for c, _ in cols} for s, t, _, cols in catalog.DATASETS}


def test_lineage_downstreams_are_published_datasets():
    pub = set(_published_columns())
    for key in lineage.TABLE_UPSTREAMS:
        assert key in pub, key


def test_lineage_column_edges_reference_real_columns():
    cols = _published_columns()
    for (ds, dt), edges in lineage.COLUMN_EDGES.items():
        for down_col, ups in edges:
            assert down_col in cols[(ds, dt)], f"downstream {ds}.{dt}.{down_col}"
            for us, ut, uc in ups:
                # CARD_REF tables aren't published catalog datasets; skip those.
                if (us, ut) in cols:
                    assert uc in cols[(us, ut)], f"upstream {us}.{ut}.{uc}"


def test_stage_tables_have_no_column_lineage_only_file_upstream():
    # card_auth / card_post originate from file feeds -> no fineGrainedLineages.
    assert ("CARD_STG", "card_auth") not in lineage.COLUMN_EDGES
    assert ("CARD_STG", "card_post") not in lineage.COLUMN_EDGES
    for t in ("card_auth", "card_post"):
        ups = lineage.TABLE_UPSTREAMS[("CARD_STG", t)]
        assert len(ups) == 1 and "dataPlatform:file" in ups[0]
