from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from sqlalchemy.engine import Engine

from . import catalog, integration, lineage, report, stage
from .assertions import specs
from .assertions.publisher import publish_results
from .assertions.specs import run_checks
from .config import AppConfig
from .db import make_engine
from .emit import make_emitter

# Application lock — serialises daily_load runs (one connection holds it for the
# session). Advisory: it blocks a second daily_load, not other DB work.
_LOCK_SQL = (
    "DECLARE @rc int; "
    "EXEC @rc = sp_getapplock @Resource = N'cardcompass_daily_load', "
    "@LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0; "
    "SELECT @rc;"
)
_UNLOCK_SQL = "EXEC sp_releaseapplock @Resource = N'cardcompass_daily_load', @LockOwner = N'Session';"


def _publish_layer(cfg: AppConfig, engine: Engine, name: str, layer_specs, biz, logger) -> None:
    with engine.connect() as conn:
        results = run_checks(conn, layer_specs, biz)
    dq_failing = [r for r in results if r.status != "PASSED"]
    summary = publish_results(cfg.proxy_url, results, cfg.source_app_name, cfg.enterprise_pid)
    # A publish failure (bad status / transport) is fatal; a FAILED assertion
    # result is a normal data condition and is published with FAILURE status.
    if summary.failed:
        raise RuntimeError(f"{name} assertion publish failed: {summary.errors}")
    logger.info("%s assertions: %d published (%d DQ-failing)", name, summary.posted, len(dq_failing))


def run(cfg: AppConfig, biz: date, logger: logging.Logger) -> None:
    engine = make_engine(cfg)
    lock_conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    exit_code = 0
    try:
        rc = lock_conn.exec_driver_sql(_LOCK_SQL).scalar()
        if rc is None or rc < 0:
            raise RuntimeError(f"could not acquire load lock (rc={rc}) - another run in progress?")

        emitter = make_emitter(cfg)
        logger.info("daily_load start: date=%s pid=%s", biz, cfg.enterprise_pid)

        sc = stage.load_stage(engine, biz, cfg.inbound_dir)
        logger.info("stage loaded: %s", sc)
        _publish_layer(cfg, engine, "stage", specs.stage_specs(), biz, logger)

        ic = integration.load_integration(engine, biz)
        logger.info("integration loaded: %s", ic)
        _publish_layer(cfg, engine, "integration", specs.integration_specs(), biz, logger)

        rp = report.load_report(engine, biz)
        logger.info("report loaded: %s", rp)
        _publish_layer(cfg, engine, "report", specs.report_specs(), biz, logger)

        n_ds = catalog.publish_catalog(emitter, cfg.source_app_name)
        n_lin = lineage.publish_lineage(emitter)
        logger.info("catalog: %d datasets | lineage: %d edges", n_ds, n_lin)

        logger.info(
            "daily_load OK date=%s stage=%s integration=%s report=%s catalog=%d lineage=%d",
            biz, sc, ic, rp, n_ds, n_lin,
        )
    except Exception as exc:  # noqa: BLE001 — log, release lock, exit non-zero
        logger.error("daily_load FAILED date=%s: %s", biz, exc)
        exit_code = 1
    finally:
        try:
            lock_conn.exec_driver_sql(_UNLOCK_SQL)
        except Exception:  # noqa: BLE001 — lock may never have been held
            pass
        lock_conn.close()
        engine.dispose()
    if exit_code:
        sys.exit(exit_code)


def main() -> None:
    ap = argparse.ArgumentParser(description="CardCompass daily ETL + DataHub publish")
    ap.add_argument("--date", default=None, help="business date YYYY-MM-DD (default: yesterday)")
    args = ap.parse_args()

    cfg = AppConfig.from_env()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("cardcompass")

    biz = date.fromisoformat(args.date) if args.date else (date.today() - timedelta(days=1))
    run(cfg, biz, logger)


if __name__ == "__main__":
    main()
