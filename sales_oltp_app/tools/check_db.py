"""Connectivity check for the Sales OLTP database. Verifies the dev path
(Lenovo -> MySQL@Zeenie over the LAN) and prints who we are + table row counts."""
import sys

from sqlalchemy import text

from sales_db import get_engine

TABLES = ["product_categories", "customers", "products", "stores", "orders", "order_items"]


def main(role: str = "app") -> int:
    eng = get_engine(role)
    with eng.connect() as cx:
        who = cx.execute(text("SELECT CURRENT_USER()")).scalar()
        ver = cx.execute(text("SELECT @@version")).scalar()
        print(f"connected as {who} to MySQL {ver}")
        for t in TABLES:
            n = cx.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t:20} {n:>8}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "app"))
