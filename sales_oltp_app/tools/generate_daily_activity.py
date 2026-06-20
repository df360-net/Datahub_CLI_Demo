"""Sales OLTP data generator.

Two modes:
  seed                       one-time: products + customers + N-day order-history backfill
  daily  --date YYYY-MM-DD   one business day: new orders + status transitions + dimension drift

Deterministic (fixed master seed) so runs are reproducible. Config-driven DB access via sales_db,
so the same script runs from Lenovo (dev) or inside the airflow container (prod). Connects as the
DML user (sales_app). Categories and stores are assumed already seeded (db/03_seed_reference.sql).
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
from decimal import ROUND_HALF_UP, Decimal

from faker import Faker
from sqlalchemy import text

from sales_db import get_engine

MASTER_SEED = 42
CENTS = Decimal("0.01")

CATEGORIES = {
    "Electronics":       ("ELEC", (29.99, 1499.99), ["Headphones", "Speaker", "Monitor", "Keyboard", "Charger", "Webcam", "Router"]),
    "Home & Kitchen":    ("HOME", (9.99, 399.99),   ["Blender", "Kettle", "Cookware Set", "Lamp", "Vacuum", "Toaster"]),
    "Apparel":           ("APPR", (12.99, 199.99),  ["Jacket", "Sneakers", "T-Shirt", "Jeans", "Hoodie", "Cap"]),
    "Sports & Outdoors": ("SPRT", (14.99, 599.99),  ["Tent", "Backpack", "Bottle", "Dumbbell Set", "Bike Helmet", "Yoga Mat"]),
    "Books":             ("BOOK", (6.99, 49.99),    ["Novel", "Cookbook", "Field Guide", "Biography", "Textbook"]),
    "Toys & Games":      ("TOYS", (5.99, 149.99),   ["Board Game", "Puzzle", "Building Set", "Action Figure", "Plush Toy"]),
}
SEGMENTS = (["CONSUMER"] * 70) + (["SMB"] * 22) + (["ENTERPRISE"] * 8)
DISCOUNTS = ([Decimal("0.00")] * 70) + ([Decimal("0.05")] * 15) + ([Decimal("0.10")] * 10) + ([Decimal("0.15")] * 5)


def _category_ids(cx) -> dict[str, int]:
    rows = cx.execute(text("SELECT category_id, category_name FROM product_categories")).all()
    return {name: cid for cid, name in rows}


def _store_ids(cx) -> list[int]:
    return [r[0] for r in cx.execute(text("SELECT store_id FROM stores")).all()]


def _load_products(cx) -> list[tuple[int, Decimal]]:
    return [(r[0], r[1]) for r in cx.execute(text("SELECT product_id, unit_price FROM products")).all()]


def _customer_ids(cx) -> list[int]:
    return [r[0] for r in cx.execute(text("SELECT customer_id FROM customers")).all()]


def seed_products(cx, rng: random.Random, fake: Faker, n: int) -> None:
    cat_ids = _category_ids(cx)
    rows, i = [], 0
    cat_names = list(CATEGORIES)
    for name in cat_names:
        prefix, (lo, hi), nouns = CATEGORIES[name]
        share = n // len(cat_names)
        for _ in range(share):
            i += 1
            price = round(rng.uniform(lo, hi), 2)
            rows.append({
                "sku": f"{prefix}-{i:05d}",
                "product_name": f"{fake.word().capitalize()} {rng.choice(nouns)}",
                "category_id": cat_ids[name],
                "unit_price": price,
                "active_flag": 0 if rng.random() < 0.05 else 1,
            })
    cx.execute(text(
        "INSERT INTO products (sku, product_name, category_id, unit_price, active_flag) "
        "VALUES (:sku, :product_name, :category_id, :unit_price, :active_flag)"), rows)
    print(f"  products inserted: {len(rows)}")


def seed_customers(cx, rng: random.Random, fake: Faker, n: int, end_date: dt.date) -> None:
    rows = []
    for i in range(1, n + 1):
        first, last = fake.first_name(), fake.last_name()
        signup = end_date - dt.timedelta(days=rng.randint(1, 730))
        rows.append({
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{i}@example.com".lower(),
            "segment": rng.choice(SEGMENTS),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "signup_date": signup,
        })
    cx.execute(text(
        "INSERT INTO customers (first_name, last_name, email, segment, city, state, country, signup_date) "
        "VALUES (:first_name, :last_name, :email, :segment, :city, :state, :country, :signup_date)"), rows)
    print(f"  customers inserted: {len(rows)}")


def _status_for(rng: random.Random, age_days: int) -> str:
    if age_days <= 1:
        return rng.choice(["PLACED", "PLACED", "PAID"])
    if age_days <= 4:
        return rng.choice(["PAID", "SHIPPED", "SHIPPED"])
    # terminal-ish for older orders
    r = rng.random()
    if r < 0.86:
        return "DELIVERED"
    if r < 0.93:
        return "RETURNED"
    return "CANCELLED"


def generate_orders_for_date(cx, rng, products, customer_ids, store_ids, date, n_orders, end_date) -> int:
    age = (end_date - date).days
    lines_made = 0
    for _ in range(n_orders):
        cust = rng.choice(customer_ids)
        store = rng.choice(store_ids)
        secs = rng.randint(9 * 3600, 21 * 3600)  # business hours-ish
        order_ts = dt.datetime.combine(date, dt.time()) + dt.timedelta(seconds=secs)
        nlines = rng.randint(1, 5)
        picks = rng.sample(products, min(nlines, len(products)))
        lines, total = [], Decimal("0.00")
        for ln, (pid, price) in enumerate(picks, start=1):
            qty = rng.randint(1, 4)
            disc = rng.choice(DISCOUNTS)
            line_amt = (qty * price * (Decimal(1) - disc)).quantize(CENTS, ROUND_HALF_UP)
            total += line_amt
            lines.append({"line_no": ln, "product_id": pid, "quantity": qty,
                          "unit_price": price, "discount_pct": disc})
        status = _status_for(rng, age)
        r = cx.execute(text(
            "INSERT INTO orders (customer_id, store_id, order_ts, order_date, status, order_total) "
            "VALUES (:c, :s, :ts, :d, :st, :tot)"),
            {"c": cust, "s": store, "ts": order_ts, "d": date, "st": status, "tot": total})
        oid = r.lastrowid
        cx.execute(text(
            "INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price, discount_pct) "
            "VALUES (:o, :line_no, :product_id, :quantity, :unit_price, :discount_pct)"),
            [{"o": oid, **ln} for ln in lines])
        lines_made += len(lines)
    return lines_made


def drift(cx, rng, fake, date) -> None:
    """Low-rate dimension change: a price bump, a segment change, a new customer."""
    # bump a few product prices
    pids = [r[0] for r in cx.execute(text(
        "SELECT product_id FROM products ORDER BY RAND(:s) LIMIT 5"), {"s": rng.randint(0, 1 << 30)}).all()]
    for pid in pids:
        cx.execute(text("UPDATE products SET unit_price = ROUND(unit_price * :f, 2) WHERE product_id = :p"),
                   {"f": rng.choice([1.05, 1.10, 0.95]), "p": pid})
    # change a few customers' segment
    cids = [r[0] for r in cx.execute(text(
        "SELECT customer_id FROM customers ORDER BY RAND(:s) LIMIT 3"), {"s": rng.randint(0, 1 << 30)}).all()]
    for cid in cids:
        cx.execute(text("UPDATE customers SET segment = :seg WHERE customer_id = :c"),
                   {"seg": rng.choice(SEGMENTS), "c": cid})
    # occasionally add a new customer
    if rng.random() < 0.5:
        first, last = fake.first_name(), fake.last_name()
        cx.execute(text(
            "INSERT INTO customers (first_name, last_name, email, segment, city, state, country, signup_date) "
            "VALUES (:f, :l, :e, :seg, :city, :st, 'USA', :d)"),
            {"f": first, "l": last, "e": f"{first}.{last}.{date:%Y%m%d}@example.com".lower(),
             "seg": rng.choice(SEGMENTS), "city": fake.city(), "st": fake.state_abbr(), "d": date})
    print(f"  drift: {len(pids)} price changes, {len(cids)} segment changes")


def advance_statuses(cx, rng, date) -> None:
    """Advance some in-flight orders along PLACED->PAID->SHIPPED->DELIVERED; bumps updated_at."""
    flow = {"PLACED": "PAID", "PAID": "SHIPPED", "SHIPPED": "DELIVERED"}
    moved = 0
    rows = cx.execute(text(
        "SELECT order_id, status FROM orders WHERE status IN ('PLACED','PAID','SHIPPED') "
        "ORDER BY RAND(:s) LIMIT 200"), {"s": rng.randint(0, 1 << 30)}).all()
    for oid, st in rows:
        if rng.random() < 0.6:
            cx.execute(text("UPDATE orders SET status = :ns WHERE order_id = :o"),
                       {"ns": flow[st], "o": oid})
            moved += 1
    print(f"  status transitions: {moved}")


def cmd_seed(args) -> None:
    rng = random.Random(MASTER_SEED)
    fake = Faker("en_US")
    fake.seed_instance(MASTER_SEED)
    end_date = dt.date.fromisoformat(args.end_date) if args.end_date else dt.date.today()
    eng = get_engine("app")
    with eng.begin() as cx:
        print("seeding products + customers...")
        seed_products(cx, rng, fake, args.products)
        seed_customers(cx, rng, fake, args.customers, end_date)
    with eng.connect() as cx:
        products = _load_products(cx)
        customer_ids = _customer_ids(cx)
        store_ids = _store_ids(cx)
    print(f"backfilling {args.history_days} days of orders ending {end_date}...")
    total_orders = total_lines = 0
    for d in range(args.history_days, 0, -1):
        day = end_date - dt.timedelta(days=d)
        n = rng.randint(args.min_orders, args.max_orders)
        with eng.begin() as cx:
            total_lines += generate_orders_for_date(cx, rng, products, customer_ids, store_ids, day, n, end_date)
            total_orders += n
    print(f"backfill done: {total_orders} orders, {total_lines} lines across {args.history_days} days")


def cmd_daily(args) -> None:
    rng = random.Random(MASTER_SEED + int(dt.date.fromisoformat(args.date).strftime("%Y%m%d")))
    fake = Faker("en_US")
    fake.seed_instance(MASTER_SEED)
    day = dt.date.fromisoformat(args.date)
    eng = get_engine("app")
    with eng.connect() as cx:
        products = _load_products(cx)
        customer_ids = _customer_ids(cx)
        store_ids = _store_ids(cx)
    n = rng.randint(args.min_orders, args.max_orders)
    with eng.begin() as cx:
        lines = generate_orders_for_date(cx, rng, products, customer_ids, store_ids, day, n, day)
        drift(cx, rng, fake, day)
        advance_statuses(cx, rng, day)
    print(f"daily {day}: {n} new orders, {lines} lines")


def main() -> None:
    p = argparse.ArgumentParser(description="Sales OLTP data generator")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed", help="one-time products + customers + order-history backfill")
    s.add_argument("--products", type=int, default=200)
    s.add_argument("--customers", type=int, default=2000)
    s.add_argument("--history-days", type=int, default=90, dest="history_days")
    s.add_argument("--end-date", default=None, help="last backfill date (default: today)")
    s.add_argument("--min-orders", type=int, default=80, dest="min_orders")
    s.add_argument("--max-orders", type=int, default=160, dest="max_orders")
    s.set_defaults(func=cmd_seed)

    d = sub.add_parser("daily", help="generate one business day of activity")
    d.add_argument("--date", required=True, help="YYYY-MM-DD")
    d.add_argument("--min-orders", type=int, default=120, dest="min_orders")
    d.add_argument("--max-orders", type=int, default=400, dest="max_orders")
    d.set_defaults(func=cmd_daily)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
