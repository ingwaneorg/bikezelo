"""
Preload step for the bikezelo pipeline monitor.

Batch-loads a seed file of historical TechMart orders before the live feed starts,
so the dashboard opens with data already in it - a small "backfill, then stream"
pattern, the way real pipelines seed history in bulk and switch to streaming after.

Two layers:

  1. RAW (bronze) - dlt reads data/seed_orders.json and builds two tables it fully
     owns: raw_orders (one row per order, the nested `customer` object flattened in)
     and raw_orders__items (one row per line item, linked back to its parent by dlt's
     own generated keys). This is the faithful, messy copy of the source.

  2. CURATED (silver) - a plain INSERT ... SELECT rolls the line items up into a
     single order_amount per order and writes just the four business columns into
     `orders`, the table the app already reads.

`orders` is never touched by dlt, so app.py and simulate.py carry on unchanged.

Run once, after setup_db.py and before the simulator:
    python setup_db.py
    python preload.py
"""
import os
import json
import sqlite3
import dlt
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "orders.db")
SEED_PATH = os.path.join(BASE_DIR, "data", "seed_orders.json")

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def shift_timestamps(orders):
    """
    Shift every parseable timestamp forward so the seed data ends right before
    now, preserving the spacing between orders. seed_orders.json is fixed at
    2026-03-05 for readability; without this shift the preloaded rows sit months
    before simulate.py's live, datetime.now()-stamped rows, and calculate_forecast()
    (app.py) divides by that multi-month span - rows_per_min rounds to 0 until
    enough live rows push the March rows out of the 500-row cap.

    The deliberately malformed row (ORD-0029, "05/03/2026 10:58") is left as-is -
    it isn't a real date, so shifting it has no defined meaning, and it needs to
    stay unparseable to exercise the regex expectation in rules.py.
    """
    parsed = {}
    for i, order in enumerate(orders):
        try:
            parsed[i] = datetime.strptime(order["timestamp"], TIMESTAMP_FORMAT)
        except (KeyError, ValueError):
            continue

    if not parsed:
        return orders

    offset = datetime.now() - max(parsed.values())
    for i, dt in parsed.items():
        orders[i]["timestamp"] = (dt + offset).strftime(TIMESTAMP_FORMAT)

    return orders


def load_raw():
    """
    Layer 1 - dlt loads the nested JSON straight into orders.db.

    dlt does the unnesting for us: the `customer` object is flattened into columns
    on raw_orders (customer__customer_id, customer__name, customer__region) and the
    `items` list is exploded into a child table, raw_orders__items, wired back to the
    parent by _dlt_parent_id -> _dlt_id. No unnesting code on our side.
    """
    with open(SEED_PATH, encoding="utf-8") as f:
        orders = json.load(f)
    orders = shift_timestamps(orders)

    # SQLAlchemy wants forward slashes in the SQLite URL even on Windows, so
    # normalise backslashes here (a no-op on Linux and the Pi).
    db_url = "sqlite:///" + DB_PATH.replace("\\", "/")

    pipeline = dlt.pipeline(
        pipeline_name="bikezelo_preload",
        # dataset_name="main" writes straight into orders.db - no separate attached file
        destination=dlt.destinations.sqlalchemy(credentials=db_url),
        dataset_name="main",
    )

    # write_disposition="replace" - re-running preload rebuilds the raw layer
    # cleanly instead of appending a second copy.
    #
    # columns={"timestamp": {"data_type": "text"}} - without this, dlt infers the
    # ISO string as a datetime, reformats it (space instead of "T", trailing
    # .000000), and turns the deliberately malformed ORD-0029 row into NULL. The
    # hint keeps timestamp a faithful, unparsed copy of the source string - both the
    # good and the bad rows land exactly as written in the JSON.
    load_info = pipeline.run(
        orders,
        table_name="raw_orders",
        write_disposition="replace",
        columns={"timestamp": {"data_type": "text"}},
    )
    print(load_info)
    return len(orders)


def curate():
    """
    Layer 2 - roll the items up into one order_amount per order and write the four
    business columns into `orders`.

    LEFT JOIN, not INNER: an order with no items (or all-bad items) still lands, as a
    NULL amount, rather than silently vanishing. row_id is left out so SQLite's
    AUTOINCREMENT assigns fresh ids, and ORDER BY timestamp keeps them chronological
    so the dashboard's row numbers ascend with time.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # orders must already exist - setup_db.py owns its schema
    exists = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'"
    ).fetchone()
    if not exists:
        conn.close()
        raise SystemExit("Table 'orders' not found - run 'python setup_db.py' first.")

    # Show what dlt actually built - handy as a teaching aid, and a quick check that
    # the flattened column names below match what dlt generated on your version.
    cols = [row[1] for row in cursor.execute("PRAGMA table_info(raw_orders)").fetchall()]
    print("\nraw_orders columns:", ", ".join(cols))

    # Reset the buffer so preload is repeatable and never double-seeds `orders`.
    cursor.execute("DELETE FROM orders")

    cursor.execute("""
        INSERT INTO orders (timestamp, customer_id, order_amount, status)
        SELECT
            o.timestamp,
            o.customer__customer_id,
            ROUND(SUM(i.qty * i.unit_price), 2),
            o.status
        FROM raw_orders o
        LEFT JOIN raw_orders__items i
               ON i._dlt_parent_id = o._dlt_id
        GROUP BY o._dlt_id
        ORDER BY o.timestamp
    """)

    written = cursor.rowcount
    conn.commit()
    conn.close()
    return written


def preload():
    print(f"Preloading from {SEED_PATH}")
    raw_count = load_raw()
    written = curate()
    print(f"\nRaw orders loaded  : {raw_count}")
    print(f"Curated into orders: {written}")
    print("orders reset to the seed buffer - safe to start the simulator now.")


if __name__ == "__main__":
    preload()

