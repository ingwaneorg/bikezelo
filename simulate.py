import sqlite3
import os
import time
import random
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "orders.db")

STATUSES = ["NEW", "PAID", "SHIPPED", "REFUNDED"]
BAD_STATUSES = ["PENDING", "CANCELLED", "UNKNOWN"]
INTERVAL = 2  # seconds between rows


def random_customer_id():
    return f"CUST{random.randint(1000, 9999)}"


def random_amount():
    return round(random.uniform(5.00, 950.00), 2)


def make_bad_row():
    """One of four bad row types, chosen at random."""
    bad_type = random.choice(["null_customer", "bad_amount", "bad_status", "bad_timestamp"])

    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    customer_id = random_customer_id()
    order_amount = random_amount()
    status = random.choice(STATUSES)

    if bad_type == "null_customer":
        customer_id = None
    elif bad_type == "bad_amount":
        order_amount = round(random.uniform(-500.00, -1.00), 2)
    elif bad_type == "bad_status":
        status = random.choice(BAD_STATUSES)
    elif bad_type == "bad_timestamp":
        timestamp = "NOT-A-TIMESTAMP"

    return (timestamp, customer_id, order_amount, status)


def make_good_row():
    return (
        datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        random_customer_id(),
        random_amount(),
        random.choice(STATUSES),
    )


def insert_row(conn, row):
    conn.execute("""
        INSERT INTO orders (timestamp, customer_id, order_amount, status)
        VALUES (?, ?, ?, ?)
    """, row)

    # Keep only the most recent 500 rows
    conn.execute("""
        DELETE FROM orders
        WHERE row_id NOT IN (
            SELECT row_id FROM orders
            ORDER BY row_id DESC
            LIMIT 500
        )
    """)
    conn.commit()


def simulate():
    print(f"Simulator started - writing to {DB_PATH}")
    print("Press Ctrl+C to stop.\n")

    conn = sqlite3.connect(DB_PATH)

    row_count = 0
    try:
        while True:
            # Roughly 1 in 8 rows is bad
            if random.random() < 0.125:
                row = make_bad_row()
                label = "BAD"
            else:
                row = make_good_row()
                label = "ok"

            insert_row(conn, row)
            row_count += 1
            print(f"[{row_count:>4}] {label}  {row[0]}  {str(row[1]):<12}  {str(row[2]):>8}  {row[3]}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    simulate()
