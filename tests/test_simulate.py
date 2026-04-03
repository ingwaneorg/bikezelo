import sqlite3
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "orders.db")

INTERVAL = 1.0  # faster than simulate.py so rows cycle quickly

TEST_ROWS = [
    # Good rows
    ("2026-03-06T10:00:00", "CUST1234", 194.31, "PAID"),
    ("2026-03-06T10:00:01", "CUST9999", 999.99, "SHIPPED"),
    ("2026-03-06T10:00:02", "CUST0001",   0.01, "NEW"),
    ("2026-03-06T10:00:03", "CUST5678", 500.00, "REFUNDED"),

    # Null customer_id
    ("2026-03-06T10:00:04", None, 194.31, "PAID"),

    # Bad order_amount
    ("2026-03-06T10:00:05", "CUST1234",    -1.00, "PAID"),
    ("2026-03-06T10:00:06", "CUST1234",  -999.99, "PAID"),
    ("2026-03-06T10:00:07", "CUST1234", 99999.99, "PAID"),

    # Boundary order_amount
    ("2026-03-06T10:00:08", "CUST1234", 0.00,    "PAID"),
    ("2026-03-06T10:00:09", "CUST1234", 999.99,  "PAID"),
    ("2026-03-06T10:00:10", "CUST1234", 1000.00, "PAID"),

    # Bad status
    ("2026-03-06T10:00:11", "CUST1234", 194.31, "PENDING"),
    ("2026-03-06T10:00:12", "CUST1234", 194.31, "CANCELLED"),
    ("2026-03-06T10:00:13", "CUST1234", 194.31, "UNKNOWN"),
    ("2026-03-06T10:00:14", "CUST1234", 194.31, ""),

    # Bad timestamp
    ("NOT-A-TIMESTAMP",     "CUST1234", 194.31, "PAID"),
    ("2026-13-06T10:00:00", "CUST1234", 194.31, "PAID"),
    ("06-03-2026",          "CUST1234", 194.31, "PAID"),
    ("",                    "CUST1234", 194.31, "PAID"),

    # Bad order_amount type
    ("2026-03-06T10:00:15", "CUST1234", None, "PAID"),

    # Oversized fields
    ("2026-03-06T10:00:16", "CUST"+"X"*20, 194.31, "PAID"),
    ("2026-03-06T10:00:17", "CUST1234",    194.31, "PAID"+"X"*20),

    # Completely null row
    (None, None, None, None),

    # Multiple bad fields in one row
    (None, None, -999.99, "UNKNOWN"),
    ("NOT-A-TIMESTAMP", None, -1.00, "CANCELLED"),
]


def insert_row(conn, row):
    conn.execute("""
        INSERT INTO orders (timestamp, customer_id, order_amount, status)
        VALUES (?, ?, ?, ?)
    """, row)
    conn.execute("""
        DELETE FROM orders
        WHERE row_id NOT IN (
            SELECT row_id FROM orders
            ORDER BY row_id DESC
            LIMIT 500
        )
    """)
    conn.commit()


def test_simulate():
    print(f"Test simulator started - writing to {DB_PATH}")
    print(f"{len(TEST_ROWS)} test rows in rotation")
    print("Press Ctrl+C to stop.\n")

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")

    cursor = conn.execute("SELECT COALESCE(MAX(row_id), 0) FROM orders")
    row_count = cursor.fetchone()[0]

    loop = 0
    try:
        while True:
            loop += 1
            print(f"--- loop {loop} ---")
            for row in TEST_ROWS:
                insert_row(conn, row)
                row_count += 1
                label = str(row)[:80]
                print(f"[{row_count:>4}]  {label}")
                time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nTest simulator stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    test_simulate()

