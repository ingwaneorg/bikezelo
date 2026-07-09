# bikezelo ~ TechMart Pipeline Monitor

![Version](https://img.shields.io/badge/version-1.1.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Teaching](https://img.shields.io/badge/module-DE5M6-blue)
![Python Version](https://img.shields.io/badge/python-3.9--3.12-blue.svg)

![Open Issues](https://img.shields.io/github/issues/ingwaneorg/bikezelo)
![Open PRs](https://img.shields.io/github/issues-pr/ingwaneorg/bikezelo)
![Last Commit](https://img.shields.io/github/last-commit/ingwaneorg/bikezelo)

A lightweight pipeline monitoring dashboard that preloads historical data, simulates a live data feed, validates incoming records against quality rules, and forecasts pipeline behaviour.

---

## Setup

### Step 1: Clone the repo

```bash
git clone https://github.com/ingwaneorg/bikezelo.git
cd bikezelo
```

### Step 2: Install dependencies:

*Optional: Create and activate a virtual environment ~ we will skip this part*

```bash
pip install -r requirements.txt
```

### Step 3: Initialise the database:

```bash
python setup_db.py
```

### Step 4: Preload seed data (optional):

```bash
python preload.py
```

This batch-loads around 40 historical orders so the dashboard opens with data already in it, instead of an empty feed. It's a run-once step - see [Preloading data](#preloading-data) for what it does.

---

## Running

Open two terminals.

**Terminal 1 - start the simulation:**
```bash
python simulate.py
```

**Terminal 2 - start the app:**
```bash
python app.py
```

Then open a browser and go to: [http://localhost:5000](http://localhost:5000)

---

## What it does

`simulate.py` writes a new TechMart order record to the database every 2 seconds. Most rows are valid, but roughly 1 in 8 is intentionally bad - missing values, invalid amounts, bad status codes, or malformed timestamps.

Occasionally an **incident spike** fires: a burst of 4-8 consecutive bad rows, printed to the terminal so you can watch the dashboard react in real time.

The dashboard has two panels:

**Live feed** - rows appear as they arrive. Each row starts white (unvalidated), then turns green (passed), amber (warning), or red (failed) when the next validation sweep runs.

**Pipeline stats** - total rows, passed, warnings, errors, error rate, and a forecast of rows and errors per hour based on the current arrival rate.

Validation runs every 10 seconds against the whole dataset using Great Expectations rules defined in `rules.py`. If `rules.py` contains an error, the dashboard will show a message rather than silently passing all rows.

---

## Preloading data

`preload.py` seeds the database with a batch of historical orders before you start the live feed - the "backfill, then stream" pattern real pipelines use to load history in bulk and switch to streaming for what comes next. It's built with [dlt](https://dlthub.com), a data-loading library you'll meet again in industry.

The seed file `data/seed_orders.json` is *nested* - each order carries a `customer` object and a list of `items`:

```json
{
  "order_id": "ORD-0001",
  "timestamp": "2026-03-05T08:02:14",
  "status": "PAID",
  "customer": { "customer_id": "CUST1042", "name": "Aisha Bello", "region": "London" },
  "items": [
    { "sku": "SKU-1001", "qty": 1, "unit_price": 199.99 },
    { "sku": "SKU-1044", "qty": 2, "unit_price": 24.50 }
  ]
}
```

Preload works in two layers, so after running it you'll see **three** tables in the database, not one:

| Table               | Layer            | What's in it                                                         |
|---------------------|------------------|---------------------------------------------------------------------|
| `raw_orders`        | raw (bronze)     | one row per order, `customer` flattened in, as the source arrived    |
| `raw_orders__items` | raw (bronze)     | one row per line item, linked back to its parent order              |
| `orders`            | curated (silver) | the clean model the app reads - `order_amount` totalled from items   |

dlt does the unnesting for you: it builds `raw_orders` and the `raw_orders__items` child table and wires them together with its own generated keys. `preload.py` then rolls the items up (`SUM(qty * unit_price)`) into a single `order_amount` per order and writes just the four business columns into `orders`. The app never touches the raw tables, so `simulate.py` and `app.py` carry on unchanged.

> **Callback to M3 / M5:** you flattened nested JSON by hand with `pandas.json_normalize()` and managed the parent-child link yourself. This is the same shape - here you watch a data-loading tool build the child table and the keys for you.

Preload is a run-once buffer. Those seed rows carry the lowest row IDs, so once the simulator has written 500 newer rows they age out on their own (the database is capped at 500 rows).

---

## Adding rules

Open `rules.py`. It has numbered steps - uncomment the examples or add your own. Save the file and the next validation sweep picks up the changes automatically, no restart needed.

**FAIL rules** - rows that break these turn red:
```python
def get_failures(suite):
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    return suite
```

**WARNING rules** - rows that break these turn amber:
```python
def get_warnings(suite):
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="timestamp",
            regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
        )
    )
    return suite
```

**Available expectations:**

| Expectation                          | What it checks                      |
|--------------------------------------|-------------------------------------|
| `ExpectColumnValuesToNotBeNull`      | column must have a value            |
| `ExpectColumnValuesToBeBetween`      | numeric value within a min/max range|
| `ExpectColumnValuesToBeInSet`        | value must be one of a fixed list   |
| `ExpectColumnValuesToMatchRegex`     | value must match a pattern          |
| `ExpectColumnValueLengthsToBeBetween`| string length within a min/max range|
| `ExpectColumnValuesToBeUnique`       | no duplicate values in the column   |

The seed data in `data/seed_orders.json` includes a few deliberately bad orders - a null customer, a bad status, an over-range amount, a malformed timestamp - so as you uncomment each rule you can watch it catch exactly the seed rows it should.

---

## Testing rules

**Testing your rules with predictable data**

To generate a predictable set of edge-case rows instead of random data, run this in place of `simulate.py`:

```bash
python tests/test_simulate.py
```

This cycles through 25 hand-crafted rows covering nulls, boundary values, bad statuses, and malformed timestamps - useful when you want to verify that a new rule catches exactly the rows you expect.

**Unit tests**

To run the project's unit tests:

```bash
venv/bin/pytest tests/
```

These test the Flask endpoints and forecast logic in `app.py`. They finish in a few seconds and do not require the simulator or app to be running.

---

## Project structure

```
bikezelo/
├── app.py                # Flask app - serves the dashboard and runs validation
├── rules.py              # Great Expectations rules - edit this
├── simulate.py           # Writes a live stream of orders to the database
├── preload.py            # One-time batch load of seed data (raw -> curated)
├── setup_db.py           # One-time database setup
├── requirements.txt
├── data/
│   ├── orders.db         # SQLite database (created by setup_db.py)
│   └── seed_orders.json  # Nested seed data for preload.py
├── tests/
│   ├── test_app.py       # Unit tests
│   └── test_simulate.py  # Writes predictable edge-case rows for testing rules
└── templates/
    └── index.html        # Dashboard
```
