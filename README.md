# bikezelo ~ TechMart Pipeline Monitor

A lightweight pipeline monitoring dashboard that simulates a live data feed, validates incoming records against quality rules, and forecasts pipeline behaviour.

---

## Setup

Clone the repo and install dependencies:

```bash
git clone https://github.com/ingwaneorg/bikezelo.git
cd bikezelo
pip install -r requirements.txt
```

Activate a virtual enviroment


Initialise the database:

```bash
python setup_db.py
```

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

Then open a browser and go to `http://localhost:5000`.

---

## What it does

`simulate.py` writes a new TechMart order record to the database every 2 seconds. Most rows are valid, but roughly 1 in 8 is intentionally bad — missing values, invalid amounts, bad status codes, or malformed timestamps.

Occasionally an **incident spike** fires: a burst of 4–8 consecutive bad rows, printed to the terminal so you can watch the dashboard react in real time.

The dashboard has two panels:

**Live feed** — rows appear as they arrive. Each row starts white (unvalidated), then turns green (passed), amber (warning), or red (failed) when the next validation sweep runs.

**Pipeline stats** — total rows, passed, warnings, errors, error rate, and a forecast of rows and errors per hour based on the current arrival rate.

Validation runs every 10 seconds against the whole dataset using Great Expectations rules defined in `rules.py`. If `rules.py` contains an error, the dashboard will show a message rather than silently passing all rows.

---

## Adding rules

Open `rules.py`. It has numbered steps — uncomment the examples or add your own. Save the file and the next validation sweep picks up the changes automatically, no restart needed.

**FAIL rules** — rows that break these turn red:
```python
def get_rules(suite):
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    return suite
```

**WARNING rules** — rows that break these turn amber:
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

---

## Testing rules

To generate a predictable set of edge-case rows instead of random data, use:

```bash
python test_simulate.py
```

This cycles through 22 hand-crafted rows covering nulls, boundary values, bad statuses, and malformed timestamps — useful when you want to verify that a new rule catches exactly what you expect.

---

## Project structure

```
bikezelo/
├── app.py                # Flask app - serves the dashboard and runs validation
├── rules.py              # Great Expectations rules - edit this
├── simulate.py           # Writes a live stream of orders to the database
├── setup_db.py           # One-time database setup
├── requirements.txt
├── bin/
│   └── select.sh         # Query the database from the terminal
├── data/
│   └── orders.db         # SQLite database (created by setup_db.py)
├── tests/
│   ├── test_app.py       # Unit tests
│   └── test_simulate.py  # Writes predictable edge-case rows for testing rules
└── templates/
    └── index.html        # Dashboard
```
