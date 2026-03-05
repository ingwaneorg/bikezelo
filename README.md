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

---

## Running

Open two terminals.

**Terminal 1 - start the simulation:**
```bash
bash bin/simulate.sh
```

**Terminal 2 - start the app:**
```bash
python app.py
```

Open a browser and go to:
```
http://localhost:5000
```

---

## What it does

`simulate.sh` writes a new TechMart order record to `data/orders.csv` every 2 seconds. Occasionally it inserts bad rows - missing values, invalid amounts, bad status codes, malformed timestamps.

The dashboard has two panels:

**Live feed** - rows appear as they arrive. Each row starts white (unvalidated), then turns green (passed) or red (failed) when the next validation sweep runs.

**Pipeline stats** - total rows, passed, warnings, errors, error rate, and a forecast of rows and errors per hour based on the current arrival rate.

Validation runs every 30 seconds against the whole dataset using Great Expectations rules defined in `rules.py`.

---

## Adding rules

Open `rules.py` in VS Code. Uncomment the example rules or add your own. Save the file - the app reloads automatically and the next validation sweep will apply your rules.

```python
def get_rules(suite):
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    return suite
```

---

## Project structure

```
bikezelo/
├── app.py          # Flask app - serves the dashboard and runs validation
├── rules.py        # Great Expectations rules - edit this
├── requirements.txt
├── bin/
│   └── simulate.sh # Simulates a live data feed
├── data/
│   └── orders.csv  # Live data file - written by simulate.sh
└── templates/
    └── index.html  # Dashboard
```

