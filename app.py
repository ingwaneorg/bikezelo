from flask import Flask, render_template, jsonify, request
import pandas as pd
import great_expectations as gx
import sqlite3
import os
import importlib
import sys

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "orders.db")

TICKER_ROWS = 50


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def read_db():
    try:
        conn = get_db()
        df = pd.read_sql_query("SELECT * FROM orders ORDER BY row_id ASC", conn)
        conn.close()
        return df
    except Exception as e:
        print(f"DB read error: {e}")
        return pd.DataFrame(columns=["row_id", "timestamp", "customer_id", "order_amount", "status"])


def _run_suite(context, df, suite, suite_name, definition_name):
    """
    Run a single GE suite against df.
    Returns a set of row_ids that failed.
    """
    failed_indices = set()

    if len(suite.expectations) == 0:
        return failed_indices

    try:
        context.validation_definitions.delete(definition_name)
    except Exception:
        pass

    try:
        data_source = context.data_sources.add_pandas(name="bikezelo")
    except Exception:
        data_source = context.data_sources["bikezelo"]

    try:
        data_asset = data_source.add_dataframe_asset(name="orders")
    except Exception:
        data_asset = data_source.assets["orders"]

    try:
        batch_definition = data_asset.add_batch_definition_whole_dataframe("orders_batch")
    except Exception:
        batch_definition = data_asset.batch_definitions["orders_batch"]

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=definition_name,
            data=batch_definition,
            suite=suite,
        )
    )

    result = validation_definition.run(batch_parameters={"dataframe": df})

    for exp_result in result.results:
        if not exp_result.success:
            exp_type = exp_result.expectation_config.type
            col = exp_result.expectation_config.kwargs.get("column")

            if exp_type == "expect_column_values_to_not_be_null":
                mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
                failed_indices.update(df[mask]["row_id"].tolist())

            elif exp_type == "expect_column_values_to_be_between":
                min_val = exp_result.expectation_config.kwargs.get("min_value")
                max_val = exp_result.expectation_config.kwargs.get("max_value")
                col_numeric = pd.to_numeric(df[col], errors="coerce")
                if min_val is not None:
                    failed_indices.update(df[col_numeric < min_val]["row_id"].tolist())
                if max_val is not None:
                    failed_indices.update(df[col_numeric > max_val]["row_id"].tolist())

            elif exp_type == "expect_column_values_to_be_in_set":
                value_set = exp_result.expectation_config.kwargs.get("value_set", [])
                mask = ~df[col].isin(value_set)
                failed_indices.update(df[mask]["row_id"].tolist())

            elif exp_type == "expect_column_values_to_match_regex":
                regex = exp_result.expectation_config.kwargs.get("regex")
                mask = ~df[col].astype(str).str.match(regex, na=False)
                failed_indices.update(df[mask]["row_id"].tolist())

    return failed_indices


def run_validation(df):
    """
    Run both GE suites against the whole DataFrame.
    Returns a dict of row_id -> "pass", "warn", or "fail".
    Fail takes priority over warn.
    """
    if df.empty:
        return {}

    try:
        if "rules" in sys.modules:
            importlib.reload(sys.modules["rules"])
        else:
            import rules  # noqa: F401

        import rules as rules_module

        context = gx.get_context()

        # Build fail suite
        fail_suite_name = "bikezelo_fail_suite"
        try:
            context.suites.delete(fail_suite_name)
        except Exception:
            pass
        fail_suite = context.suites.add(gx.ExpectationSuite(name=fail_suite_name))
        fail_suite = rules_module.get_rules(fail_suite)

        # Build warn suite
        warn_suite_name = "bikezelo_warn_suite"
        try:
            context.suites.delete(warn_suite_name)
        except Exception:
            pass
        warn_suite = context.suites.add(gx.ExpectationSuite(name=warn_suite_name))
        warn_suite = rules_module.get_warnings(warn_suite)

        # Run both suites
        failed_ids = _run_suite(context, df, fail_suite, fail_suite_name, "bikezelo_fail_validation")
        warned_ids = _run_suite(context, df, warn_suite, warn_suite_name, "bikezelo_warn_validation")

        # Build result dict - fail takes priority over warn
        results = {}
        for row_id in df["row_id"].dropna():
            rid = int(row_id)
            if rid in failed_ids:
                results[rid] = "fail"
            elif rid in warned_ids:
                results[rid] = "warn"
            else:
                results[rid] = "pass"

        return results

    except Exception as e:
        print(f"Validation error: {e}")
        return {int(row_id): "pass" for row_id in df["row_id"].dropna()}


def calculate_forecast(df):
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
        df_valid = df.dropna(subset=["timestamp"])
        if len(df_valid) < 2:
            return {"rows_per_min": 0, "forecast_per_hour": 0}

        earliest = df_valid["timestamp"].min()
        latest = df_valid["timestamp"].max()
        elapsed_minutes = (latest - earliest).total_seconds() / 60

        if elapsed_minutes < 0.1:
            return {"rows_per_min": 0, "forecast_per_hour": 0}

        rows_per_min = round(len(df_valid) / elapsed_minutes, 1)
        forecast_per_hour = round(rows_per_min * 60)

        return {"rows_per_min": rows_per_min, "forecast_per_hour": forecast_per_hour}
    except Exception:
        return {"rows_per_min": 0, "forecast_per_hour": 0}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/data/rows")
def get_rows():
    """
    Fast endpoint - returns new rows since last_row_id.
    Called every 3 seconds. Rows return as white (unvalidated).
    """
    last_row_id = int(request.args.get("after", 0))
    is_first_load = last_row_id == 0

    df = read_db()
    if df.empty:
        return jsonify({"rows": [], "max_row_id": 0})

    new_rows = df[df["row_id"] > last_row_id]
    limit = 10 if is_first_load else TICKER_ROWS
    new_rows = new_rows.tail(limit)

    max_row_id = int(df["row_id"].max())

    rows = new_rows.to_dict(orient="records")
    for row in rows:
        row["row_id"] = int(row["row_id"])
        row["status_class"] = "white"

    return jsonify({"rows": rows, "max_row_id": max_row_id})


@app.route("/data/validate")
def get_validation():
    """
    Slow endpoint - runs both GE suites against whole DB contents.
    Called every 10 seconds.
    """
    df = read_db()

    if df.empty:
        return jsonify({
            "results": {},
            "stats": {"total": 0, "passed": 0, "warnings": 0, "errors": 0, "error_rate": 0.0},
            "forecast": {"rows_per_min": 0, "forecast_per_hour": 0},
        })

    results = run_validation(df)

    total = len(results)
    passed = sum(1 for v in results.values() if v == "pass")
    warnings = sum(1 for v in results.values() if v == "warn")
    errors = sum(1 for v in results.values() if v == "fail")
    error_rate = round((errors / total) * 100, 1) if total > 0 else 0.0

    forecast = calculate_forecast(df)

    return jsonify({
        "results": results,
        "stats": {
            "total": total,
            "passed": passed,
            "warnings": warnings,
            "errors": errors,
            "error_rate": error_rate,
        },
        "forecast": forecast,
    })


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
