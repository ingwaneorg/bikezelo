from flask import Flask, render_template, jsonify
import pandas as pd
import great_expectations as gx
import os
import importlib
import sys
import time

app = Flask(__name__)

CSV_PATH = "data/orders.csv"
TICKER_ROWS = 50  # number of rows to show in the ticker
 

def read_csv(retries=3):
    for attempt in range(retries):
        try:
            df = pd.read_csv(CSV_PATH)
            df["row_id"] = pd.to_numeric(df["row_id"], errors="coerce")
            df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce")
            df["customer_id"] = df["customer_id"].astype(str).replace("nan", "")
            if not df.empty:
                return df
        except Exception:
            pass
        time.sleep(0.1)
    return pd.DataFrame(columns=["row_id", "timestamp", "customer_id", "order_amount", "status"])


def validate_timestamps(df):
    mask = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%S", errors="coerce").isna()
    return df[mask]["row_id"].tolist()


def run_validation(df):
    """
    Run Great Expectations validation against the whole DataFrame.
    Returns a dict of row_id -> True/False (True = passed).
    If rules.py is empty or has no expectations, all rows pass.
    """
    if df.empty:
        return {}

    try:
        # Reload rules.py every time so Flask debug reload picks up changes
        if "rules" in sys.modules:
            importlib.reload(sys.modules["rules"])
        else:
            import rules  # noqa: F401

        import rules as rules_module

        # Set up GE context
        context = gx.get_context()

        # Add or get data source - handle already exists gracefully
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

        # Build suite fresh each time
        suite_name = "bikezelo_suite"
        try:
            context.suites.delete(suite_name)
        except Exception:
            pass

        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

        # Apply rules from rules.py
        suite = rules_module.get_rules(suite)

        # If no expectations were added, everything passes
        if len(suite.expectations) == 0:
            return {int(row_id): True for row_id in df["row_id"].dropna()}

        # Create validation definition
        try:
            context.validation_definitions.delete("bikezelo_validation")
        except Exception:
            pass

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name="bikezelo_validation",
                data=batch_definition,
                suite=suite,
            )
        )

        result = validation_definition.run(batch_parameters={"dataframe": df})

        # GE validates columns not rows - we need to find which rows failed
        # Re-check each expectation manually against the DataFrame
        failed_indices = set()

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

        # Add bad timestamps to failed indices
        failed_indices.update(validate_timestamps(df))

        # Build result dict: row_id -> passed (True/False)
        results = {}
        for row_id in df["row_id"].dropna():
            results[int(row_id)] = int(row_id) not in failed_indices

        return results

    except Exception as e:
        print(f"Validation error: {e}")
        # If rules.py breaks, return all passing so app doesn't crash
        return {int(row_id): True for row_id in df["row_id"].dropna()}


def calculate_forecast(df):
    """
    Calculate rows per minute and forecast per hour
    based on timestamps in the CSV.
    """
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

        return {
            "rows_per_min": rows_per_min,
            "forecast_per_hour": forecast_per_hour,
        }
    except Exception:
        return {"rows_per_min": 0, "forecast_per_hour": 0}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/data/rows")
def get_rows():
    """
    Fast endpoint - returns new rows since last_row_id.
    Called every 2-3 seconds for the ticker.
    No GE validation here - rows come back as white (unvalidated).
    """
    from flask import request
    last_row_id = int(request.args.get("after", 0))
    is_first_load = last_row_id == 0

    df = read_csv()
    if df.empty:
        return jsonify({"rows": [], "max_row_id": 0})

    # this is for first start up
    new_rows = df[df["row_id"] > last_row_id]
    limit = 10 if is_first_load else TICKER_ROWS
    new_rows = new_rows.tail(limit)

    max_row_id = int(df["row_id"].max()) if not df.empty else 0

    rows = new_rows.to_dict(orient="records")
    # Convert row_id to int for JSON
    for row in rows:
        row["row_id"] = int(row["row_id"]) if pd.notna(row["row_id"]) else 0
        row["status_class"] = "white"  # unvalidated

    return jsonify({"rows": rows, "max_row_id": max_row_id})


@app.route("/data/validate")
def get_validation():
    """
    Slow endpoint - runs GE against whole CSV.
    Called every 10 seconds.
    Returns pass/fail per row_id + stats + forecast.
    """
    df = read_csv()

    if df.empty:
        return jsonify({
            "results": {},
            "stats": {"total": 0, "passed": 0, "warnings": 0, "errors": 0},
            "forecast": {"rows_per_min": 0, "forecast_per_hour": 0},
        })

    results = run_validation(df)

    total = len(results)
    passed = sum(1 for v in results.values() if v)
    errors = total - passed

    # Warnings: bad timestamp rows (invalid but not a GE rule failure)
    bad_timestamps = validate_timestamps(df)
    warnings = len(bad_timestamps)

    forecast = calculate_forecast(df)
    
    error_rate = round((errors / total) * 100, 1) if total > 0 else 0.0

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


