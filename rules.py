"""
Great Expectations rules

orders schema:
row_id, timestamp, customer_id, order_amount, status

Timestamp format: 2026-03-05T23:54:31
"""
import great_expectations as gx


def get_rules(suite):
    """
    FAIL rules - rows that break these turn RED on the dashboard.

    Available expectations:
    -----------------------------------------------------------------------
    # Not null check:
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")

    # Value range check:
    gx.expectations.ExpectColumnValuesToBeBetween(

    # Valid values check:
    gx.expectations.ExpectColumnValuesToBeInSet(

    -----------------------------------------------------------------------
    Add your FAIL rules below this line:
    """

    # Step 1 - Catch missing customer IDs
    #      a) uncomment the code below (3 lines)
    ##################################
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )

    # Step 2 - Catch order amounts outside range
    #      a) uncomment the code below (7 lines)
    #      b) change the min & max values if you want to
    ##################################
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="order_amount",
            min_value=0,
            max_value=999.99
        )
    )

    # Step 3 - Catch invalid status codes
    #      a) uncomment the code below (6 lines)
    #      b) change the value_set if you want to
    ##################################
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["NEW", "PAID", "SHIPPED", "REFUNDED"]
        )
    )

    # Step 4 - Your own rules
    ##################################

    return suite


def get_warnings(suite):
    """
    WARNING rules - rows that break these turn AMBER on the dashboard.
    A warning means the row is suspicious but not necessarily rejected.

    -----------------------------------------------------------------------
    Add your WARNING rules below this line:
    """

    # Step 5 - Catch malformed timestamps (warning, not a hard failure)
    #      a) uncomment the code below (5 lines)
    ##################################
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="timestamp",
            regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
        )
    )

    # Step 6 - Your own warnings
    ##################################

    return suite
