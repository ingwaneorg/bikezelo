"""
Great Expectations rules

orders schema:
row_id, timestamp, customer_id, order_amount, status

Timestamp format: 2026-03-05T23:54:31
"""
import great_expectations as gx


def get_failures(suite):
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

    # String length check:
    gx.expectations.ExpectColumnValueLengthsToBeBetween(

    # Uniqueness check:
    gx.expectations.ExpectColumnValuesToBeUnique(

    -----------------------------------------------------------------------
    Add your FAIL rules below this line:
    """

    # Step 1 - Every order must have a customer ID.
    #           Without one, the order can't be linked to an account — it's unusable.
    #      a) uncomment the code below (3 lines)
    #~~~~~~~~~~~~~~~~~~~~~~~>>>
    #suite.add_expectation(
    #    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    #)

    # Step 2 - Order amounts must be realistic.
    #           Negative values mean the data is corrupted; very high values are likely entry errors.
    #      a) uncomment the code below (7 lines)
    #      b) change the min & max values if you want to
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>
    #suite.add_expectation(
    #    gx.expectations.ExpectColumnValuesToBeBetween(
    #        column="order_amount",
    #        min_value=0,
    #        max_value=999.99
    #    )
    #)

    # Step 3 - Status must be one of the recognised codes.
    #           Anything else means the pipeline received a value it doesn't know how to process.
    #      a) uncomment the code below (6 lines)
    #      b) change the value_set if you want to
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>
    #suite.add_expectation(
    #    gx.expectations.ExpectColumnValuesToBeInSet(
    #        column="status",
    #        value_set=["NEW", "PAID", "SHIPPED", "REFUNDED"]
    #    )
    #)

    # Step 4 - Customer IDs must not be suspiciously long.
    #           A valid ID like CUST1234 is 8 characters. Anything much longer
    #           suggests corrupted or injected data.
    #      a) uncomment the code below (6 lines)
    #      b) adjust max_value if your IDs use a different format
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>
    #suite.add_expectation(
    #    gx.expectations.ExpectColumnValueLengthsToBeBetween(
    #        column="customer_id",
    #        min_value=4,
    #        max_value=12
    #    )
    #)

    # Step 5 - Your own rules
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>

    return suite


def get_warnings(suite):
    """
    WARNING rules - rows that break these turn AMBER on the dashboard.
    A warning means the row is suspicious but not necessarily rejected.

    -----------------------------------------------------------------------
    Add your WARNING rules below this line:
    """

    # Step 6 - Timestamps must be in the expected format.
    #           A malformed timestamp won't break the pipeline immediately, but it will
    #           cause problems downstream when anything tries to sort or filter by time.
    #           This is a warning rather than a hard failure — suspicious, but not rejected.
    #      a) uncomment the code below (5 lines)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>
    #suite.add_expectation(
    #    gx.expectations.ExpectColumnValuesToMatchRegex(
    #        column="timestamp",
    #        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
    #    )
    #)

    # Step 7 - Your own warnings
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>>

    return suite
