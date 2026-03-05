import great_expectations as gx


def get_rules(suite):
    """
    Add your Great Expectations rules here.
    The suite is re-built every validation cycle so changes take effect immediately.

    Available expectations:
    -----------------------------------------------------------------------
    # Not null check:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )

    # Value range check:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="order_amount",
            min_value=0,
            max_value=999.99
        )
    )

    # Valid values check:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["NEW", "PAID", "SHIPPED", "REFUNDED"]
        )
    )
    -----------------------------------------------------------------------
    Add your rules below this line:
    """

    # Step 1 - Catch missing customer IDs
    #      a) uncomment this code
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )

    # Step 2 - Catch order amounts outside range
    #      a) uncomment this code
    #      b) change the values if you want to
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="order_amount",
            min_value=0,
            max_value=999.99
        )
    )

    # Step 3 - Valid values check:
    #      a) copy the code from above
    #      b) change the values if you want to
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["NEW", "PAID", "SHIPPED", "REFUNDED"]
        )
    )

    # Step 4 - Your own rules
    #      a) change bin/simulate.sh
    #      b) then add a custom rule here




    return suite

