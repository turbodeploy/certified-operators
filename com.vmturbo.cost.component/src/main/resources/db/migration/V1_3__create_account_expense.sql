-- Top down Cloud account expense discovered by the billing probe, associated with a specific account

-- This table stores all account expense information.
CREATE TABLE account_expenses (
    -- The business account id owns this reserved instance.
    associated_account_id              BIGINT          NOT NULL,

    -- The serialized protobuf object contains detailed information about the account expense.
    -- Each account expenses info could have service and tier expenses.
    account_expenses_info              BLOB            NOT NULL,

    -- The timestamp at which the expense is received by Cost component.
    -- Note: It should be the timestamp that the expenses were generated.
    -- But currently probe doesn't have this information in it's DTO.
    -- The time is in "system time" and not necessarily UTC.
    received_time                      TIMESTAMP       NOT NULL,
    PRIMARY KEY (associated_account_id, received_time)
)