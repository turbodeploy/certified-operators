-- Discount associated with a specific business account.
-- Customers negotiate discounts with Cloud Vendors.
-- All discounts to be entered by user [Cannot be discovered].
-- It's one to one mapping between busniess account and discount.
-- Discount is assumed on global region basis. We could expend it to support tier-level if needed.

-- This table stores all account discount information.
CREATE TABLE discount (
    -- The id of discount.
    id                                 BIGINT          NOT NULL,

    -- The business account id owns this reserved instance.
    associated_account_id              BIGINT          UNIQUE NOT NULL,

    -- The serialized protobuf object contains detailed information about the discount.
    -- Each discount info could have account-level, service-levels, and tier-levels.
    discount_info                      BLOB            NOT NULL,

    PRIMARY KEY (id)
)