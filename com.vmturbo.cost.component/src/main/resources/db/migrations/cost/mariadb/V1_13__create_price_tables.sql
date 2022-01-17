-- This table stores the cloud price tables discovered by different probes.
-- For more information about price tables see com.vmturbo.common.protobuf/cost/Pricing.proto
CREATE TABLE price_table (
    -- The probe type for the origin of the price table.
    -- We assume that all price tables discovered by a particular probe are
    -- equivalent.
    associated_probe_type             VARCHAR(255)    NOT NULL,

    -- The time of the last update of the price table. This should generally be the time of the last
    -- topology broadcast. The time is UTC.
    last_update_time                  TIMESTAMP       NOT NULL,

    -- A serialized PriceTable protobuf message containing the actual price data.
    price_table_data                  BLOB            NOT NULL,

    -- A serialized ReservedInstancePriceTable protobuf message containing the price data
    -- for reserved instances.
    ri_price_table_data               BLOB            NOT NULL,

    PRIMARY KEY (associated_probe_type)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
