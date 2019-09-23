-- Table to store the compute tier demand stats data.
CREATE TABLE compute_tier_type_hourly_by_week (

    -- Hour of the day. Value range [0-23]
    hour                            TINYINT(2) NOT NULL,

    -- Day of the week. Value range [1 (Sunday) - 7 (Saturday)]
    day                             TINYINT(1) NOT NULL,

    -- The BusinessAccount OID of the master account this instance
    -- belongs to.
    account_id                      BIGINT NOT NULL,

    -- The OID of the Compute Tier entity consumed by the VMs.
    compute_tier_id                 BIGINT NOT NULL,

    -- The OID of the availability zone of this instance.
    availability_zone               BIGINT NOT NULL,

    -- Type of platform/OS. Store the enum value.
    platform                        TINYINT not NULL,

    -- The tenancy defines what hardware the instance is running on.
    -- Store the enum value.
    tenancy                         TINYINT not NULL,

    -- This is the weighted average of the histogram of the compute
    --  tier consumed by the VMs.
    -- This value is calculated from the live source topology.
    count_from_source_topology      DECIMAL(15, 3),

    -- Same as above. But the histogram is calculated from the projected topology.
    count_from_projected_topology   DECIMAL(15, 3)  DEFAULT NULL,

    PRIMARY KEY (hour, day, account_id, compute_tier_id, availability_zone, platform, tenancy)
);

