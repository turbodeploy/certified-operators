-- A group of reserved instance fields in different combination, if anyone of them changed,
-- the reserved instance price could be also changed.
CREATE TABLE reserved_instance_spec (
    -- The id of reserved instance spec.
    id                               BIGINT           NOT NULL,

    -- The type of reserved instance offering.
    offering_class                   INT              NOT NULL,

    -- The payment option of the reserved instance.
    payment_option                   INT              NOT NULL,

    -- The term of years of the reserved instance.
    term_years                       INT              NOT NULL,

    -- The tenancy of the reserved instance.
    tenancy                          INT              NOT NULL,

    -- The operating system of the reserved instance.
    os_type                          INT              NOT NULL,

    -- The entity profile id of the reserved instance using, such as t2.large.
    tier_id                          BIGINT           NOT NULL,

    -- The region id of the reserved instance.
    region_id                        BIGINT           NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance spec.
    reserved_instance_spec_info      BLOB             NOT NULL,

    PRIMARY KEY (id)
);

-- This table stores all user bought reserved instance information.
CREATE TABLE reserved_instance_bought (
    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The business account id owns this reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The probe send out unique id for the reserved instance.
    probe_reserved_instance_id         VARCHAR(255)    NOT NULL,

    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info      BLOB            NOT NULL,

    -- The foreign key referring to reserved instance spec table.
    FOREIGN  KEY (reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE,

    PRIMARY KEY (id)
);
