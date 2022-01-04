-- The attribute types tracked historically in the historical_entity_attrs table.
DROP TYPE IF EXISTS attr_type;
CREATE TYPE attr_type AS ENUM (
   -- The state of an entity, represented by the entity_state enum.
   -- We store the postgres OID of the enum as the int_value in the historical_entity_attrs table.
   'ENTITY_STATE',

   -- The number of VCPUs of a virtual machine, represented by an integer.
   'NUM_VCPU',

   -- Whether a virtual volume is attached or not.
   'VOLUME_ATTACHED'
);

-- START ENTITY_STATE SUPPORT
-- Function to support casting integers to the ENTITY_STATE attr_type.
CREATE OR REPLACE FUNCTION int2entity_state(x int)
RETURNS entity_state AS $$
BEGIN
  RETURN enumlabel::entity_state FROM pg_enum WHERE oid=x;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

-- Define a cast to allow treating an integer as an entity_state.
-- This allows us to have expressions like:
--     SELECT entity_oid, int_value::entity_state
--     FROM historical_entity_attrs
--     WHERE attr_type == 'ENTITY_STATE'
--         AND historical_entity_attrs.int_value::entity_state = 'POWERED_ON'
DROP CAST IF EXISTS (int AS entity_state);
CREATE CAST (int AS entity_state) WITH FUNCTION int2entity_state(int);
-- END ENTITY_STATE SUPPORT

-- This table tracks the historical attributes associated with an entity.
-- Each row represents the value of a particular attribute of an entity at a particular point in time.
-- New rows are written:
--    1. When the value of a property changes between topologies.
--    2. At regular intervals (e.g. daily) to keep a recent value available within a reasonable
--       time range.
DROP TABLE IF EXISTS "historical_entity_attrs";
CREATE TABLE "historical_entity_attrs" (
  -- The time this attribute was recorded.
  "time" timestamptz NOT NULL,

  -- The id of the entity the attribute pertains to.
  "entity_oid" bigint NOT NULL,

  -- The attribute type.
  "type" attr_type NOT NULL,

  -- If the attribute is boolean, this is the boolean value.
  "bool_value" boolean NULL,

  -- If the attribute is an integer, this is the integer value.
  -- If the attribute is an enum, this is the OID of the enum in the pg_enum table
  -- (to guard against name or order changes). The int_value should be castable to
  -- the enum via a custom cast (e.g. int_value::entity_state = 'POWERED_ON').
  "int_value" int NULL,

  -- If the attribute is a long, this is the long value.
  "long_value" bigint NULL,

  -- If the attribute is a double, this is the double value.
  "double_value" double precision NULL,

  -- If the attribute is a string, this is the string value.
  "string_value" text NULL,

  -- If the attribute is a list of ints, this is the value.
  -- If the attribute is a list of enums, these are the OIDs of the enums in the pg_enum table
  -- (to guard against name or order changes).
  "int_arr_value" int ARRAY NULL,

  -- If the attribute is a list of longs, this is the value.
  "long_arr_value" bigint ARRAY NULL,

  -- If the attribute is a list of strings, this is the value.
  "string_arr_value" text ARRAY NULL,

  -- If the attribute is a list of doubles, this is the value.
  "double_arr_value" double precision ARRAY NULL,

  -- If the attribute is a JSON object, this is the value.
  "json_value" jsonb NULL
);

CREATE UNIQUE INDEX "history_attrs_byOid" ON "historical_entity_attrs" USING btree ("entity_oid", "type", "time" DESC);
CREATE INDEX "history_attrs_byType" ON "historical_entity_attrs" USING btree ("type");

-- Make the completed actions table a hyper-table for easier time-based chunk management.
-- This table is not expected to be nearly as big as the metric table, so we don't use compression
-- and leave the default chunk interval (7 days).
SELECT create_hypertable('historical_entity_attrs', 'time');

-- remove retention policy if it exists
SELECT remove_retention_policy('historical_entity_attrs', if_exists => true);
-- add new retention policy, default to 12 months, and it only drop raw chunks, while keeping
-- data in the continuous aggregates
SELECT alter_job(
    -- add new policy which returns job id and use it as parameter of function alter_job_schedule
    add_retention_policy('historical_entity_attrs', INTERVAL '12 months'),
    -- set the drop_chunks background job to run every day (this is default interval)
    schedule_interval => INTERVAL '1 days',
    -- set the job to start from midnight of next day
    next_start => date_trunc('DAY', now()) + INTERVAL '1 days'
);
