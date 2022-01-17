
-- The account_expenses table contains the latest record for each expense_date, associated_entity_id,
-- and associated_account_id. We assume that given two records that represent the spent
-- on the same cloud service/compute tier in a specific account on the same date, the recent record
-- holds the correct values.
-- For this table, insertion of duplicate keys is handled in SqlAccountExpensesStore#persistAccountExpense
CREATE TABLE IF NOT EXISTS account_expenses (
    associated_account_id bigint NOT NULL,
    expense_date date NOT NULL,
    associated_entity_id bigint NOT NULL,
    entity_type int NOT NULL,
    currency int NOT NULL,
    amount numeric(20,7) NOT NULL,
    aggregated SMALLINT DEFAULT '0' NOT NULL,
    PRIMARY KEY (expense_date, associated_account_id, associated_entity_id)
);
CREATE INDEX IF NOT EXISTS ex_ai ON account_expenses (associated_account_id);

-- The account_expenses_by_month table contains the average expenses on a cloud service/compute tier
-- in a specific account within a month.
CREATE TABLE IF NOT EXISTS account_expenses_by_month (
    associated_account_id bigint NOT NULL,
    expense_date date NOT NULL,
    associated_entity_id bigint NOT NULL,
    entity_type int NOT NULL,
    currency int NOT NULL,
    amount numeric(20,7) NOT NULL,
    samples int NOT NULL,
    PRIMARY KEY (expense_date, associated_account_id, associated_entity_id)
);
CREATE INDEX IF NOT EXISTS exm_ai ON account_expenses_by_month (associated_account_id);

DROP TABLE IF EXISTS account_expenses_retention_policies;
CREATE TABLE account_expenses_retention_policies (
    policy_name VARCHAR(50) NOT NULL,
    retention_period int NOT NULL,
    PRIMARY KEY (policy_name)
);
INSERT INTO account_expenses_retention_policies VALUES ('retention_days',60),('retention_months',24);

-- create enum
DROP TYPE IF EXISTS ri_coverage;
CREATE TYPE ri_coverage AS ENUM ('BILLING','SUPPLEMENTAL_COVERAGE_ALLOCATION');

-- Introduce new table “account_to_reserved_instance_mapping”
-- snapshot_time – the last time this mapping was updated
-- business_account_id – Account Id
-- reserved_instance_id – Reserved Instance OID
-- used_coupons – number of coupons used ( daily avg as sent by billing probe)
-- ri_source_coverage – SUPPLEMENTAL expected only for discovered account
-- Mirror of the entity_to_reserved_instance_mapping table for current usage at an account granularity for undiscovered accounts.
-- Stores current usage at an account level for the undiscovered accounts populated with the contents of the
-- AccountRICoverageUpload data sent by TP for undiscovered accounts. This will have one row per account per RI.
-- to successfully drop type/enum, no table must be referencing it.
CREATE TABLE IF NOT EXISTS account_to_reserved_instance_mapping (
    snapshot_time timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    business_account_oid bigint NOT NULL,
    reserved_instance_id bigint NOT NULL,
    used_coupons double precision NOT NULL,
    ri_source_coverage ri_coverage NOT NULL,
    PRIMARY KEY (business_account_oid, reserved_instance_id, ri_source_coverage)
);

CREATE TABLE IF NOT EXISTS action_context_ri_buy (
    id serial NOT NULL,
    action_id bigint,
    plan_id bigint,
    create_time timestamp,
    template_type VARCHAR(100),
    template_family VARCHAR(100),
    data bytea,
    demand_type int NOT NULL,
    datapoint_interval VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

-- Aggregation Meta Data table creation */
-- This is a utility table used to store states of aggregation */
CREATE TABLE IF NOT EXISTS aggregation_meta_data (
    aggregate_table VARCHAR(64) NOT NULL,
    last_aggregated timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_aggregated_by_hour timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    last_aggregated_by_day timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    last_aggregated_by_month timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    PRIMARY KEY (aggregate_table)
);

-- Create billed_cost_daily table. This table stores billed cost items for billing reports discovered at a daily
-- granularity. This table will also contain records that are periodically rolled up from the hourly table.
CREATE TABLE IF NOT EXISTS billed_cost_daily (
    line_item_id bigserial NOT NULL,
    sample_time timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    entity_id bigint DEFAULT '0' NOT NULL,
    entity_type smallint DEFAULT '2047' NOT NULL,
    account_id bigint DEFAULT '0' NOT NULL,
    region_id bigint DEFAULT '0' NOT NULL,
    cloud_service_id bigint DEFAULT '0' NOT NULL,
    service_provider_id bigint NOT NULL,
    tag_group_id bigint DEFAULT '0' NOT NULL,
    price_model smallint NOT NULL,
    cost_category smallint NOT NULL,
    provider_id bigint DEFAULT '0' NOT NULL,
    provider_type smallint DEFAULT '2047' NOT NULL,
    commodity_type smallint DEFAULT '2047' NOT NULL,
    usage_amount double precision NOT NULL,
    unit smallint NOT NULL,
    currency smallint NOT NULL,
    cost double precision NOT NULL,
    UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type),
    PRIMARY KEY (line_item_id)
);

CREATE INDEX IF NOT EXISTS idx_billed_cost_daily_entity_id ON billed_cost_daily(entity_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_daily_account_id ON billed_cost_daily(account_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_daily_region_id ON billed_cost_daily(region_id);

-- Create billed_cost_hourly table. This table stores billed cost items for billing reports discovered at an hourly
-- granularity.
CREATE TABLE IF NOT EXISTS billed_cost_hourly (
    line_item_id bigserial NOT NULL,
    sample_time timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    entity_id bigint DEFAULT '0' NOT NULL,
    entity_type smallint DEFAULT '2047' NOT NULL,
    account_id bigint DEFAULT '0' NOT NULL,
    region_id bigint DEFAULT '0' NOT NULL,
    cloud_service_id bigint DEFAULT '0' NOT NULL,
    service_provider_id bigint NOT NULL,
    tag_group_id bigint DEFAULT '0' NOT NULL,
    price_model smallint NOT NULL,
    cost_category smallint NOT NULL,
    provider_id bigint DEFAULT '0' NOT NULL,
    provider_type smallint DEFAULT '2047' NOT NULL,
    commodity_type smallint DEFAULT '2047' NOT NULL,
    usage_amount double precision NOT NULL,
    unit smallint NOT NULL,
    currency smallint NOT NULL,
    cost double precision NOT NULL,
    UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type),
    PRIMARY KEY (line_item_id)
);

CREATE INDEX IF NOT EXISTS idx_billed_cost_hourly_entity_id ON billed_cost_hourly(entity_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_hourly_account_id ON billed_cost_hourly(account_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_hourly_region_id ON billed_cost_hourly(region_id);

-- Create billed_cost_monthly table. This table stores billed cost items for billing reports discovered at a monthly
-- granularity. This table will also contain records that are periodically rolled up from the daily table.
CREATE TABLE IF NOT EXISTS billed_cost_monthly (
    line_item_id bigserial NOT NULL,
    sample_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    entity_id bigint DEFAULT '0' NOT NULL,
    entity_type smallint DEFAULT '2047' NOT NULL,
    account_id bigint DEFAULT '0' NOT NULL,
    region_id bigint DEFAULT '0' NOT NULL,
    cloud_service_id bigint DEFAULT '0' NOT NULL,
    service_provider_id bigint NOT NULL,
    tag_group_id bigint DEFAULT '0' NOT NULL,
    price_model smallint NOT NULL,
    cost_category smallint NOT NULL,
    provider_id bigint DEFAULT '0' NOT NULL,
    provider_type smallint DEFAULT '2047' NOT NULL,
    commodity_type smallint DEFAULT '2047' NOT NULL,
    usage_amount double precision NOT NULL,
    unit smallint NOT NULL,
    currency smallint NOT NULL,
    cost double precision NOT NULL,
    UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type),
    PRIMARY KEY (line_item_id)
);

CREATE INDEX IF NOT EXISTS idx_billed_cost_monthly_entity_id ON billed_cost_monthly(entity_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_monthly_account_id ON billed_cost_monthly(account_id);
CREATE INDEX IF NOT EXISTS idx_billed_cost_monthly_region_id ON billed_cost_monthly(region_id);

-- persist OIDs for discovered price_table_key here.
CREATE TABLE IF NOT EXISTS price_table_key_oid (
  -- unique ID for this price_table_key
  id bigint NOT NULL,
  price_table_key text NOT NULL,
  -- the unique ID for this price table
  PRIMARY KEY (id)
);

-- persist mapping of business account and price table keys .
CREATE TABLE IF NOT EXISTS business_account_price_table_key (
    -- unique ID for this business account
    business_account_oid bigint NOT NULL,
    price_table_key_oid bigint NOT NULL,
    -- the unique business account oid.
    PRIMARY KEY (business_account_oid),
    FOREIGN KEY(price_table_key_oid) REFERENCES price_table_key_oid(id)
);

CREATE TABLE IF NOT EXISTS reserved_instance_spec (
    -- The id of reserved instance spec.
    id bigint NOT NULL,
     -- The type of reserved instance offering.
    offering_class integer NOT NULL,
    -- The payment option of the reserved instance.
    payment_option integer NOT NULL,
    -- The term of years of the reserved instance.
    term_years integer NOT NULL,
    -- The tenancy of the reserved instance.
    tenancy integer NOT NULL,
    -- The operating system of the reserved instance.
    os_type integer NOT NULL,
    -- The entity profile id of the reserved instance using, such as t2.large.
    tier_id bigint NOT NULL,
    -- The region id of the reserved instance.
    region_id bigint NOT NULL,
    -- The serialized protobuf object contains detailed information about the reserved instance spec.
    reserved_instance_spec_info bytea NOT NULL,
    PRIMARY KEY (id)
);

-- This table stores all buy RI recommendations for plans and for real time.
CREATE TABLE IF NOT EXISTS buy_reserved_instance (
 -- The id of reserved instance.
    id bigint NOT NULL,
    -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
    topology_context_id bigint NOT NULL,
    -- The business account id owns this reserved instance.
    business_account_id bigint NOT NULL,
    -- The region of the Buy RI
    region_id bigint NOT NULL,
    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id bigint NOT NULL,
    -- The number of instances to buy of the reserved instance
    count int NOT NULL,
    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info bytea NOT NULL,
    per_instance_fixed_cost double precision,
    per_instance_recurring_cost_hourly double precision,
    per_instance_amortized_cost_hourly double precision,
    PRIMARY KEY (id),
    -- The foreign key referring to reserved instance spec table.
    FOREIGN KEY(reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE
);

-- Add an index to make finding market actions by their action plan or topology context
CREATE INDEX IF NOT EXISTS buy_reserved_instance_topology_context_id_idx ON buy_reserved_instance (topology_context_id);

-- Table to store the compute tier demand stats data.
CREATE TABLE IF NOT EXISTS compute_tier_type_hourly_by_week (
    -- Hour of the day. Value range [0-23]
    hour smallint NOT NULL,
    -- Day of the week. Value range [1 (Sunday) - 7 (Saturday)]
    day smallint NOT NULL,
    -- The BusinessAccount OID of the master account this instance
    -- belongs to.
    account_id bigint NOT NULL,
    -- The OID of the Compute Tier entity consumed by the VMs.
    compute_tier_id bigint NOT NULL,
    -- The OID of the availability zone of this instance.
    region_or_zone_id bigint NOT NULL,
    -- Type of platform/OS. Store the enum value.
    platform smallint NOT NULL,
    -- The tenancy defines what hardware the instance is running on.
    -- Store the enum value.
    tenancy smallint NOT NULL,
    -- This is the weighted average of the histogram of the compute
    --  tier consumed by the VMs.
    -- This value is calculated from the live source topology.
    count_from_source_topology numeric(15,3) DEFAULT NULL,
    -- Same as above. But the histogram is calculated from the projected topology.
    count_from_projected_topology numeric(15,3) DEFAULT NULL,
    PRIMARY KEY (hour, day, account_id, compute_tier_id, region_or_zone_id, platform, tenancy)
);

-- Create cost_tag table. This table stores tags discovered from billing reports of cloud providers. For maximum
-- generality we support collation allowing case sensitivity and trailing spaces for both keys and values as at least
-- one cloud provider allows these properties.
CREATE TABLE IF NOT EXISTS cost_tag (
    tag_key VARCHAR(255) NOT NULL,
    tag_value VARCHAR(255) NOT NULL,
    tag_id bigserial NOT NULL,
    PRIMARY KEY (tag_id),
    UNIQUE (tag_key, tag_value)
);

-- Create cost_tag_grouping table. This table stores tag groups discovered from billing reports. A tag group is a set of
-- tags associated with one or more cloud resources.
CREATE TABLE IF NOT EXISTS cost_tag_grouping (
    tag_group_id bigint NOT NULL,
    tag_id bigint NOT NULL,
     PRIMARY KEY (tag_group_id, tag_id),
     FOREIGN KEY (tag_id) REFERENCES cost_tag(tag_id)
     ON DELETE NO ACTION
     ON UPDATE NO ACTION
);

CREATE TABLE IF NOT EXISTS daily_account_commitment_coverage (
    sample_date date NOT NULL,
    account_id bigint NOT NULL,
    region_id bigint DEFAULT '0' NOT NULL,
    cloud_service_id bigint DEFAULT '0' NOT NULL,
    service_provider_id bigint NOT NULL,
    covered_amount double precision NOT NULL,
    coverage_capacity double precision NOT NULL,
    -- Type will indicate coupons, spend, etc.
    coverage_type smallint NOT NULL,
    -- Subtype is applicable to only specific types e.g. spend, in which sub type
    -- represents the currency
    coverage_sub_type int DEFAULT '0' NOT NULL,
    PRIMARY KEY (sample_date, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX IF NOT EXISTS idx_daily_account_commitment_coverage_acct_id on daily_account_commitment_coverage
(account_id);
CREATE INDEX IF NOT EXISTS idx_daily_account_commitment_coverage_region_id on daily_account_commitment_coverage
(region_id);
CREATE INDEX IF NOT EXISTS idx_daily_account_commitment_coverage_svc_provider_id on
daily_account_commitment_coverage(service_provider_id);

CREATE TABLE IF NOT EXISTS daily_cloud_commitment_utilization (
    sample_date date NOT NULL,
    cloud_commitment_id bigint NOT NULL,
    -- The purchasing account of the cloud commitment
    account_id bigint NOT NULL,
    -- The region in which the cloud commitment was purchased
    region_id bigint DEFAULT '0' NOT NULL,
    service_provider_id bigint NOT NULL,
    utilization_amount double precision NOT NULL,
    commitment_capacity double precision NOT NULL,
    -- Type will indicate coupons, spend, etc.
    coverage_type smallint NOT NULL,
     -- Subtype is applicable to only specific types e.g. spend, in which sub type
     -- represents the currency
    coverage_sub_type int DEFAULT '0' NOT NULL,
    PRIMARY KEY (sample_date, cloud_commitment_id, coverage_type, coverage_sub_type)
);

CREATE INDEX IF NOT EXISTS idx_daily_cloud_commitment_utilization_cc_id ON daily_cloud_commitment_utilization
(cloud_commitment_id);
CREATE INDEX IF NOT EXISTS idx_daily_cloud_commitment_utilization_acct_id ON daily_cloud_commitment_utilization
(account_id);
CREATE INDEX IF NOT EXISTS idx_daily_cloud_commitment_utilization_region_id ON daily_cloud_commitment_utilization
(region_id);

CREATE TABLE IF NOT EXISTS discount (
    -- The id of discount.
    id bigint NOT NULL,
    -- The business account id owns this reserved instance.
    associated_account_id bigint NOT NULL,
    -- The serialized protobuf object contains detailed information about the discount.
    -- Each discount info could have account-level, service-levels, and tier-levels.
    discount_info bytea NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (associated_account_id)
);

CREATE TABLE IF NOT EXISTS entity_cloud_scope (
    entity_oid bigint NOT NULL,
    entity_type int NOT NULL,
    account_oid bigint NOT NULL,
    region_oid bigint NOT NULL,
    -- The availability zone OID may be null, given Azure allows workloads to be either regional
    -- or zonal resources.
    availability_zone_oid bigint,
    service_provider_oid bigint NOT NULL,
    resource_group_oid bigint,
    creation_time timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (entity_oid)
);

-- An index is created for each scope attribute which may reasonably be used as the primary filter
-- in querying entity data. While an index on each may be relatively expensive for a single table,
-- the storage cost of indexing is expected to be amortized over several FK'd child tables.
CREATE INDEX IF NOT EXISTS idx_entity_cloud_scope_account_oid ON entity_cloud_scope(account_oid);
CREATE INDEX IF NOT EXISTS idx_entity_cloud_scope_region_oid ON entity_cloud_scope(region_oid);
CREATE INDEX IF NOT EXISTS idx_entity_cloud_scope_availability_zone_oid ON entity_cloud_scope
(availability_zone_oid);
CREATE INDEX IF NOT EXISTS idx_entity_cloud_scope_service_provider_oid ON entity_cloud_scope
(service_provider_oid);
-- add search index for resource_group_oid column
CREATE INDEX IF NOT EXISTS idx_entity_cloud_scope_resource_group_oid ON entity_cloud_scope (resource_group_oid);

CREATE TABLE IF NOT EXISTS entity_compute_tier_allocation (
    start_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    end_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    entity_oid bigint NOT NULL,
    os_type int NOT NULL,
    tenancy int NOT NULL,
    allocated_compute_tier_oid bigint NOT NULL,
     -- entity_oid should be first in PK to allow easy querying through cloud scope
    PRIMARY KEY (entity_oid, start_time),
    FOREIGN KEY (entity_oid) REFERENCES entity_cloud_scope(entity_oid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS idx_entity_compute_tier_allocation_end_time ON entity_compute_tier_allocation
(end_time);

-- This table stores all entity costs information.
CREATE TABLE IF NOT EXISTS entity_cost (
    -- The associated entity id
    associated_entity_id bigint NOT NULL,
    -- The timestamp at which the entity cost is calculated in Cost component.
    -- The time is UTC.
    created_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    -- The associated entity type, e.g. vm_spend, app_spend
    associated_entity_type int NOT NULL,
    -- The cost type, e.g. LICENSE, IP, STORAGE, COMPUTE
    cost_type int NOT NULL,
    -- The currency code, default to USD
    currency int NOT NULL,
    -- The cost amount for a given cost_type.
    -- DECIMAL(20, 7): 20 is the precision and 7 is the scale.
    amount numeric(20,7) NOT NULL,
    cost_source int DEFAULT '4' NOT NULL,
    account_id bigint DEFAULT '0',
    region_id bigint DEFAULT '0',
    availability_zone_id bigint DEFAULT '0',
    PRIMARY KEY (associated_entity_id, created_time, cost_type, cost_source)
);

CREATE INDEX IF NOT EXISTS ec_ct ON entity_cost(created_time);
CREATE INDEX IF NOT EXISTS entity_cost_account_id_index ON entity_cost(account_id);
CREATE INDEX IF NOT EXISTS entity_cost_region_id_index ON entity_cost(region_id);
CREATE INDEX IF NOT EXISTS entity_cost_availability_zone_id_index ON entity_cost(availability_zone_id);

CREATE TABLE IF NOT EXISTS entity_cost_by_day (
    associated_entity_id bigint NOT NULL,
    created_time timestamp DEFAULT '0001-01-01 00:00:00.000',
    associated_entity_type int NOT NULL,
    cost_type int NOT NULL,
    currency int NOT NULL,
    amount numeric(20,7) NOT NULL,
    samples int,
    account_id bigint DEFAULT '0',
    region_id bigint DEFAULT '0',
    availability_zone_id bigint DEFAULT '0'
);

CREATE INDEX IF NOT EXISTS ecd_ct ON entity_cost_by_day(created_time);
CREATE INDEX IF NOT EXISTS entity_cost_by_day_account_id_index ON entity_cost_by_day(account_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_day_region_id_index ON entity_cost_by_day(region_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_day_availability_zone_id_index ON entity_cost_by_day(availability_zone_id);

CREATE TABLE IF NOT EXISTS entity_cost_by_hour (
    associated_entity_id bigint NOT NULL,
    created_time timestamp,
    associated_entity_type int NOT NULL,
    cost_type int NOT NULL,
    currency int NOT NULL,
    amount numeric(20,7) NOT NULL,
    samples int,
    account_id bigint DEFAULT '0',
    region_id bigint DEFAULT '0',
    availability_zone_id bigint DEFAULT '0'
);

CREATE INDEX IF NOT EXISTS ech_ct ON entity_cost_by_hour(created_time);
CREATE INDEX IF NOT EXISTS entity_cost_by_hour_account_id_index ON entity_cost_by_hour(account_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_hour_region_id_index ON entity_cost_by_hour(region_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_hour_availability_zone_id_index ON entity_cost_by_hour(availability_zone_id);

CREATE TABLE IF NOT EXISTS entity_cost_by_month (
    associated_entity_id bigint NOT NULL,
    created_time timestamp,
    associated_entity_type int NOT NULL,
    cost_type int NOT NULL,
    currency int NOT NULL,
    amount numeric(20,7) NOT NULL,
    samples int,
    account_id bigint DEFAULT '0',
    region_id bigint DEFAULT '0',
    availability_zone_id bigint DEFAULT '0'
);

CREATE INDEX IF NOT EXISTS ecm_ct ON entity_cost_by_month(created_time);
CREATE INDEX IF NOT EXISTS entity_cost_by_month_account_id_index ON entity_cost_by_month(account_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_month_region_id_index ON entity_cost_by_month(region_id);
CREATE INDEX IF NOT EXISTS entity_cost_by_month_availability_zone_id_index ON entity_cost_by_month(availability_zone_id);

CREATE TABLE IF NOT EXISTS entity_savings_audit_events (
  -- VM/DB oid whose state we are tracking now, or have ever tracked before, even deleted ones.
  entity_oid BIGINT NOT NULL,

  -- Type of entity event to track. Action recommendations/executions, topology events
  -- (creation/deletion/powerToggles etc.). Other possible events are group membership changes.
  event_type int NOT NULL,

  -- Action OID if action event type, or Vendor event id for topology events if applicable, or ''.
  event_id VARCHAR(255) NOT NULL,
  event_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',

  -- Additional info ('' if not applicable) about event where applicable, stored as a JSON string.
  event_info TEXT NOT NULL,

  PRIMARY KEY (entity_oid, event_type, event_id, event_time)
);
CREATE INDEX idx_entity_savings_audit_events_event_time ON entity_savings_audit_events(event_time);

CREATE TABLE IF NOT EXISTS entity_savings_by_hour (
    entity_oid bigint NOT NULL,
    stats_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    stats_type int NOT NULL,
    stats_value double precision NOT NULL,
    PRIMARY KEY (entity_oid, stats_time, stats_type)
);
CREATE INDEX idx_entity_savings_by_hour_stats_time ON entity_savings_by_hour(stats_time);

CREATE TABLE IF NOT EXISTS entity_savings_by_day (
    entity_oid bigint NOT NULL,
    stats_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    stats_type int NOT NULL,
    stats_value double precision NOT NULL,
    samples int NOT NULL,
    PRIMARY KEY (entity_oid, stats_time, stats_type)
);
CREATE INDEX idx_entity_savings_by_day_stats_time ON entity_savings_by_day(stats_time);

CREATE TABLE IF NOT EXISTS entity_savings_by_month (
    entity_oid bigint NOT NULL,
    stats_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    stats_type int NOT NULL,
    stats_value double precision NOT NULL,
    samples int NOT NULL,
    PRIMARY KEY (entity_oid, stats_time, stats_type)
);
CREATE INDEX idx_entity_savings_by_month_stats_time ON entity_savings_by_month(stats_time);

-- Create entity_savings_state table for storing states used for savings calculations.
CREATE TABLE IF NOT EXISTS entity_savings_state (
    entity_oid BIGINT NOT NULL,
    updated SMALLINT NOT NULL DEFAULT 0,
    entity_state TEXT DEFAULT NULL,
    next_expiration_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    CONSTRAINT fk_entity_savings_state_entity_oid FOREIGN KEY (entity_oid) REFERENCES
    entity_cloud_scope (entity_oid) ON DELETE NO ACTION ON UPDATE NO ACTION,
    PRIMARY KEY (entity_oid)
);
-- Records will be searched by the updated flag. Hence add an index for this column.
CREATE INDEX idx_updated ON entity_savings_state (updated);

-- This table store the entity with reserved instance coverage mapping, and this data only comes from
-- billing topology.
CREATE TABLE IF NOT EXISTS entity_to_reserved_instance_mapping (
    -- The time of last update
    snapshot_time                      TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The id of entity.
    entity_id                          BIGINT           NOT NULL,

    -- The id of reserved instance.
    reserved_instance_id               BIGINT           NOT NULL,

    -- The used coupons amount of the entity covered by the reserved instance.
    used_coupons                       DOUBLE PRECISION            NOT NULL,

    ri_source_coverage ri_coverage NOT NULL,

    PRIMARY KEY (entity_id, reserved_instance_id, ri_source_coverage)
);

-- Introduce new table “hist_entity_reserved_instance_mapping”
-- snapshot_time – the last time this mapping was updated
-- entity_id – Entity Id
-- reserved_instance_id – Reserved Instance OID
-- used_coupons – number of coupons used ( daily avg as sent by billing probe)
-- start_usage_time  - the billing window start time
-- end_usage_time – the billing window end time
-- Stores all usage of discovered entities of any RI.
-- Populated from the data coming in from the EntityRICoverageUpload with the usage start and usage end.
-- Each row is identified by the entity id, RI id and the usage start. Each entity may have multiple historical
-- RI coverage periods (same or different RI) and each such record is uniquely determined by entity id, reserved
-- instance id and the start time of the coverage period.
CREATE TABLE IF NOT EXISTS hist_entity_reserved_instance_mapping (
  snapshot_time         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  entity_id             BIGINT NOT NULL,
  reserved_instance_id  BIGINT NOT NULL,
  start_usage_time      TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
  end_usage_time        TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
  PRIMARY KEY (entity_id, reserved_instance_id, start_usage_time)
);

CREATE TABLE IF NOT EXISTS hourly_account_commitment_coverage (
   sample_time                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
   account_id                 BIGINT          NOT NULL,
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   cloud_service_id           BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   covered_amount             DOUBLE PRECISION         NOT NULL,
   coverage_capacity          DOUBLE PRECISION         NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              SMALLINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_time, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_hourly_account_commitment_coverage_acct_id on hourly_account_commitment_coverage(account_id);
CREATE INDEX idx_hourly_account_commitment_coverage_region_id on hourly_account_commitment_coverage(region_id);
CREATE INDEX idx_hourly_account_commitment_coverage_svc_provider_id on hourly_account_commitment_coverage(service_provider_id);

CREATE TABLE IF NOT EXISTS hourly_cloud_commitment_utilization (
   sample_time                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
   cloud_commitment_id        BIGINT          NOT NULL,
   -- The purchasing account of the cloud commitment
   account_id                 BIGINT          NOT NULL,
   -- The region in which the cloud commitment was purchased
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   utilization_amount         DOUBLE PRECISION           NOT NULL,
   commitment_capacity        DOUBLE PRECISION           NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              SMALLINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_time, cloud_commitment_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_hourly_cloud_commitment_utilization_cc_id ON hourly_cloud_commitment_utilization(cloud_commitment_id);
CREATE INDEX idx_hourly_cloud_commitment_utilization_acct_id on hourly_cloud_commitment_utilization(account_id);
CREATE INDEX idx_hourly_cloud_commitment_utilization_region_id on hourly_cloud_commitment_utilization(region_id);

CREATE TABLE IF NOT EXISTS ingested_live_topology (
  topology_id BIGINT NOT NULL,
  creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (topology_id)
);

CREATE INDEX idx_ingested_live_topology_creation_time ON ingested_live_topology(creation_time);

CREATE TABLE IF NOT EXISTS last_updated (
  table_name varchar(50) NOT NULL,
  column_name varchar(50) NOT NULL,
  last_update timestamp NULL DEFAULT NULL,
  PRIMARY KEY (table_name,column_name)
);

CREATE TABLE IF NOT EXISTS monthly_account_commitment_coverage (
   sample_date                DATE            NOT NULL,
   account_id                 BIGINT          NOT NULL,
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   cloud_service_id           BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   covered_amount             DOUBLE PRECISION          NOT NULL,
   coverage_capacity          DOUBLE PRECISION          NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              smallint         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_date, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_monthly_account_commitment_coverage_acct_id on monthly_account_commitment_coverage(account_id);
CREATE INDEX idx_monthly_account_commitment_coverage_region_id on monthly_account_commitment_coverage(region_id);
CREATE INDEX idx_monthly_account_commitment_coverage_svc_provider_id on monthly_account_commitment_coverage(service_provider_id);

CREATE TABLE IF NOT EXISTS monthly_cloud_commitment_utilization (
   sample_date                DATE            NOT NULL,
   cloud_commitment_id        BIGINT          NOT NULL,
   -- The purchasing account of the cloud commitment
   account_id                 BIGINT          NOT NULL,
   -- The region in which the cloud commitment was purchased
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   utilization_amount         DOUBLE PRECISION          NOT NULL,
   commitment_capacity          DOUBLE PRECISION          NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              smallint         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_date, cloud_commitment_id, coverage_type, coverage_sub_type)
);
CREATE INDEX IF NOT EXISTS idx_monthly_cloud_commitment_utilization_cc_id ON
monthly_cloud_commitment_utilization(cloud_commitment_id);
CREATE INDEX IF NOT EXISTS idx_monthly_cloud_commitment_utilization_acct_id ON
monthly_cloud_commitment_utilization(account_id);
CREATE INDEX IF NOT EXISTS idx_monthly_cloud_commitment_utilization_region_id ON
monthly_cloud_commitment_utilization(region_id);


CREATE TABLE IF NOT EXISTS plan_entity_cost (
    associated_entity_id bigint NOT NULL,
    created_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    associated_entity_type int NOT NULL,
    cost_type int NOT NULL,
    currency int NOT NULL,
    -- The cost amount for a given cost_type.
    -- DECIMAL(20, 7): 20 is the precision and 7 is the scale.
    amount                            DECIMAL(20, 7)  NOT NULL,
    cost_source                       int         NOT NULL DEFAULT '4',
    account_id                        BIGINT      DEFAULT '0',
    region_id                         BIGINT      DEFAULT '0',
    availability_zone_id              BIGINT      DEFAULT '0',
    -- The id of plan topology context.
    plan_id                           BIGINT          NOT NULL,

    PRIMARY KEY (associated_entity_id, plan_id, cost_type, cost_source)
);
CREATE INDEX IF NOT EXISTS ec_pid ON plan_entity_cost(plan_id);


CREATE TABLE IF NOT EXISTS plan_projected_entity_cost (
     -- The id of plan topology context.
     plan_id                         BIGINT           NOT NULL,

     -- The associated entity id
     associated_entity_id            BIGINT          NOT NULL,

     entity_cost                     bytea             NOT NULL,

     PRIMARY KEY (plan_id, associated_entity_id)
);

-- This table store projected entity to reserved instance mapping
CREATE TABLE IF NOT EXISTS plan_projected_entity_to_reserved_instance_mapping (

    -- The id of entity.
    entity_id                          BIGINT           NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

    -- The id of reserved instance.
    reserved_instance_id               BIGINT           NOT NULL,

    -- The used coupons amount of the entity covered by the reserved instance.
    used_coupons                       DOUBLE PRECISION            NOT NULL,

    PRIMARY KEY (entity_id, plan_id, reserved_instance_id)
);

-- This table store the projected coverage information of reserved instance, which means for each entity,
-- what is the total coupons it has, and what is the amount of coupons covered by reserved instance.
CREATE TABLE IF NOT EXISTS plan_projected_reserved_instance_coverage (

    -- The Id of the entity.
    entity_id                          BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

    -- The region id of the entity.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of the entity.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of the entity.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the entity has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which covered by reserved instance.
    used_coupons                       DOUBLE PRECISION           NOT NULL,

    PRIMARY KEY (entity_id, plan_id)
);

-- This table store the projected utilization of reserved instance, which means for each reserved instance
-- what is the total coupons it has, and what is the amount of coupons which used by other entities.
CREATE TABLE IF NOT EXISTS plan_projected_reserved_instance_utilization (

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       DOUBLE PRECISION           NOT NULL,

    PRIMARY KEY (id, plan_id)
);

-- This table stores the cloud price tables discovered by different probes.
-- For more information about price tables see com.vmturbo.common.protobuf/cost/Pricing.proto
CREATE TABLE IF NOT EXISTS price_table (
    -- The time of the last update of the price table. This should generally be the time of the last
    -- topology broadcast. The time is UTC.
    last_update_time                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- A serialized PriceTable protobuf message containing the actual price data.
    price_table_data                  bytea            NOT NULL,

    -- A serialized ReservedInstancePriceTable protobuf message containing the price data
    -- for reserved instances.
    ri_price_table_data               bytea            NOT NULL,

    oid                               bigint           NOT NULL,

    price_table_key                   text             DEFAULT NULL,

    checksum                          bigint           DEFAULT NULL,

    CONSTRAINT price_table_ibfk FOREIGN  KEY (oid) REFERENCES price_table_key_oid(id) ON DELETE CASCADE,

    PRIMARY KEY (oid)

);

-- This table store the utilization of reserved instance, which means for each reserved instance
-- what is the total coupons it has, and what is the amount of coupons which used by other entities.
CREATE TABLE IF NOT EXISTS reserved_instance_utilization_latest (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       DOUBLE PRECISION          NOT NULL,

    hour_key                           VARCHAR(32),
    day_key                            VARCHAR(32),
    month_key                          VARCHAR(32),
    PRIMARY KEY (id, snapshot_time)
);
CREATE INDEX IF NOT EXISTS riul_st ON reserved_instance_utilization_latest(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_utilization_by_hour (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       DOUBLE PRECISION          NOT NULL,

    hour_key                           VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (hour_key)
);
CREATE INDEX IF NOT EXISTS riuh_idst ON reserved_instance_utilization_by_hour(id,
snapshot_time);
CREATE INDEX IF NOT EXISTS riubh_st ON reserved_instance_utilization_by_hour(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_utilization_by_day (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       DOUBLE PRECISION          NOT NULL,

    day_key                            VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (day_key)
);
CREATE INDEX IF NOT EXISTS riud_idst ON reserved_instance_utilization_by_day(id,
snapshot_time);
CREATE INDEX IF NOT EXISTS riubd_st ON reserved_instance_utilization_by_day(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_utilization_by_month (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      DOUBLE PRECISION           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       DOUBLE PRECISION          NOT NULL,

    month_key                            VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (month_key)
);
CREATE INDEX IF NOT EXISTS rium_idst ON reserved_instance_utilization_by_month(id,
snapshot_time);
CREATE INDEX IF NOT EXISTS riubm_st ON reserved_instance_utilization_by_month(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_bought (
    -- The id of reserved instance.
    id bigint NOT NULL,
    -- The business account id owns this reserved instance.
    business_account_id bigint NOT NULL,
    -- The probe send out unique id for the reserved instance.
    probe_reserved_instance_id VARCHAR(255) NOT NULL,
    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id bigint NOT NULL,
    -- The availability zone id of reserved instance.
    availability_zone_id bigint NOT NULL,
    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info bytea NOT NULL,

    count int NOT NULL,
    per_instance_fixed_cost double precision,
    per_instance_recurring_cost_hourly double precision,
    per_instance_amortized_cost_hourly double precision,
    start_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    expiry_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    discovery_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00',
    FOREIGN  KEY (reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS reserved_instance_coverage_latest (
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      DOUBLE PRECISION           NOT NULL,
    used_coupons                       DOUBLE PRECISION           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    PRIMARY KEY (entity_id, snapshot_time)
);
CREATE INDEX IF NOT EXISTS ricl_st ON reserved_instance_coverage_latest(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_coverage_by_hour (
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      DOUBLE PRECISION           NOT NULL,
    used_coupons                       DOUBLE PRECISION           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (hour_key)
);
CREATE INDEX IF NOT EXISTS rich_eidst ON reserved_instance_coverage_by_hour(entity_id,
snapshot_time);
CREATE INDEX IF NOT EXISTS ricbh_st ON reserved_instance_coverage_by_hour(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_coverage_by_day (
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT '0001-01-01 00:00:00',
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      DOUBLE PRECISION           NOT NULL,
    used_coupons                       DOUBLE PRECISION           NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (day_key)
);
CREATE INDEX IF NOT EXISTS ricd_eidst ON reserved_instance_coverage_by_day(entity_id,
snapshot_time);
CREATE INDEX IF NOT EXISTS ricbd_st ON reserved_instance_coverage_by_day(snapshot_time);

CREATE TABLE IF NOT EXISTS reserved_instance_coverage_by_month (
    snapshot_time                      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      DOUBLE PRECISION           NOT NULL,
    used_coupons                       DOUBLE PRECISION           NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            int             NOT NULL,
    PRIMARY KEY (month_key)
);
CREATE INDEX IF NOT EXISTS ricm_eidst ON reserved_instance_coverage_by_month(entity_id,
snapshot_time);
CREATE INDEX IF NOT EXISTS ricbm_st ON reserved_instance_coverage_by_month(snapshot_time);

-- This table stores all bought reserved instance information for the plan.
CREATE TABLE IF NOT EXISTS plan_reserved_instance_bought (
    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT          NOT NULL,

    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id          BIGINT          NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info      bytea            NOT NULL,

    -- Add one count column to reserved instance bought table, it means the number of instances bought
    -- of the reserved instance
    count                              INT             NOT NULL,

    per_instance_fixed_cost DOUBLE PRECISION DEFAULT NULL,
    per_instance_recurring_cost_hourly DOUBLE PRECISION DEFAULT NULL,
    per_instance_amortized_cost_hourly DOUBLE PRECISION DEFAULT NULL,

-- The foreign key referring to reserved instance spec table.
    CONSTRAINT plan_reserved_instance_bought_ibfk FOREIGN KEY (reserved_instance_spec_id) REFERENCES
     reserved_instance_spec (id) ON DELETE CASCADE,

    PRIMARY KEY (id, plan_id)
);
CREATE INDEX IF NOT EXISTS reserved_instance_spec_id ON plan_reserved_instance_bought
(reserved_instance_spec_id);
