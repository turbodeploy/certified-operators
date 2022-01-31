-- Initial baseline Postgres migration for history component, based on current cumulative
-- impact of existing MariaDB migrations

-- Create a SP to generate entity stats tables. This will be retained for use when creating new
-- entity stats tables in the future, in order to promote consistency across entity types.
CREATE OR REPLACE PROCEDURE build_entity_stats_tables(prefix text)
AS $$
BEGIN
  -- create latest table and indexes
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_latest ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) NOT NULL, producer_uuid varchar(20),'
    'property_type varchar(80), property_subtype varchar(36),'
    'relation smallint, commodity_key varchar(80),'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float,'
    'hour_key char(32))', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_snapshot_time_idx '
    'ON %1$I_stats_latest(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_uuid_idx '
    'ON %1$I_stats_latest(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_property_name_idx '
    'ON %1$I_stats_latest(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_property_subtype_idx '
    'ON %1$I_stats_latest(property_subtype)', prefix);
  -- create hourly rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_hour ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) NOT NULL, producer_uuid varchar(20),'
    'property_type varchar(80), property_subtype varchar(36),'
    'relation smallint, commodity_key varchar(80),'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'hour_key char(32) NOT NULL,'
    'PRIMARY KEY(hour_key, snapshot_time))', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_snapshot_time_idx '
    'ON %1$I_stats_by_hour(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_uuid_idx '
    'ON %1$I_stats_by_hour(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_property_name_idx '
    'ON %1$I_stats_by_hour(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_property_subtype_idx '
    'ON %1$I_stats_by_hour(property_subtype)', prefix);
  -- create daily rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_day ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) NOT NULL, producer_uuid varchar(20),'
    'property_type varchar(80), property_subtype varchar(36),'
    'relation smallint, commodity_key varchar(80),'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'day_key char(32),'
    'PRIMARY KEY(day_key, snapshot_time))', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_snapshot_time_idx '
    'ON %1$I_stats_by_day(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_uuid_idx '
    'ON %1$I_stats_by_day(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_property_name_idx '
    'ON %1$I_stats_by_day(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_property_subtype_idx '
    'ON %1$I_stats_by_day(property_subtype)', prefix);
  -- create monthly rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_month ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) NOT NULL, producer_uuid varchar(20),'
    'property_type varchar(80), property_subtype varchar(36),'
    'relation smallint, commodity_key varchar(80),'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'month_key char(32),'
    'PRIMARY KEY(month_key, snapshot_time))', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_snapshot_time_idx '
    'ON %1$I_stats_by_month(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_uuid_idx '
    'ON %1$I_stats_by_month(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_property_name_idx '
    'ON %1$I_stats_by_month(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_property_subtype_idx '
    'ON %1$I_stats_by_month(property_subtype)', prefix);
END;
$$ LANGUAGE plpgsql;

-- generate current collection of entity stats tables; future migrations can make individual
-- calls to the build function for new entitity types, but for the initial set we'll be a bit
-- more concise.
DO $$ DECLARE
  all_names text[] := ARRAY[
    'app_component', 'app_server', 'app', 'bu', 'business_app', 'business_transaction', 'ch',
    'cnt_spec', 'cnt', 'container_cluster', 'cpod', 'da', 'db_server', 'db',
    'desktop_pool', 'dpod', 'ds', 'iom', 'load_balancer', 'lp', 'nspace', 'pm', 'sc','service',
    'sw', 'vdc', 'view_pod', 'virtual_app', 'virtual_volume', 'vm', 'vpod', 'wkld_ctl'
  ];
  name text;
BEGIN
  FOREACH name IN ARRAY all_names LOOP
     CALL build_entity_stats_tables(name);
  END LOOP;
END; $$;

-- now create the rest of the tables in the schmea, with associated primary keys, indexes,
-- data, and foreign-key constraints. Latter must appear after both related tables have been
-- created.
CREATE TABLE aggregation_meta_data (
    aggregate_table character varying(64) NOT NULL,
    last_aggregated timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_aggregated_by_hour timestamp with time zone,
    last_aggregated_by_day timestamp with time zone,
    last_aggregated_by_month timestamp with time zone,
    PRIMARY KEY (aggregate_table)
);

CREATE TABLE appl_performance (
    log_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id character varying(32) DEFAULT NULL::character varying,
    performance_type character varying(64) DEFAULT NULL::character varying,
    entity_type character varying(64) DEFAULT NULL::character varying,
    rows_aggregated bigint,
    start_time timestamp with time zone,
    end_time timestamp with time zone,
    runtime_seconds bigint
);

CREATE TABLE audit_log_entries (
    id bigserial,
    snapshot_time timestamp with time zone,
    action_name character varying(80) DEFAULT NULL::character varying,
    category character varying(80) DEFAULT NULL::character varying,
    user_name character varying(80) DEFAULT NULL::character varying,
    target_object_class character varying(80) DEFAULT NULL::character varying,
    target_object_name character varying(250) DEFAULT NULL::character varying,
    target_object_uuid varchar(20) DEFAULT NULL,
    source_class character varying(80) DEFAULT NULL::character varying,
    source_name character varying(250) DEFAULT NULL::character varying,
    source_uuid varchar(20) DEFAULT NULL,
    destination_class character varying(80) DEFAULT NULL::character varying,
    destination_name character varying(250) DEFAULT NULL::character varying,
    destination_uuid varchar(20) DEFAULT NULL,
    details text,
    PRIMARY KEY (id)
);
CREATE INDEX audit_log_entries_action_name ON audit_log_entries USING btree (action_name);
CREATE INDEX audit_log_entries_category ON audit_log_entries USING btree (category);
CREATE INDEX audit_log_entries_destination_uuid ON audit_log_entries USING btree (destination_uuid);
CREATE INDEX audit_log_entries_snapshot_time ON audit_log_entries USING btree (snapshot_time);
CREATE INDEX audit_log_entries_source_uuid ON audit_log_entries USING btree (source_uuid);
CREATE INDEX audit_log_entries_target_object_uuid ON audit_log_entries USING btree (target_object_uuid);

CREATE TABLE audit_log_retention_policies (
    policy_name character varying(50) NOT NULL,
    retention_period bigint,
    PRIMARY KEY (policy_name)
);
COPY audit_log_retention_policies (policy_name, retention_period) FROM stdin;
retention_days	365
\.

CREATE TABLE available_timestamps (
    history_variety character varying(20) NOT NULL,
    time_frame character varying(10) NOT NULL,
    time_stamp timestamp with time zone NOT NULL,
    expires_at timestamp with time zone,
    PRIMARY KEY (history_variety, time_frame, time_stamp)
);

CREATE TABLE cluster_stats_latest (
    recorded_on timestamp with time zone NOT NULL,
    internal_name character varying(250) NOT NULL,
    property_type character varying(36) NOT NULL,
    property_subtype character varying(36) NOT NULL,
    value double precision NOT NULL,
    PRIMARY KEY (recorded_on, internal_name, property_type, property_subtype)
);

CREATE TABLE cluster_stats_by_hour (
    recorded_on timestamp with time zone NOT NULL,
    internal_name character varying(250) NOT NULL,
    property_type character varying(36) NOT NULL,
    property_subtype character varying(36) NOT NULL,
    value double precision NOT NULL,
    samples bigint,
    PRIMARY KEY (recorded_on, internal_name, property_type, property_subtype)
);
CREATE INDEX cluster_stats_by_hour_internal_name ON cluster_stats_by_hour USING btree (internal_name);
CREATE INDEX cluster_stats_by_hour_property_subtype ON cluster_stats_by_hour USING btree (property_subtype);
CREATE INDEX cluster_stats_by_hour_property_type ON cluster_stats_by_hour USING btree (property_type);
CREATE INDEX cluster_stats_by_hour_recorded_on ON cluster_stats_by_hour USING btree (recorded_on);

CREATE TABLE cluster_stats_by_day (
    recorded_on timestamp with time zone NOT NULL,
    internal_name character varying(250) NOT NULL,
    property_type character varying(36) NOT NULL,
    property_subtype character varying(36) NOT NULL,
    value double precision NOT NULL,
    samples bigint,
    PRIMARY KEY (recorded_on, internal_name, property_type, property_subtype)
);
CREATE INDEX cluster_stats_by_day_internal_name ON cluster_stats_by_day USING btree (internal_name);
CREATE INDEX cluster_stats_by_day_property_subtype ON cluster_stats_by_day USING btree (property_subtype);
CREATE INDEX cluster_stats_by_day_property_type ON cluster_stats_by_day USING btree (property_type);
CREATE INDEX cluster_stats_by_day_recorded_on ON cluster_stats_by_day USING btree (recorded_on);

CREATE TABLE cluster_stats_by_month (
    recorded_on timestamp with time zone NOT NULL,
    internal_name character varying(250) NOT NULL,
    property_type character varying(36) NOT NULL,
    property_subtype character varying(36) NOT NULL,
    value double precision NOT NULL,
    samples bigint,
    PRIMARY KEY (recorded_on, internal_name, property_type, property_subtype)
);
CREATE INDEX cluster_stats_by_month_internal_name ON cluster_stats_by_month USING btree (internal_name);
CREATE INDEX cluster_stats_by_month_property_subtype ON cluster_stats_by_month USING btree (property_subtype);
CREATE INDEX cluster_stats_by_month_property_type ON cluster_stats_by_month USING btree (property_type);
CREATE INDEX cluster_stats_by_month_recorded_on ON cluster_stats_by_month USING btree (recorded_on);

CREATE TABLE entities (
    id bigint NOT NULL,
    name character varying(250) DEFAULT NULL::character varying,
    display_name character varying(250) DEFAULT NULL::character varying,
    uuid varchar(80) DEFAULT NULL,
    creation_class character varying(80) DEFAULT NULL::character varying,
    created_at timestamp with time zone,
    PRIMARY KEY (id)
);
COPY entities (id, name, display_name, uuid, creation_class, created_at) FROM stdin;
1	MarketSettingsManager	MarketSettingsManager	_1wq9QTUoEempSo2vlSygbA	MarketSettingsManager	\N
2	PresentationManager	PresentationManager	_zfrLATUoEempSo2vlSygbA	PresentationManager	\N
\.
CREATE INDEX entities_creation_class ON entities USING btree (creation_class);
CREATE INDEX entities_uuid ON entities USING btree (uuid);

CREATE TABLE entity_attrs (
    id bigserial,
    name character varying(80) DEFAULT NULL::character varying,
    value character varying(1000) DEFAULT NULL::character varying,
    entity_entity_id bigint,
    PRIMARY KEY (id)
);
COPY entity_attrs (id, name, value, entity_entity_id) FROM stdin;
1	utilThreshold_SA	90.0	1
2	utilThreshold_IOPS	100.0	1
3	utilThreshold_CPU	100.0	1
4	utilThreshold_MEM	100.0	1
5	utilThreshold_IO	50.0	1
6	utilThreshold_NW	50.0	1
7	utilThreshold_NW_Switch	70.0	1
8	utilThreshold_NW_Network	100.0	1
9	utilThreshold_NW_internet	100.0	1
10	utilThreshold_SW	20.0	1
11	utilThreshold_CPU_SC	100.0	1
12	utilThreshold_LT	100.0	1
13	utilThreshold_RQ	50.0	1
14	utilUpperBound_VMEM	85.0	1
15	utilUpperBound_VCPU	85.0	1
16	utilLowerBound_VMEM	10.0	1
17	utilLowerBound_VCPU	10.0	1
18	utilUpperBound_VStorage	85.0	1
19	utilLowerBound_VStorage	10.0	1
20	usedIncrement_VMEM	1024.0	1
21	usedIncrement_VCPU	1800.0	1
22	usedIncrement_VStorage	999999.0	1
23	usedIncrement_VDCMem	1024.0	1
24	usedIncrement_VDCCPU	1800.0	1
25	usedIncrement_VDCStorage	1.0	1
26	historicalTimeRange	8	1
27	cost_PhysicalMachine	9000.0	1
28	cost_VCPU	200.0	1
29	cost_VMem	50.0	1
30	cost_VStorage	50.0	1
31	usedIncrement_StAmt	100.0	1
32	utilTarget	70.0	1
33	targetBand	10.0	1
34	peaK_WEIGHT	99.0	1
35	useD_WEIGHT	50.0	1
36	ratE_OF_RESIZE	2.0	1
37	pM_MAX_PRICE	10000.0	1
38	sT_MAX_PRICE	10000.0	1
39	capacity_CPUProvisioned	1000.0	1
40	capacity_MemProvisioned	1000.0	1
41	disableDas	false	1
42	vmDeletionSensitivity	5	1
43	currencySetting	$	2
\.
CREATE INDEX entity_attrs_entity_entity_id ON entity_attrs USING btree (entity_entity_id);
CREATE INDEX entity_attrs_name ON entity_attrs USING btree (name, entity_entity_id);
ALTER TABLE ONLY entity_attrs
    ADD CONSTRAINT entity_attrs_ibfk_1 FOREIGN KEY (entity_entity_id) REFERENCES entities(id) ON UPDATE RESTRICT ON DELETE CASCADE;

CREATE TABLE hist_utilization (
    oid bigint NOT NULL,
    producer_oid bigint NOT NULL,
    property_type_id bigint NOT NULL,
    property_subtype_id bigint NOT NULL,
    commodity_key character varying(80) NOT NULL,
    value_type bigint NOT NULL,
    property_slot bigint NOT NULL,
    utilization numeric(15,3) DEFAULT NULL::numeric,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    PRIMARY KEY (oid, producer_oid, property_type_id, property_subtype_id, value_type, property_slot, commodity_key)
);

CREATE TABLE ingestion_status (
    snapshot_time bigint NOT NULL,
    status character varying(10000) DEFAULT NULL::character varying,
    PRIMARY KEY (snapshot_time)
);

CREATE TABLE market_stats_latest (
    snapshot_time timestamp with time zone,
    time_series_key char(32),
    topology_context_id bigint,
    topology_id bigint,
    entity_type character varying(80) DEFAULT NULL::character varying,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric,
    environment_type smallint DEFAULT '1'::smallint NOT NULL,
    PRIMARY KEY (snapshot_time, time_series_key)
);
CREATE INDEX market_stats_latest_market_latest_idx ON market_stats_latest USING btree (snapshot_time, topology_context_id, entity_type, property_type, property_subtype, relation);
CREATE INDEX market_stats_latest_property_subtype ON market_stats_latest USING btree (property_subtype);
CREATE INDEX market_stats_latest_property_type ON market_stats_latest USING btree (property_type);
CREATE INDEX market_stats_latest_snapshot_time ON market_stats_latest USING btree (snapshot_time);
CREATE INDEX market_stats_latest_topology_context_id ON market_stats_latest USING btree (topology_context_id);
CREATE INDEX market_stats_latest_topology_id ON market_stats_latest USING btree (topology_id);

CREATE TABLE market_stats_by_hour (
    snapshot_time timestamp with time zone,
    time_series_key char(32),
    topology_context_id bigint,
    entity_type character varying(80) DEFAULT NULL::character varying,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    samples bigint,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric,
    environment_type smallint DEFAULT '1'::smallint NOT NULL,
    PRIMARY KEY (snapshot_time, time_series_key)
);
CREATE INDEX market_stats_by_hour_market_stats_by_hour_idx ON market_stats_by_hour USING btree (snapshot_time, topology_context_id, entity_type, property_type, property_subtype, relation);
CREATE INDEX market_stats_by_hour_property_subtype ON market_stats_by_hour USING btree (property_subtype);
CREATE INDEX market_stats_by_hour_property_type ON market_stats_by_hour USING btree (property_type);
CREATE INDEX market_stats_by_hour_snapshot_time ON market_stats_by_hour USING btree (snapshot_time);
CREATE INDEX market_stats_by_hour_topology_context_id ON market_stats_by_hour USING btree (topology_context_id);

CREATE TABLE market_stats_by_day (
    snapshot_time timestamp with time zone,
    time_series_key char(32),
    topology_context_id bigint,
    entity_type character varying(80) DEFAULT NULL::character varying,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    samples bigint,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric,
    environment_type smallint DEFAULT '1'::smallint NOT NULL,
    PRIMARY KEY (snapshot_time, time_series_key)
);
CREATE INDEX market_stats_by_day_market_stats_by_hour_idx ON market_stats_by_day USING btree (snapshot_time, topology_context_id, entity_type, property_type, property_subtype, relation);
CREATE INDEX market_stats_by_day_property_subtype ON market_stats_by_day USING btree (property_subtype);
CREATE INDEX market_stats_by_day_property_type ON market_stats_by_day USING btree (property_type);
CREATE INDEX market_stats_by_day_snapshot_time ON market_stats_by_day USING btree (snapshot_time);
CREATE INDEX market_stats_by_day_topology_context_id ON market_stats_by_day USING btree (topology_context_id);

CREATE TABLE market_stats_by_month (
    snapshot_time timestamp with time zone,
    time_series_key char(32),
    topology_context_id bigint,
    entity_type character varying(80) DEFAULT NULL::character varying,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    samples bigint,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric,
    environment_type smallint DEFAULT '1'::smallint NOT NULL,
    PRIMARY KEY (snapshot_time, time_series_key)
);
CREATE INDEX market_stats_by_month_market_stats_by_hour_idx ON market_stats_by_month USING btree (snapshot_time, topology_context_id, entity_type, property_type, property_subtype, relation);
CREATE INDEX market_stats_by_month_property_subtype ON market_stats_by_month USING btree (property_subtype);
CREATE INDEX market_stats_by_month_property_type ON market_stats_by_month USING btree (property_type);
CREATE INDEX market_stats_by_month_snapshot_time ON market_stats_by_month USING btree (snapshot_time);
CREATE INDEX market_stats_by_month_topology_context_id ON market_stats_by_month USING btree (topology_context_id);

CREATE TABLE mkt_snapshots (
    id bigserial,
    mkt_snapshot_source_id bigint,
    scenario_id bigint NOT NULL,
    display_name character varying(250) DEFAULT ''::character varying NOT NULL,
    state character varying(80) DEFAULT NULL::character varying,
    committed boolean,
    run_time timestamp with time zone NOT NULL,
    run_complete_time timestamp with time zone,
    PRIMARY KEY (id)
);
CREATE INDEX mkt_snapshots_committed ON mkt_snapshots USING btree (committed);
CREATE INDEX mkt_snapshots_scenario_id ON mkt_snapshots USING btree (scenario_id);

CREATE TABLE mkt_snapshots_stats (
    id bigserial,
    recorded_on timestamp with time zone NOT NULL,
    mkt_snapshot_id bigint NOT NULL,
    property_type character varying(36) DEFAULT ''::character varying NOT NULL,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(30,3) DEFAULT NULL::numeric,
    avg_value numeric(30,3) DEFAULT NULL::numeric,
    min_value numeric(30,3) DEFAULT NULL::numeric,
    max_value numeric(30,3) DEFAULT NULL::numeric,
    projection_time timestamp with time zone,
    entity_type smallint DEFAULT '2047'::smallint NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX mkt_snapshots_stats_mkt_snapshot_id ON mkt_snapshots_stats USING btree (mkt_snapshot_id);
ALTER TABLE ONLY mkt_snapshots_stats
    ADD CONSTRAINT mkt_snapshots_stats_ibfk_1 FOREIGN KEY (mkt_snapshot_id) REFERENCES mkt_snapshots(id) ON UPDATE CASCADE ON DELETE CASCADE;

CREATE TABLE moving_statistics_blobs (
    start_timestamp bigint NOT NULL,
    data bytea,
    chunk_index bigint DEFAULT '0'::bigint NOT NULL,
    PRIMARY KEY (start_timestamp, chunk_index)
);
CREATE INDEX moving_statistics_blobs_start_timestamp ON moving_statistics_blobs USING btree (start_timestamp);

CREATE TABLE notifications (
    id bigint NOT NULL,
    clear_time timestamp with time zone,
    last_notify_time timestamp with time zone,
    severity character varying(80) DEFAULT NULL::character varying,
    category character varying(80) DEFAULT NULL::character varying,
    name character varying(250) DEFAULT NULL::character varying,
    uuid varchar(20) DEFAULT NULL,
    importance double precision,
    description text,
    notification_uuid varchar(20) DEFAULT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE percentile_blobs (
    start_timestamp bigint NOT NULL,
    aggregation_window_length bigint DEFAULT '0'::bigint NOT NULL,
    data bytea,
    chunk_index bigint DEFAULT '0'::bigint NOT NULL,
    PRIMARY KEY (start_timestamp, chunk_index)
);
CREATE INDEX percentile_blobs_start_timestamp ON percentile_blobs USING btree (start_timestamp);

CREATE TABLE retention_policies (
    policy_name character varying(50) NOT NULL,
    retention_period bigint,
    unit character varying(20) DEFAULT NULL::character varying,
    PRIMARY KEY (policy_name)
);
COPY retention_policies (policy_name, retention_period, unit) FROM stdin;
moving_statistics_retention_days	2	DAYS
percentile_retention_days	91	DAYS
retention_days	60	DAYS
retention_hours	72	HOURS
retention_latest_hours	2	HOURS
retention_months	24	MONTHS
systemload_retention_days	30	DAYS
timeslot_retention_hours	720	\N
\.

CREATE TABLE ri_stats_latest (
    snapshot_time timestamp with time zone,
    uuid varchar(20) DEFAULT NULL,
    producer_uuid varchar(20) DEFAULT NULL,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    commodity_key character varying(80) DEFAULT NULL::character varying,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric
);
CREATE INDEX ri_stats_latest_property_subtype ON ri_stats_latest USING btree (property_subtype);
CREATE INDEX ri_stats_latest_property_type ON ri_stats_latest USING btree (property_type);
CREATE INDEX ri_stats_latest_snapshot_time ON ri_stats_latest USING btree (snapshot_time);
CREATE INDEX ri_stats_latest_uuid ON ri_stats_latest USING btree (uuid);

CREATE TABLE ri_stats_by_hour (
    snapshot_time timestamp with time zone,
    uuid varchar(20) DEFAULT NULL,
    producer_uuid varchar(20) DEFAULT NULL,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    commodity_key character varying(80) DEFAULT NULL::character varying,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric
);
CREATE INDEX ri_stats_by_hour_snapshot_time ON ri_stats_by_hour USING btree (snapshot_time, uuid, property_type, property_subtype);

CREATE TABLE ri_stats_by_day (
    snapshot_time date,
    uuid varchar(20) DEFAULT NULL,
    producer_uuid varchar(20) DEFAULT NULL,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    commodity_key character varying(80) DEFAULT NULL::character varying,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric
);
CREATE INDEX ri_stats_by_day_snapshot_time ON ri_stats_by_day USING btree (snapshot_time, uuid, property_type, property_subtype);

CREATE TABLE ri_stats_by_month (
    snapshot_time date,
    uuid varchar(20) DEFAULT NULL,
    producer_uuid varchar(20) DEFAULT NULL,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity numeric(15,3) DEFAULT NULL::numeric,
    avg_value numeric(15,3) DEFAULT NULL::numeric,
    min_value numeric(15,3) DEFAULT NULL::numeric,
    max_value numeric(15,3) DEFAULT NULL::numeric,
    relation smallint,
    commodity_key character varying(80) DEFAULT NULL::character varying,
    effective_capacity numeric(15,3) DEFAULT NULL::numeric
);
CREATE INDEX ri_stats_by_month_snapshot_time ON ri_stats_by_month USING btree (snapshot_time, uuid, property_type, property_subtype);

CREATE TABLE scenarios (
    id bigserial,
    display_name character varying(250) DEFAULT ''::character varying NOT NULL,
    create_time timestamp with time zone NOT NULL,
    update_time timestamp with time zone,
    source_scenario_id bigint,
    type character varying(80) DEFAULT NULL::character varying,
    PRIMARY KEY (id)
);
CREATE INDEX scenarios_display_name ON scenarios USING btree (display_name);
CREATE INDEX scenarios_source_scenario_id ON scenarios USING btree (source_scenario_id);
ALTER TABLE ONLY mkt_snapshots
    ADD CONSTRAINT mkt_snapshots_ibfk_1 FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON UPDATE CASCADE ON DELETE CASCADE;

CREATE TABLE system_load (
    slice character varying(250) DEFAULT NULL::character varying,
    snapshot_time timestamp with time zone,
    uuid varchar(20) DEFAULT NULL,
    producer_uuid varchar(20) DEFAULT NULL,
    property_type character varying(36) DEFAULT NULL::character varying,
    property_subtype character varying(36) DEFAULT NULL::character varying,
    capacity double precision,
    avg_value double precision,
    min_value double precision,
    max_value double precision,
    relation smallint,
    commodity_key character varying(80) DEFAULT NULL::character varying
);
CREATE INDEX system_load_slice_property_time ON system_load USING btree (slice, property_type, snapshot_time);
CREATE INDEX system_load_time ON system_load USING btree (snapshot_time);

CREATE TABLE version_info (
    id bigserial,
    version double precision,
    PRIMARY KEY (id)
);
COPY version_info (id, version) FROM stdin;
1	73.7
\.

CREATE TABLE volume_attachment_history (
    volume_oid bigint NOT NULL,
    vm_oid bigint NOT NULL,
    last_attached_date date NOT NULL,
    last_discovered_date date NOT NULL,
    PRIMARY KEY (volume_oid, vm_oid)
);
