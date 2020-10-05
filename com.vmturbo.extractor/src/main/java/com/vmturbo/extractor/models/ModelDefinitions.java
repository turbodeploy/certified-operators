package com.vmturbo.extractor.models;

import java.sql.Timestamp;
import java.time.OffsetDateTime;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.tables.Metric;

/**
 * Definitions of models, tables, and columns used in topology ingestion.
 */
public class ModelDefinitions {

    private ModelDefinitions() {
    }

    /** TIME column. */
    public static final Column<Timestamp> TIME = Column.timestampColumn("time");
    /** ENTITY_OID column. */
    public static final Column<Long> ENTITY_OID = Column.longColumn("entity_oid");
    /** ENTITY_OID, named just "oid". */
    public static final Column<Long> ENTITY_OID_AS_OID = Column.longColumn("oid");
    /** ENTITY_HASH column. */
    public static final Column<Long> ENTITY_HASH = Column.longColumn("entity_hash");
    /** ENTITY_HASH column, named just "hash". */
    public static final Column<Long> ENTITY_HASH_AS_HASH = Column.longColumn("hash");
    /** ENTITY_NAME column. */
    public static final Column<String> ENTITY_NAME = Column.stringColumn("name");
    /** ATTRS column. */
    public static final Column<JsonString> ATTRS = Column.jsonColumn("attrs");
    /** SCOPED_OIDS column. */
    public static final Column<Long[]> SCOPED_OIDS = Column.longSetColumn("scoped_oids");
    /** FIRST_SEEN column. */
    public static final Column<Timestamp> FIRST_SEEN = Column.timestampColumn("first_seen");
    /** LAST_SEEN column. */
    public static final Column<Timestamp> LAST_SEEN = Column.timestampColumn("last_seen");
    /** COMMODITY_TYPE column. */
    public static final Column<MetricType> COMMODITY_TYPE = new Column<>(Metric.METRIC.TYPE, ColType.METRIC_TYPE);
    /** COMMODITY_KEY column. */
    public static final Column<String> COMMODITY_KEY = Column.stringColumn("key");
    /** COMMODITY_CURRENT column. */
    public static final Column<Double> COMMODITY_CURRENT = Column.doubleColumn("current");
    /** COMMODITY_CAPACITY column. */
    public static final Column<Double> COMMODITY_CAPACITY = Column.doubleColumn("capacity");
    /** COMMODITY_UTILIZATION column. */
    public static final Column<Double> COMMODITY_UTILIZATION = Column.doubleColumn("utilization");
    /** COMMODITY_CONSUMED column. */
    public static final Column<Double> COMMODITY_CONSUMED = Column.doubleColumn("consumed");
    /** COMMODITY_PROVIDER column. */
    public static final Column<Long> COMMODITY_PROVIDER = Column.longColumn("provider_oid");
    /** SCOPED_OID column. */
    public static final Column<Long> SCOPED_OID = Column.longColumn("scoped_oid");
    /** SCOPED_TYPE column. */
    public static final Column<EntityType> SCOPED_TYPE = Column.entityTypeColumn("scoped_type");
    /** SCOPE_BEGIN column. */
    public static final Column<OffsetDateTime> SCOPE_START = Column.offsetDateTimeColumn("start");
    /** SCOPE_END column. */
    public static final Column<OffsetDateTime> SCOPE_FINISH = Column.offsetDateTimeColumn("finish");

    /** Column for file path. */
    public static final Column<String> FILE_PATH = Column.stringColumn("path");
    /** Column for last modification time. */
    public static final Column<Timestamp> MODIFICATION_TIME = Column.timestampColumn("modification_time");
    /** Column for file size. */
    public static final Column<Long> FILE_SIZE = Column.longColumn("file_size_kb");
    /** Column for storage oid. */
    public static final Column<Long> STORAGE_OID = Column.longColumn("storage_oid");
    /** Column for storage displayName. */
    public static final Column<String> STORAGE_NAME = Column.stringColumn("storage_name");

    /** ENTITY STATE enum column. */
    public static final Column<EntityState> ENTITY_STATE_ENUM = Column.entityStateColumn("state");
    /** ENVIRONMENT TYPE enum column. */
    public static final Column<EnvironmentType> ENVIRONMENT_TYPE_ENUM = Column.environmentTypeColumn("environment");
    /** ENTITY TYPE enum column. */
    public static final Column<EntityType> ENTITY_TYPE_ENUM = Column.entityTypeColumn("type");
    /** ENTITY SEVERITY enum column. */
    public static final Column<Severity> SEVERITY_ENUM = Column.severityColumn("severity");
    /** ACTIONS COUNT enum column. */
    public static final Column<Integer> NUM_ACTIONS = Column.intColumn("num_actions");

    /** wasted_file table. */
    public static final Table WASTED_FILE_TABLE = Table.named("wasted_file")
            .withColumns(FILE_PATH, FILE_SIZE, MODIFICATION_TIME, STORAGE_OID, STORAGE_NAME)
            .build();

    /** ENTITY_TABLE. */
    public static final Table ENTITY_TABLE = Table.named("entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_HASH_AS_HASH, ENTITY_TYPE_ENUM, ENTITY_NAME,
                    ENVIRONMENT_TYPE_ENUM, ENTITY_STATE_ENUM, ATTRS, SCOPED_OIDS, FIRST_SEEN, LAST_SEEN)
            .build();

    /** SCOPE_TABLE. */
    public static final Table SCOPE_TABLE = Table.named("scope")
            .withColumns(ENTITY_OID, SCOPED_OID, SCOPED_TYPE, SCOPE_START, SCOPE_FINISH)
            .build();

    /** METRIC_TABLE. */
    public static final Table METRIC_TABLE = Table.named("metric")
            .withColumns(TIME, ENTITY_OID, ENTITY_HASH, COMMODITY_TYPE,
                    COMMODITY_CURRENT, COMMODITY_CAPACITY, COMMODITY_UTILIZATION, COMMODITY_CONSUMED,
                    COMMODITY_PROVIDER,
                    COMMODITY_KEY)
            .build();

    /** REPORTING_MODEL. */
    public static final Model REPORTING_MODEL = Model.named("reporting")
            .withTables(ENTITY_TABLE, METRIC_TABLE, WASTED_FILE_TABLE)
            .build();

    /** SEARCH_ENTITY_TABLE. */
    public static final Table SEARCH_ENTITY_TABLE = Table.named("search_entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_TYPE_ENUM, ENTITY_NAME, ENVIRONMENT_TYPE_ENUM,
                    ENTITY_STATE_ENUM, ATTRS)
            .build();

    /** SEARCH_ENTITY_ACTION_TABLE. */
    public static final Table SEARCH_ENTITY_ACTION_TABLE = Table.named("search_entity_action")
            .withColumns(ENTITY_OID_AS_OID, NUM_ACTIONS, SEVERITY_ENUM)
            .build();

    /** SEARCH_MODEL. */
    public static final Model SEARCH_MODEL = Model.named("search")
            .withTables(SEARCH_ENTITY_TABLE)
            .build();

}

