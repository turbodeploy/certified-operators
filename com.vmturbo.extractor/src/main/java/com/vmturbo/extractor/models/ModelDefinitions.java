package com.vmturbo.extractor.models;

import java.sql.Timestamp;
import java.time.OffsetDateTime;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.Tables;
import com.vmturbo.extractor.schema.enums.AttrType;
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
    /** ENTITY_NAME column. */
    public static final Column<String> ENTITY_NAME = Column.stringColumn("name");
    /** ATTRS column. */
    public static final Column<JsonString> ATTRS = Column.jsonColumn("attrs");
    /** SEED column. */
    public static final Column<Long> SEED_OID = Column.longColumn("seed_oid");
    /** FIRST_SEEN column. */
    public static final Column<OffsetDateTime> FIRST_SEEN = Column.offsetDateTimeColumn("first_seen");
    /** LAST_SEEN column. */
    public static final Column<OffsetDateTime> LAST_SEEN = Column.offsetDateTimeColumn("last_seen");
    /** COMMODITY_TYPE column. */
    public static final Column<MetricType> COMMODITY_TYPE = new Column<>(Metric.METRIC.TYPE, ColType.METRIC_TYPE);
    /** COMMODITY_KEY column. */
    public static final Column<String> COMMODITY_KEY = Column.stringColumn("key");
    /** COMMODITY_CURRENT column. */
    public static final Column<Double> COMMODITY_CURRENT = Column.doubleColumn("current");
    /** COMMODITY CURRENT PEAK column. */
    public static final Column<Double> COMMODITY_PEAK_CURRENT = Column.doubleColumn("peak_current");
    /** COMMODITY_CAPACITY column. */
    public static final Column<Double> COMMODITY_CAPACITY = Column.doubleColumn("capacity");
    /** COMMODITY_UTILIZATION column. */
    public static final Column<Double> COMMODITY_UTILIZATION = Column.doubleColumn("utilization");
    /** COMMODITY_CONSUMED column. */
    public static final Column<Double> COMMODITY_CONSUMED = Column.doubleColumn("consumed");
    /** COMMODITY CONSUMED PEAK column. */
    public static final Column<Double> COMMODITY_PEAK_CONSUMED = Column.doubleColumn("peak_consumed");
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
    /** ENTITY TYPE enum column, named as "type". */
    public static final Column<EntityType> ENTITY_TYPE_AS_TYPE_ENUM = Column.entityTypeColumn("type");
    /** ENTITY TYPE enum column. */
    public static final Column<EntityType> ENTITY_TYPE_ENUM = Column.entityTypeColumn("entity_type");
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
            .withColumns(ENTITY_OID_AS_OID, ENTITY_TYPE_AS_TYPE_ENUM, ENTITY_NAME,
                    ENVIRONMENT_TYPE_ENUM, ATTRS, FIRST_SEEN, LAST_SEEN)
            .build();

    /** SCOPE_TABLE. */
    public static final Table SCOPE_TABLE = Table.named("scope")
            .withColumns(SEED_OID, SCOPED_OID, SCOPED_TYPE, SCOPE_START, SCOPE_FINISH)
            .build();

    /** METRIC_TABLE. */
    public static final Table METRIC_TABLE = Table.named("metric")
            .withColumns(TIME, ENTITY_OID, COMMODITY_TYPE, COMMODITY_PROVIDER, COMMODITY_KEY,
                    COMMODITY_CURRENT, COMMODITY_CAPACITY, COMMODITY_UTILIZATION,
                    COMMODITY_CONSUMED, COMMODITY_PEAK_CURRENT, COMMODITY_PEAK_CONSUMED,
                    ENTITY_TYPE_ENUM)
            .build();

    /** REPORTING_MODEL. */
    public static final Model REPORTING_MODEL = Model.named("reporting")
            .withTables(ENTITY_TABLE, METRIC_TABLE, WASTED_FILE_TABLE)
            .build();

    /** SEARCH_ENTITY_TABLE. */
    public static final Table SEARCH_ENTITY_TABLE = Table.named("search_entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_TYPE_AS_TYPE_ENUM, ENTITY_NAME, ENVIRONMENT_TYPE_ENUM,
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

    /**
     * The historical attributes model.
     */
    public static class HistoricalAttributes {
        /** TIME column. */
        public static final Column<Timestamp> TIME = Column.timestampColumn(Tables.HISTORICAL_ENTITY_ATTRS.TIME);

        /** ENTITY_OID column. */
        public static final Column<Long> ENTITY_OID = Column.longColumn(Tables.HISTORICAL_ENTITY_ATTRS.ENTITY_OID);

        /** TYPE column. */
        public static final Column<AttrType> TYPE = Column.attrTypeColumn(Tables.HISTORICAL_ENTITY_ATTRS.TYPE);

        /** BOOL_VALUE column. */
        public static final Column<Boolean> BOOL_VALUE = Column.boolColumn(Tables.HISTORICAL_ENTITY_ATTRS.BOOL_VALUE);

        /** INT_VALUE column. */
        public static final Column<Integer> INT_VALUE = Column.intColumn(Tables.HISTORICAL_ENTITY_ATTRS.INT_VALUE);

        /** LONG_VALUE column. */
        public static final Column<Long> LONG_VALUE = Column.longColumn(Tables.HISTORICAL_ENTITY_ATTRS.LONG_VALUE);

        /** DOUBLE_VALUE column. */
        public static final Column<Double> DOUBLE_VALUE = Column.doubleColumn(Tables.HISTORICAL_ENTITY_ATTRS.DOUBLE_VALUE);

        /** STRING_VALUE column. */
        public static final Column<String> STRING_VALUE = Column.stringColumn(Tables.HISTORICAL_ENTITY_ATTRS.STRING_VALUE);

        /** INT_ARR_VALUE column. */
        public static final Column<Integer[]> INT_ARR_VALUE = Column.intArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.INT_ARR_VALUE);

        /** LONG_ARR_VALUE column. */
        public static final Column<Long[]> LONG_ARR_VALUE = Column.longArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.LONG_ARR_VALUE);

        /** DOUBLE_ARR_VALUE column. */
        public static final Column<Double[]> DOUBLE_ARR_VALUE = Column.doubleArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.DOUBLE_ARR_VALUE);

        /** STRING_ARR_VALUE column. */
        public static final Column<String[]> STRING_ARR_VALUE = Column.stringArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.STRING_ARR_VALUE);

        /** JSON_VALUE column. */
        public static final Column<JsonString> JSON_VALUE = Column.jsonColumn(Tables.HISTORICAL_ENTITY_ATTRS.JSON_VALUE);

        /** TABLE containing all the columns. */
        public static final Table TABLE = Table.named(Tables.HISTORICAL_ENTITY_ATTRS.getName())
                .withColumns(TIME,
                        ENTITY_OID,
                        TYPE,
                        BOOL_VALUE,
                        INT_VALUE,
                        LONG_VALUE,
                        DOUBLE_VALUE,
                        STRING_VALUE,
                        INT_ARR_VALUE,
                        LONG_ARR_VALUE,
                        DOUBLE_ARR_VALUE,
                        STRING_ARR_VALUE,
                        JSON_VALUE)
                .build();

    }

}

