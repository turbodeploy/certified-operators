package com.vmturbo.extractor.models;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.Tables;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.FileType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.enums.SavingsType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.tables.Metric;
import com.vmturbo.search.metadata.DbFieldDescriptor;
import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Definitions of models, tables, and columns used in topology ingestion.
 */
public class ModelDefinitions {

    private ModelDefinitions() {
    }

    /**
     * TIME column.
     */
    public static final Column<Timestamp> TIME = Column.timestampColumn("time");
    /**
     * ENTITY_OID column.
     */
    public static final Column<Long> ENTITY_OID = Column.longColumn("entity_oid");
    /**
     * ENTITY_OID, named just "oid".
     */
    public static final Column<Long> ENTITY_OID_AS_OID = Column.longColumn("oid");
    /**
     * ENTITY_NAME column.
     */
    public static final Column<String> ENTITY_NAME = Column.stringColumn("name");
    /**
     * FIELD_NAME column.
     */
    public static final Column<Integer> FIELD_NAME = Column.intColumn("name");
    /**
     * ATTRS column.
     */
    public static final Column<JsonString> ATTRS = Column.jsonColumn("attrs");
    /**
     * SEED column.
     */
    public static final Column<Long> SEED_OID = Column.longColumn("seed_oid");
    /**
     * FIRST_SEEN column.
     */
    public static final Column<OffsetDateTime> FIRST_SEEN = Column.offsetDateTimeColumn("first_seen");
    /**
     * LAST_SEEN column.
     */
    public static final Column<OffsetDateTime> LAST_SEEN = Column.offsetDateTimeColumn("last_seen");
    /**
     * COMMODITY_TYPE column.
     */
    public static final Column<MetricType> COMMODITY_TYPE = new Column<>(Metric.METRIC.TYPE, ColType.METRIC_TYPE);
    /**
     * COMMODITY_KEY column.
     */
    public static final Column<String> COMMODITY_KEY = Column.stringColumn("key");
    /**
     * COMMODITY_CURRENT column.
     */
    public static final Column<Double> COMMODITY_CURRENT = Column.doubleColumn("current");
    /**
     * COMMODITY CURRENT PEAK column.
     */
    public static final Column<Double> COMMODITY_PEAK_CURRENT = Column.doubleColumn("peak_current");
    /**
     * COMMODITY_CAPACITY column.
     */
    public static final Column<Double> COMMODITY_CAPACITY = Column.doubleColumn("capacity");
    /**
     * COMMODITY_UTILIZATION column.
     */
    public static final Column<Double> COMMODITY_UTILIZATION = Column.doubleColumn("utilization");
    /**
     * COMMODITY_CONSUMED column.
     */
    public static final Column<Double> COMMODITY_CONSUMED = Column.doubleColumn("consumed");
    /**
     * COMMODITY CONSUMED PEAK column.
     */
    public static final Column<Double> COMMODITY_PEAK_CONSUMED = Column.doubleColumn("peak_consumed");
    /**
     * COMMODITY_PROVIDER column.
     */
    public static final Column<Long> COMMODITY_PROVIDER = Column.longColumn("provider_oid");
    /**
     * SCOPED_OID column.
     */
    public static final Column<Long> SCOPED_OID = Column.longColumn("scoped_oid");
    /**
     * SCOPED_TYPE column.
     */
    public static final Column<EntityType> SCOPED_TYPE = Column.entityTypeColumn("scoped_type");
    /**
     * SCOPE_BEGIN column.
     */
    public static final Column<OffsetDateTime> SCOPE_START = Column.offsetDateTimeColumn("start");
    /**
     * SCOPE_END column.
     */
    public static final Column<OffsetDateTime> SCOPE_FINISH = Column.offsetDateTimeColumn("finish");

    /**
     * Column for volume doi.
     */
    public static final Column<Long> VOLUME_OID = Column.longColumn("volume_oid");
    /**
     * Column for file path.
     */
    public static final Column<String> FILE_PATH = Column.stringColumn("path");
    /**
     * Column for file type.
     */
    public static final Column<FileType> FILE_TYPE = Column.fileTypeColumn("type");
    /**
     * Column for last modification time.
     */
    public static final Column<Timestamp> MODIFICATION_TIME = Column.timestampColumn(
            "modification_time");
    /**
     * Column for file size.
     */
    public static final Column<Long> FILE_SIZE = Column.longColumn("file_size_kb");
    /**
     * Column for storage oid.
     */
    public static final Column<Long> STORAGE_OID = Column.longColumn("storage_oid");
    /**
     * Column for is_attached column.
     */
    public static final Column<Boolean> IS_ATTACHED = Column.boolColumn("is_attached");
    /**
     * Colum for file record hash.
     */
    public static final Column<Long> FILE_HASH = Column.longColumn("hash");
    /**
     * Column for storage displayName (appears in wasted_file view).
     */
    public static final Column<String> STORAGE_NAME = Column.stringColumn("storage_name");

    /**
     * ENTITY STATE enum column.
     */
    public static final Column<EntityState> ENTITY_STATE_ENUM = Column.entityStateColumn("state");
    /**
     * ENVIRONMENT TYPE enum column.
     */
    public static final Column<EnvironmentType> ENVIRONMENT_TYPE_ENUM = Column.environmentTypeColumn("environment");
    /**
     * ENTITY TYPE enum column, named as "type".
     */
    public static final Column<EntityType> ENTITY_TYPE_AS_TYPE_ENUM = Column.entityTypeColumn("type");
    /**
     * ENTITY TYPE enum column.
     */
    public static final Column<EntityType> ENTITY_TYPE_ENUM = Column.entityTypeColumn("entity_type");
    /**
     * ENTITY SEVERITY enum column.
     */
    public static final Column<Severity> SEVERITY_ENUM = Column.severityColumn("severity");
    /**
     * ACTIONS COUNT enum column.
     */
    public static final Column<Integer> NUM_ACTIONS = Column.intColumn("num_actions");
    /**
     * STRING VALUE column.
     */
    public static final Column<String> STRING_VALUE = Column.stringColumn("value");
    /**
     * DOUBLE VALUE column.
     */
    public static final Column<Double> DOUBLE_VALUE = Column.doubleColumn("value");
    /**
     * file table.
     */
    public static final Table FILE_TABLE = Table.named("file").withColumns(VOLUME_OID, FILE_PATH,
            FILE_TYPE, FILE_SIZE, MODIFICATION_TIME, STORAGE_OID, IS_ATTACHED, FILE_HASH).build();

    /**
     * ENTITY_TABLE.
     */
    public static final Table ENTITY_TABLE = Table.named("entity").withColumns(ENTITY_OID_AS_OID,
            ENTITY_TYPE_AS_TYPE_ENUM, ENTITY_NAME, ENVIRONMENT_TYPE_ENUM, ATTRS, FIRST_SEEN,
            LAST_SEEN).build();

    /**
     * SCOPE_TABLE.
     */
    public static final Table SCOPE_TABLE = Table.named("scope").withColumns(SEED_OID, SCOPED_OID,
            SCOPED_TYPE, SCOPE_START, SCOPE_FINISH).build();

    /**
     * METRIC_TABLE.
     */
    public static final Table METRIC_TABLE = Table.named("metric").withColumns(TIME, ENTITY_OID,
            COMMODITY_TYPE, COMMODITY_PROVIDER, COMMODITY_KEY, COMMODITY_CURRENT, COMMODITY_CAPACITY, COMMODITY_UTILIZATION,
            COMMODITY_CONSUMED, COMMODITY_PEAK_CURRENT, COMMODITY_PEAK_CONSUMED, ENTITY_TYPE_ENUM).build();

    /**
     * REPORTING_MODEL.
     */
    public static final Model REPORTING_MODEL = Model.named("reporting").withTables(ENTITY_TABLE,
            METRIC_TABLE, FILE_TABLE).build();

    // -------- search model is driven by fields' declarations in SearchMetadataMapping used for both writing and reading

    private static final Map<DataType<?>, Function<String, Column<?>>> JOOQ_TO_HANDWRITTEN_EXTRACTOR_ORM =
                    new ImmutableMap.Builder<DataType<?>, Function<String, Column<?>>>()
                        .put(SQLDataType.DOUBLE, (name) -> Column.doubleColumn(name))
                        .put(SQLDataType.TINYINT, (name) -> Column.shortColumn(name))
                        .put(SQLDataType.SMALLINT, (name) -> Column.intColumn(name))
                        .put(SQLDataType.BIGINT, (name) -> Column.longColumn(name))
                        .put(SQLDataType.VARCHAR(DbFieldDescriptor.STRING_SIZE), (name) -> Column.stringColumn(name))
                        .build();

    /**
     * SEARCH_ENTITY_TABLE.
     */
    public static final Table SEARCH_ENTITY_TABLE = Table.named(Location.Entities.getTable())
            .withColumns(getSearchColumns(Location.Entities)).build();

    /**
     * SEARCH_ENTITY_ACTION_TABLE.
     */
    public static final Table SEARCH_ENTITY_ACTION_TABLE = Table.named(Location.Actions.getTable())
                    .withColumns(getSearchColumns(Location.Actions)).build();

    /**
     * SEARCH_ENTITY_STRING_TABLE.
     */
    public static final Table SEARCH_ENTITY_STRING_TABLE = Table.named(Location.Strings.getTable())
                    .withColumns(ENTITY_OID_AS_OID, FIELD_NAME, STRING_VALUE).build();

    /**
     * SEARCH_ENTITY_NUMERIC_TABLE.
     */
    public static final Table SEARCH_ENTITY_NUMERIC_TABLE = Table.named(Location.Numerics.getTable())
                    .withColumns(ENTITY_OID_AS_OID, FIELD_NAME, DOUBLE_VALUE).build();

    /**
     * SEARCH_MODEL.
     */
    public static final Model SEARCH_MODEL = Model.named("search")
                    .withTables(SEARCH_ENTITY_TABLE, SEARCH_ENTITY_ACTION_TABLE,
                                    SEARCH_ENTITY_STRING_TABLE, SEARCH_ENTITY_NUMERIC_TABLE)
                    .build();

    /**
     * The historical attributes model.
     */
    public static class HistoricalAttributes {
        /**
         * TIME column.
         */
        public static final Column<Timestamp> TIME = Column.timestampColumn(Tables.HISTORICAL_ENTITY_ATTRS.TIME);

        /**
         * ENTITY_OID column.
         */
        public static final Column<Long> ENTITY_OID = Column.longColumn(Tables.HISTORICAL_ENTITY_ATTRS.ENTITY_OID);

        /**
         * TYPE column.
         */
        public static final Column<AttrType> TYPE = Column.attrTypeColumn(Tables.HISTORICAL_ENTITY_ATTRS.TYPE);

        /**
         * BOOL_VALUE column.
         */
        public static final Column<Boolean> BOOL_VALUE = Column.boolColumn(Tables.HISTORICAL_ENTITY_ATTRS.BOOL_VALUE);

        /**
         * INT_VALUE column.
         */
        public static final Column<Integer> INT_VALUE = Column.intColumn(Tables.HISTORICAL_ENTITY_ATTRS.INT_VALUE);

        /**
         * LONG_VALUE column.
         */
        public static final Column<Long> LONG_VALUE = Column.longColumn(Tables.HISTORICAL_ENTITY_ATTRS.LONG_VALUE);

        /**
         * DOUBLE_VALUE column.
         */
        public static final Column<Double> DOUBLE_VALUE = Column.doubleColumn(Tables.HISTORICAL_ENTITY_ATTRS.DOUBLE_VALUE);

        /**
         * STRING_VALUE column.
         */
        public static final Column<String> STRING_VALUE = Column.stringColumn(Tables.HISTORICAL_ENTITY_ATTRS.STRING_VALUE);

        /**
         * INT_ARR_VALUE column.
         */
        public static final Column<Integer[]> INT_ARR_VALUE = Column.intArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.INT_ARR_VALUE);

        /**
         * LONG_ARR_VALUE column.
         */
        public static final Column<Long[]> LONG_ARR_VALUE = Column.longArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.LONG_ARR_VALUE);

        /**
         * DOUBLE_ARR_VALUE column.
         */
        public static final Column<Double[]> DOUBLE_ARR_VALUE = Column.doubleArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.DOUBLE_ARR_VALUE);

        /**
         * STRING_ARR_VALUE column.
         */
        public static final Column<String[]> STRING_ARR_VALUE = Column.stringArrayColumn(Tables.HISTORICAL_ENTITY_ATTRS.STRING_ARR_VALUE);

        /**
         * JSON_VALUE column.
         */
        public static final Column<JsonString> JSON_VALUE = Column.jsonColumn(Tables.HISTORICAL_ENTITY_ATTRS.JSON_VALUE);

        /**
         * TABLE containing all the columns.
         */
        public static final Table TABLE = Table.named(Tables.HISTORICAL_ENTITY_ATTRS.getName())
                .withColumns(TIME, ENTITY_OID, TYPE, BOOL_VALUE, INT_VALUE, LONG_VALUE,
                        DOUBLE_VALUE, STRING_VALUE, INT_ARR_VALUE, LONG_ARR_VALUE, DOUBLE_ARR_VALUE,
                        STRING_ARR_VALUE, JSON_VALUE)
                .build();
    }

    /**
     * The entity_cost model.
     */
    public static class EntityCost {
        /**
         * TIME column.
         */
        public static final Column<Timestamp> TIME = Column.timestampColumn(Tables.ENTITY_COST.TIME);

        /**
         * ENTITY_OID column.
         */
        public static final Column<Long> ENTITY_OID = Column.longColumn(Tables.ENTITY_COST.ENTITY_OID);

        /**
         * CATEGORY column.
         */
        public static final Column<CostCategory> CATEGORY = Column.costCategoryColumn(Tables.ENTITY_COST.CATEGORY);

        /**
         * SOURCE column.
         */
        public static final Column<CostSource> SOURCE = Column.costSourceColumn(Tables.ENTITY_COST.SOURCE);

        /**
         * COST column.
         */
        public static final Column<Float> COST = Column.floatColumn(Tables.ENTITY_COST.COST.getName());

        /**
         * TABLE containing all the columns.
         */
        public static final Table TABLE = Table.named(Tables.ENTITY_COST.getName()).withColumns(TIME,
                ENTITY_OID, CATEGORY, SOURCE, COST).build();
    }

    /**
     * Table definition for cloud_service_cost table having billing account expense data.
     */
    public static class CloudServiceCost {
        /**
         * Timestamp column.
         */
        public static final Column<Timestamp> TIME = Column.timestampColumn(
                Tables.CLOUD_SERVICE_COST.TIME);

        /**
         * Business account oid.
         */
        public static final Column<Long> ACCOUNT_OID = Column.longColumn(
                Tables.CLOUD_SERVICE_COST.ACCOUNT_OID);

        /**
         * Cloud service oid.
         */
        public static final Column<Long> CLOUD_SERVICE_OID = Column.longColumn(
                Tables.CLOUD_SERVICE_COST.CLOUD_SERVICE_OID);

        /**
         * Cloud service cost ($/hr).
         */
        public static final Column<Double> COST = Column.doubleColumn(
                Tables.CLOUD_SERVICE_COST.COST.getName());

        /**
         * Cloud service cost table.
         */
        public static final Table TABLE = Table.named(Tables.CLOUD_SERVICE_COST.getName())
                .withColumns(TIME, ACCOUNT_OID, CLOUD_SERVICE_OID, COST)
                .build();
    }

    /**
     * The entity_savings model.
     */
    public static class EntitySavings {
        /**
         * TIME column.
         */
        public static final Column<Timestamp> TIME = Column.timestampColumn(
                Tables.ENTITY_SAVINGS.TIME);

        /**
         * ENTITY_OID column.
         */
        public static final Column<Long> ENTITY_OID = Column.longColumn(
                Tables.ENTITY_SAVINGS.ENTITY_OID);

        /**
         * SAVINGS_TYPE column.
         */
        public static final Column<SavingsType> SAVINGS_TYPE = Column.savingsTypeColumn(
                Tables.ENTITY_SAVINGS.SAVINGS_TYPE);

        /**
         * STATS_VALUE column.
         */
        public static final Column<Float> STATS_VALUE = Column.floatColumn(
                Tables.ENTITY_SAVINGS.STATS_VALUE.getName());

        /**
         * Cloud entity savings table.
         */
        public static final Table TABLE = Table.named(Tables.ENTITY_SAVINGS.getName()).withColumns(
                TIME, ENTITY_OID, SAVINGS_TYPE, STATS_VALUE).build();

        /**
         * Display name.
         * @return Table display name for logging.
         */
        public static String name() {
            return Tables.ENTITY_SAVINGS.getName();
        }
    }

    private static Collection<Column<?>> getSearchColumns(Location location) {
        return Arrays.stream(SearchMetadataMapping.values())
                        .filter(smm -> smm.getDbDescriptor().getLocations().contains(location))
                        .map(SearchMetadataMapping::getDbDescriptor)
                        .map(field -> Optional.ofNullable(JOOQ_TO_HANDWRITTEN_EXTRACTOR_ORM.get(field.getDbType()))
                                        .map(fn -> fn.apply(field.getColumn())))
                        .filter(Optional::isPresent).map(Optional::get)
                        .collect(Collectors.toList());
    }
}
