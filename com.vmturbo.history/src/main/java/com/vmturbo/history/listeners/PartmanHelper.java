package com.vmturbo.history.listeners;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Parameter;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.AbstractRoutine;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;

/**
 * Class to handle integration with the Postgres pg_partman extension to manage partitions for
 * entity-stats tables.
 *
 * <p>There are two events that require action by this class:</p>
 *
 * <ul>
 *     <li>Ensuring that a partition eligible to receive new data exists in the receiving table.</li>
 *     <li>Ensuring that current retention settings are reflected in the pg_partman metadata.</li>
 * </ul>
 *
 * <p>The first situation arises whenever an ingested topology produces records that need to be
 * added to the `_latest` table for some entity type. Since all records for a given topology will
 * have the same timestamp, this only needs to happen for the first record inserted. Therefore, we
 * now support a "pre-insertion hook" in {@link BulkInserter}, and {@link SimpleBulkLoaderFactory}
 * sets up a hook that calls {@link PartmanHelper#prepareForInsertion(Table, Timestamp)} to
 * sure the needed partition exists. The hook is not invoked until a record is actually sent to the
 * responsible {@link BulkInserter} for insertion.</p>
 *
 * <p>The first scenario also arises whenever we're about to perform rollups. In that case we know
 * the target table and the timestamp of all the records that will be affected in that table, so
 * a call to {@link PartmanHelper#prepareForInsertion(Table, Timestamp)} has been added directly
 * in the code path in {@link RollupProcessor}.</p>
 *
 * <p>The second scenario is triggered by a scheduling executor, which fires once an hour by
 * default by invoking {@link PartmanHelper#performRetentionUpdate()}. That method retrieves
 * current retention settings from `group` component and then saves them in pg_partman's
 * `part_config` table. It then runs partman's maintenance function for all tables, which will
 * remove any partitions that are expired according to the current settings.</p>
 */
public class PartmanHelper {
    // a bunch of hand-crafted DSL objects for composing SQL statements
    private static final Logger logger = LogManager.getLogger();

    private static final String PARTMAN_SCHEMA_NAME = "partman";
    private static final String PART_CONFIG_TABLE_NAME = "part_config";

    private static final Schema PARTMAN_SCHEMA = DSL.schema(DSL.name(PARTMAN_SCHEMA_NAME));
    private static final Table<?> PART_CONFIG_TABLE = partmanTable(PART_CONFIG_TABLE_NAME);

    private static final String RETENTION_FIELD_NAME = "retention";
    private static final Field<String> RETENTION_FIELD = tableField(PART_CONFIG_TABLE,
            RETENTION_FIELD_NAME, String.class);

    private static final String PARENT_TABLE_FIELD_NAME = "parent_table";
    private static final Field<String> PARENT_TABLE_FIELD = tableField(PART_CONFIG_TABLE,
            PARENT_TABLE_FIELD_NAME, String.class);

    private static final String INFINITE_TIME_PARTITIONS_FIELD_NAME = "infinite_time_partitions";
    private static final Field<Boolean> INFINITE_TIME_PARTITIONS_FIELD = tableField(
            PART_CONFIG_TABLE, INFINITE_TIME_PARTITIONS_FIELD_NAME, Boolean.class);

    private static final String RETENTION_KEEP_TABLE_FIELD_NAME = "retention_keep_table";
    private static final Field<Boolean> RETENTION_KEEP_TABLE_FIELD = tableField(
            PART_CONFIG_TABLE, RETENTION_KEEP_TABLE_FIELD_NAME, Boolean.class);
    private final ScheduledExecutorService retentionUpdateThreadPool;

    // static methods used in above
    private static Table<?> partmanTable(String tableName) {
        return DSL.table(DSL.name(PARTMAN_SCHEMA.getName(), tableName));
    }

    private static <T> Field<T> tableField(Table<?> table, String fieldName, Class<T> type) {
        return DSL.field(DSL.name(table.getName(), fieldName), type);
    }

    private static final String NATIVE_PARTITION_TYPE_NAME = "native";

    private static final String LATEST_TABLE_SUFFIX = "_latest";
    private static final String HOURLY_TABLE_SUFFIX = "_by_hour";
    private static final String DAILY_TABLE_SUFFIX = "_by_day";
    private static final String MONTHLY_TABLE_SUFFIX = "by_month";

    private final DSLContext dsl;
    private final String currentSchema;
    private final Map<TimeFrame, String> partitionIntervals;
    private final Duration retentionUpdateInterval;
    private final RetentionSettings retentionSettings;

    /**
     * Create a new instance.
     *
     * @param dsl                     for database access
     * @param settingService          access to group component's setting service
     * @param partitionIntervals      intervals for new partitions, based on config properties
     * @param latestRetentionMinutes  retention setting for `_latest` table (not available as a
     *                                setting or adjustable via the UI)
     * @param retentionUpdateInterval how often to run retention settings update
     * @param retentionUpdateThreadPool scheduling thread pool for updating retention settings
     * @throws PartitionProcessingException if there's a problem during construction
     */
    public PartmanHelper(DSLContext dsl, SettingServiceBlockingStub settingService,
            Map<TimeFrame, String> partitionIntervals, int latestRetentionMinutes,
            Duration retentionUpdateInterval, ScheduledExecutorService retentionUpdateThreadPool)
            throws PartitionProcessingException {
        this.dsl = DSL.using(dsl.configuration().derive());
        this.dsl.settings().setRenderSchema(true);
        this.dsl.settings().setParamType(ParamType.INLINED);
        this.currentSchema = getCurrentSchema(dsl);
        this.partitionIntervals = partitionIntervals;
        this.retentionSettings = new RetentionSettings(settingService, latestRetentionMinutes);
        this.retentionUpdateInterval = retentionUpdateInterval;
        this.retentionUpdateThreadPool = retentionUpdateThreadPool;
        if (dsl.dialect() == SQLDialect.POSTGRES) {
            startRetentionUpdates();
        }
    }

    private String getCurrentSchema(DSLContext dsl) throws PartitionProcessingException {
        try {
            return dsl.select(DSL.currentSchema()).fetchOne(0, String.class);
        } catch (DataAccessException e) {
            throw new PartitionProcessingException("Failed to determine current schema", e);
        }
    }

    /**
     * Ensure that the given partitioned table is under management by pg_partman postgres extension
     * and ready to receive records with a given timestamp.
     *
     * <p>If the table is not already listed in the `partman.part_config` table, then it needs to
     * initialized for pg_partman management. Either way, we must also run maintenance on the table
     * to ensure that partitions are up-to-date.</p>
     *
     * @param table            table that needs to be active
     * @param initialTimestamp snapshot_time value for the records about to be inserted
     * @throws PartitionProcessingException if there's any problem completing the operation
     */
    public void prepareForInsertion(Table<?> table, Timestamp initialTimestamp)
            throws PartitionProcessingException {
        if (dsl.dialect() != SQLDialect.POSTGRES) {
            throw new PartitionProcessingException(
                    "PartmanHelper should only be used for POSTGRES dialect");
        }
        Field<String> qualifiedTableName = DSL.concat(DSL.currentSchema(), "." + table.getName());
        if (!dsl.fetchExists(PART_CONFIG_TABLE, PARENT_TABLE_FIELD.eq(qualifiedTableName))) {
            activateTable(table, initialTimestamp, dsl);
        }
        fixPartConfig(table, dsl);
        runMaintenance(table, dsl);
    }

    /**
     * Make the given partitioned table known to pg_partman so it will start managing it. This
     * should happen the very first time a record is inserted into the table. The table will be
     * listed in the `partman.partman_config` as a side-effect of this operation.
     *
     * @param table            table to be placed under partman management
     * @param initialTimestamp timestamp of initial data, so an eligible partition will be created
     * @param dsl              for database access
     * @throws PartitionProcessingException if the operation fails for any reason
     */
    private void activateTable(Table<?> table, Timestamp initialTimestamp, DSLContext dsl)
            throws PartitionProcessingException {
        CreateParent createParent = new CreateParent();
        // note: we can't use table.getQualifiedName here; that will use the production
        // schema name (vmtdb), which won't be correct in tests.
        createParent.setParentTable(currentSchema + "." + table.getName());
        createParent.setControl(StringConstants.SNAPSHOT_TIME);
        createParent.setType(NATIVE_PARTITION_TYPE_NAME);
        createParent.setInterval(getPartitionInterval(table));
        createParent.setStartPartition(initialTimestamp.toString());
        createParent.execute(dsl.configuration());
    }

    /**
     * Create a partition interval value for the given table, based on its time frame and config
     * properties.
     *
     * <p>N.B. This is the partitioning interval - i.e. the interval represented by each
     * individual partition. It is not related to retention periods.</p>
     *
     * @param table table being configured
     * @return interval string for table's partition
     * @throws PartitionProcessingException if the operation fails
     */
    private String getPartitionInterval(Table<?> table) throws PartitionProcessingException {
        Optional<TimeFrame> tableTimeFrame = EntityType.timeFrameOfTable(table);
        if (tableTimeFrame.isPresent()) {
            return partitionIntervals.get(tableTimeFrame.get());
        } else {
            throw new PartitionProcessingException(
                    String.format("Table %s does not appear to be an entity stats table",
                            table.getName()));
        }
    }

    /**
     * Update the `partman.part_config` table with any settings that cannot be specified as part of
     * the `create_parent` function invocation, and where the default value is not the one we want.
     *
     * <p>Currently, the following settings are updated</p>
     *
     * <dl>
     *     <dt>`inifiite_time_partitions` = `true`</dt>
     *     <dd>This ensures that there will be no partitioning gaps up to current time. This is
     *     necessary because we always insert records with timestamps in the past (copied from
     *     topology creation timestamp). Without this we could end up attempting to insert into a
     *     gap.</dd>
     *     <dt>`retention_keep_tables` = `false`</dt>
     *     <dd>This prevents a build-up of expired partitions that we would have to clean up by
     *     other means. Dropping the partition tables when they expire is in line with our
     *     implementation in MariaDB.</dd>
     * </dl>
     *
     * @param table table whose settings need to be updated
     * @param dsl   database access
     * @throws PartitionProcessingException if there's a problem with this operation
     */
    private void fixPartConfig(Table<?> table, DSLContext dsl) throws PartitionProcessingException {
        Field<String> qualifiedName = DSL.currentSchema().concat("." + table.getName());
        try {
            dsl.update(PART_CONFIG_TABLE)
                    .set(INFINITE_TIME_PARTITIONS_FIELD, true)
                    .set(RETENTION_KEEP_TABLE_FIELD, false)
                    .where(PARENT_TABLE_FIELD.eq(qualifiedName))
                    .execute();
        } catch (DataAccessException e) {
            throw new PartitionProcessingException("Failed to update partition config table", e);
        }
    }

    /**
     * Run the `pg_partman` maintenance function globally. This ensures that all tables have all
     * their required partitions, and it also drops partitions that have aged out of their retention
     * period.
     *
     * @param dsl database access
     * @throws PartitionProcessingException if there's a problem running maintenance function
     */
    private void runMaintenance(DSLContext dsl) throws PartitionProcessingException {
        try {
            new RunMaintenance().execute(dsl.configuration());
        } catch (DataAccessException e) {
            throw new PartitionProcessingException("Failed to run maintenance globally", e);
        }
    }

    /**
     * Run the `pg_partman` maintenance function for a single table. This ensures that the table has
     * all required partitions, and it also drops partitions that have aged out of the table's
     * retention period.
     *
     * @param table table to be processed
     * @param dsl   database access
     * @throws PartitionProcessingException if there's a problem running the function
     */
    private void runMaintenance(Table<?> table, DSLContext dsl)
            throws PartitionProcessingException {
        RunMaintenance runMaintenance = new RunMaintenance();
        // note: we can't use table.getQualifiedName here; that will use the production
        // schema name (vmtdb), which won't be correct in tests.
        runMaintenance.setParentTable(currentSchema + "." + table.getName());
        try {
            runMaintenance.execute(dsl.configuration());
        } catch (DataAccessException e) {
            throw new PartitionProcessingException(
                    "Failed to run maintenance for table " + table.getName(), e);
        }
    }

    /**
     * Kick off a scheduling executor that will run a {@link PartmanHelper#performRetentionUpdate()}
     * periodically.
     *
     * <p>This need not be coordinated with any other opertions, nor is there any problem with
     * down-time. At worst it will mean that expired partitions will be kept a bit longer than
     * intended.</p>
     *
     * <p>We arrange for the updates to be scheduled on whole-interval boundaries since the epoch,
     * so execution times will be predictable regardless of when the component started.</p>
     */
    private void startRetentionUpdates() {
        retentionUpdateThreadPool.scheduleAtFixedRate(
                this::performRetentionUpdate,
                timeUntilInterval(retentionUpdateInterval),
                retentionUpdateInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Compute milliseconds between now and the next whole-interval boundary of the given interval
     * based on the epoch.
     *
     * @param interval an interval
     * @return msec until next interval boundary
     */
    private static long timeUntilInterval(Duration interval) {
        long millis = interval.toMillis();
        return millis - (System.currentTimeMillis() % millis);
    }

    /**
     * Perform a retention update, consisting of retrieving current retention settings, updating the
     * `partman.part_config` table accordingly, and then running glboal maintenance.
     */
    private void performRetentionUpdate() {
        try {
            logger.info("Updating retention settings for entity-stats tables");
            retentionSettings.refresh();
            updateAllRetentionSettings();
            runMaintenance(dsl);
        } catch (Exception e) {
            // catching all exceptions so the scheduled thread pool continues to schedule executions
            logger.error("Retention update ran into difficulties", e);
        }
    }

    /**
     * Ensure that all `retention` intervals in `partman.part_config` table reflect current
     * retention settings.
     */
    private void updateAllRetentionSettings() {
        retentionSettings.describeChanges().ifPresent(diffs ->
                logger.info("Retention settings changes: [{}]", String.join("; ", diffs)));
        // update settings for each table that might be under partman management
        EntityType.allEntityTypes().stream()
                .filter(EntityType::rollsUp)
                // TODO fix EnittyType so we don't need this clumsiness
                .filter(t -> t != EntityType.named(StringConstants.CLUSTER).orElse(null))
                .forEach(t -> updateAllRetentionSettings(t, retentionSettings));
    }

    /**
     * Update retention settings for all tables associated with the given entity type.
     *
     * @param type              enitity type
     * @param retentionSettings current retention settings
     */
    private void updateAllRetentionSettings(EntityType type, RetentionSettings retentionSettings) {
        for (TimeFrame timeFrame : Arrays.asList(
                // we can't just do TimeFrame.values() because that includes the probably
                // unused value YEAR
                TimeFrame.LATEST, TimeFrame.HOUR, TimeFrame.DAY, TimeFrame.MONTH)) {
            try {
                String retentionString = retentionSettings.getRetentionString(timeFrame);
                Optional<Table<?>> table = type.getTimeFrameTable(timeFrame);
                if (table.isPresent()) {
                    updateRetentionSetting(table.get(), retentionString);
                } else {
                    throw new PartitionProcessingException(
                            String.format("No table for %s:%s", type, timeFrame));
                }
            } catch (PartitionProcessingException e) {
                logger.error("Failed to update retentions settings for {}:{}",
                        type, timeFrame, e);
            }
        }
    }

    /**
     * Update the `retention` interval for the given table if it's under `pg_partman` control.
     *
     * @param table           table whose settings are being updated
     * @param retentionString interval string to be set in that table's `retention` column
     */
    private void updateRetentionSetting(Table<?> table, String retentionString) {
        dsl.update(PART_CONFIG_TABLE)
                .set(RETENTION_FIELD, retentionString)
                .where(PARENT_TABLE_FIELD.eq(currentSchema + "." + table.getName()))
                .execute();
    }

    /**
     * Checked exception for failures that can occur in {@link PartmanHelper} methods.
     */
    public static class PartitionProcessingException extends Exception {
        /**
         * Create a new instance without message or cause.
         */
        public PartitionProcessingException() {
        }

        /**
         * Create a new instance with message and no cause.
         *
         * @param message message
         */
        public PartitionProcessingException(String message) {
            super(message);
        }

        /**
         * Create a new instance with a mesasge and a cause.
         *
         * @param message message
         * @param cause   cause
         */
        public PartitionProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Class to support invocation of pg_partman's `create_parent` function via jooQ. We don't
     * define all the parameters - just the ones we currently need.
     */
    private static class CreateParent extends AbstractRoutine<Void> {

        private static final String CREATE_PARENT_FUNCTION_NAME = "create_parent";

        private static final String PARENT_TABLE_PARM_NAME = "p_parent_table";
        public static final Parameter<String> PARENT_TABLE_PARAM = Internal.createParameter(
                PARENT_TABLE_PARM_NAME, SQLDataType.CLOB(Integer.MAX_VALUE), false, false);

        private static final String CONTROL_PARM_NAME = "p_control";
        private static final Parameter<String> CONTROL_PARM = Internal.createParameter(
                CONTROL_PARM_NAME, SQLDataType.CLOB(Integer.MAX_VALUE), false, false);

        private static final String TYPE_PARM_NAME = "p_type";
        private static final Parameter<String> TYPE_PARM = Internal.createParameter(TYPE_PARM_NAME,
                SQLDataType.CLOB(Integer.MAX_VALUE), false, false);

        private static final String INTERVAL_PARM_NAME = "p_interval";
        private static final Parameter<String> INTERVAL_PARM = Internal.createParameter(
                INTERVAL_PARM_NAME, SQLDataType.CLOB(Integer.MAX_VALUE), false, false);

        private static final String START_PARTITION_PARM_NAME = "p_start_partition";
        private static final Parameter<String> START_PARTITION_PARM = Internal.createParameter(
                START_PARTITION_PARM_NAME, SQLDataType.CLOB(Integer.MAX_VALUE), true, false);


        /**
         * Create a new instance.
         */
        CreateParent() {
            super(CREATE_PARENT_FUNCTION_NAME, PARTMAN_SCHEMA);
            // the actual function has additional parameters not needed here
            addInParameter(PARENT_TABLE_PARAM);
            addInParameter(CONTROL_PARM);
            addInParameter(TYPE_PARM);
            addInParameter(INTERVAL_PARM);
            addInParameter(START_PARTITION_PARM);
        }

        public void setParentTable(String parentTableName) {
            setValue(PARENT_TABLE_PARAM, parentTableName);
        }

        public void setControl(String control) {
            setValue(CONTROL_PARM, control);
        }

        public void setType(String type) {
            setValue(TYPE_PARM, type);
        }

        public void setInterval(String interval) {
            setValue(INTERVAL_PARM, interval);
        }

        public void setStartPartition(String startPartition) {
            setValue(START_PARTITION_PARM, startPartition);
        }
    }

    /**
     * Class to support invocation of pg_partman's `run_maintenance procedure via jOOQ. We don't
     * define all the parameters, only the onews we currently use.
     */
    private static class RunMaintenance extends AbstractRoutine<Void> {

        private static final String RUN_MEINTENANCE_FUNCTION_NAME = "run_maintenance";

        private static final String PARENT_TABLE_PARM_NAME = "p_parent_table";
        public static final Parameter<String> PARENT_TABLE_PARAM = Internal.createParameter(
                PARENT_TABLE_PARM_NAME, SQLDataType.CLOB(Integer.MAX_VALUE), true, false);

        /**
         * Create a new instance.
         */
        RunMaintenance() {
            super(RUN_MEINTENANCE_FUNCTION_NAME, PARTMAN_SCHEMA);
            // the actual function has additional parameters not needed here
            addInParameter(PARENT_TABLE_PARAM);
        }

        public void setParentTable(String parentTableName) {
            setValue(PARENT_TABLE_PARAM, parentTableName);
        }
    }
}
