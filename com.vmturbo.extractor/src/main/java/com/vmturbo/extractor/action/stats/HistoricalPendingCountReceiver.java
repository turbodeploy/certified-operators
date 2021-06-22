package com.vmturbo.extractor.action.stats;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.api.export.ActionRollupExport;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.HourlyActionStat;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.action.ActionConverter;
import com.vmturbo.extractor.models.ActionStatsModel;
import com.vmturbo.extractor.models.ActionStatsModel.ActionGroup;
import com.vmturbo.extractor.models.ActionStatsModel.PendingActionStats;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionMode;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Receives action stat rollup notifications from the action orchestrator and writes them to
 * the database.
 */
public class HistoricalPendingCountReceiver implements Consumer<ActionRollupNotification> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Feature flags determining which extractor features are enabled.
     */
    private final ExtractorFeatureFlags extractorFeatureFlags;

    /**
     * Endpoint to connect to the database.
     */
    private final DbEndpoint dbEndpoint;

    /**
     * Thread pool for asynchronous writes to the database.
     */
    private final ExecutorService pool;

    /**
     * Shared database writing configuration (e.g. how long to wait for transactions to complete).
     */
    private final WriterConfig writerConfig;

    /**
     * Create a HistoricalPendingCountReceiver for recording pending action stats for reporting.
     *
     * @param extractorFeatureFlags feature flags determining which extractor features are enabled.
     * @param dbEndpoint endpoint to connect to the database.
     * @param pool thread pool for asynchronous writes to the database.
     * @param writerConfig shared database writing configuration.
     */
    public HistoricalPendingCountReceiver(ExtractorFeatureFlags extractorFeatureFlags,
            DbEndpoint dbEndpoint,
            ExecutorService pool,
            WriterConfig writerConfig) {
        this.extractorFeatureFlags = extractorFeatureFlags;
        this.dbEndpoint = dbEndpoint;
        this.pool = pool;
        this.writerConfig = writerConfig;
    }

    @Override
    public void accept(final ActionRollupNotification rollupNotification) {
        logger.info("Received historical count: {}", rollupNotification);

        if (extractorFeatureFlags.isReportingActionIngestionEnabled()) {
            processPendingActions(rollupNotification);
        } else {
            logger.info("Reporting feature is disabled; skipping processing pending actions stats.");
        }
    }

    private void processPendingActions(ActionRollupNotification rollupNotification) {
        // Process action group records
        List<Record> actionGroupRecords =
                makeActionGroupRecords(rollupNotification.getActionGroupsList());
        writeRecords(actionGroupRecords,
                ActionGroup.TABLE,
                ImmutableList.of(ActionGroup.ID));
        // Process hourly actions stats records
        List<Record> records =
                makePendingActionStatsRecords(rollupNotification.getHourlyActionStatsList());
        writeRecords(records,
                PendingActionStats.TABLE,
                ImmutableList.of(PendingActionStats.SCOPE_OID, PendingActionStats.ENTITY_TYPE,
                        PendingActionStats.ENVIRONMENT_TYPE, PendingActionStats.ACTION_GROUP,
                        PendingActionStats.TIME));
    }

    /**
     * Create records for the pending action stats table from a list of {@link HourlyActionStat}.
     *
     * @param actionStats the hourly action stats contained rolled up action data
     * @return The database {@link Record}s the action stats map to, arranged by id.
     */
    @Nonnull
    private List<Record> makePendingActionStatsRecords(final List<HourlyActionStat> actionStats) {
        final List<Record> retList = new ArrayList<>(actionStats.size());
        actionStats.forEach(hourlyActionStat -> {
            final Record record = new Record(ActionStatsModel.PendingActionStats.TABLE);
            record.set(PendingActionStats.TIME, new Timestamp(hourlyActionStat.getHourTime()));
            record.set(PendingActionStats.SCOPE_OID, hourlyActionStat.getScopeOid());
            if (hourlyActionStat.hasScopeEntityType()) {
                final EntityType entityType = EntityTypeUtils.protoIntToDb(
                        hourlyActionStat.getScopeEntityType(), null);
                record.set(PendingActionStats.ENTITY_TYPE, entityType);
            } else {
                record.set(PendingActionStats.ENTITY_TYPE, EntityType._NONE_);
            }
            final EnvironmentType environmentType = EnvironmentTypeUtils.protoToDb(
                    hourlyActionStat.getScopeEnvironmentType());
            record.set(PendingActionStats.ENVIRONMENT_TYPE, environmentType);
            record.set(PendingActionStats.ACTION_GROUP, hourlyActionStat.getActionGroupId());
            record.set(PendingActionStats.PRIOR_ACTION_COUNT, hourlyActionStat.getPriorActionCount());
            record.set(PendingActionStats.CLEARED_ACTION_COUNT, hourlyActionStat.getClearedActionCount());
            record.set(PendingActionStats.NEW_ACTION_COUNT, hourlyActionStat.getNewActionCount());
            record.set(PendingActionStats.INVOLVED_ENTITY_COUNT, hourlyActionStat.getEntityCount());
            record.set(PendingActionStats.SAVINGS, hourlyActionStat.getSavings());
            record.set(PendingActionStats.INVESTMENTS, hourlyActionStat.getInvestments());
            retList.add(record);
        });
        return retList;
    }

    /**
     * Create records for the action groups table from a list of {@link ActionGroup}.
     *
     * @param actionGroups the action groups referenced in the rolled up action data.
     * @return The database {@link Record}s the action stats map to, arranged by id.
     */
    @Nonnull
    private List<Record> makeActionGroupRecords(final List<ActionRollupExport.ActionGroup> actionGroups) {
        final List<Record> retList = new ArrayList<>(actionGroups.size());
        actionGroups.forEach(actionGroup -> {
            final Record record = new Record(ActionStatsModel.ActionGroup.TABLE);
            record.set(ActionGroup.ID, actionGroup.getId());
            final ActionType actionType =
                    ActionConverter.extractActionType(actionGroup.getActionType());
            record.set(ActionGroup.TYPE, actionType);
            final ActionCategory category =
                    ActionConverter.extractActionCategory(actionGroup.getActionCategory());
            record.set(ActionGroup.CATEGORY, category);
            final ActionState state =
                    ActionConverter.extractActionState(actionGroup.getActionState());
            record.set(ActionGroup.STATE, state);
            final ActionMode mode =
                    ActionConverter.extractActionMode(actionGroup.getActionMode());
            record.set(ActionGroup.MODE, mode);

            final String[] risks = actionGroup.getRiskDescriptionsList().stream()
                .distinct()
                .toArray(String[]::new);
            record.set(ActionGroup.RISKS, risks);
            retList.add(record);
        });
        return retList;
    }

    private void writeRecords(final List<Record> records, final Table table,
            List<Column<?>> upsertConflicts) {
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter recordUpserter =
                     table.open(getUpsertSink(dsl, table, upsertConflicts, Collections.emptyList()),
                             "Action Stats Upserter",
                             logger)) {
            records.forEach(record -> {
                try (Record r = recordUpserter.open(record)) {
                    // Nothing to change in the record.
                }
            });

            logger.debug("Finished writing {} action stats records", records.size());
        } catch (UnsupportedDialectException e) {
            //Metrics.PROCESSING_ERRORS_CNT.labels("dialect").increment();
            // This shouldn't happen.
            logger.error("{} failed to write action data due to invalid dialect.",
                    getClass().getSimpleName(), e);
        } catch (SQLException e) {
            //Metrics.PROCESSING_ERRORS_CNT.labels("sql_" + e.getErrorCode()).increment();
            logger.error("{} failed to write action data due to SQL error.",
                    getClass().getSimpleName(), e);
        } catch (InterruptedException e) {
            //Metrics.PROCESSING_ERRORS_CNT.labels("interrupted").increment();
            Thread.currentThread().interrupt();
            logger.error("{} interrupted while waiting for action data to write.",
                    getClass().getSimpleName(), e);
        }
    }

    @VisibleForTesting
    DslUpsertRecordSink getUpsertSink(final DSLContext dsl,
            final Table table,
            final List<Column<?>> upsertConflicts,
            final List<Column<?>> upsertUpdates) {
        return new DslUpsertRecordSink(dsl, table, writerConfig, pool, "upsert", upsertConflicts, upsertUpdates);
    }
}
