package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * The {@link ActionWriter} is responsible for listening for action updates from the action
 * orchestrator, and writing action-related data to the extractor's database at regular
 * configurable intervals.
 */
public class ActionWriter implements ActionsListener {
    private static final Logger logger = LogManager.getLogger();

    private static final List<Column<?>> UPSERT_CONFLICTS = ImmutableList.of(
            ActionModel.ActionSpec.SPEC_OID, ActionModel.ActionSpec.HASH);
    private static final List<Column<?>> UPSERT_UPDATES =
            Collections.singletonList(ActionModel.ActionSpec.LAST_SEEN);

    private static final List<Column<?>> UPDATE_INCLUDES = ImmutableList.of(
            ActionModel.ActionSpec.HASH, ActionModel.ActionSpec.LAST_SEEN);
    private static final List<Column<?>> UPDATE_MATCHES = ImmutableList.of(
            ActionModel.ActionSpec.HASH);
    private static final List<Column<?>> UPDATE_UPDATES = ImmutableList.of(
            ActionModel.ActionSpec.LAST_SEEN);

    /**
     * System clock.
     */
    private final Clock clock;

    /**
     * Threadpool for asynchronous writes to the database.
     */
    private final ExecutorService pool;

    private final ActionsServiceBlockingStub actionService;

    /**
     * Endpoint to connect to the database.
     */
    private final DbEndpoint dbEndpoint;

    /**
     * Shared database writing configuration (e.g. how long to wait for transactions to complete).
     */
    private final WriterConfig writerConfig;

    /**
     * Converts action orchestrator actions to database {@link Record}s.
     */
    private final ActionConverter actionConverter;

    /**
     * Responsible for assigning hashes to action specs to reduce unnecessary writes to the
     * database.
     */
    private final ActionHashManager actionHashManager;

    private final long realtimeContextId;

    /**
     * The minimum interval for writing action information to the database. We don't write actions
     * every broadcast, because for reporting purposes we don't need action information at 10-minute
     * intervals - especially because actions don't change as frequently as commodities.
     */
    private final long actionWritingIntervalMillis;

    private final boolean enableIngestion;

    /**
     * The last time we wrote actions for each action plan type. It's necessary to split up by
     * type, because otherwise a BuyRI action plan will prevent a follow-up Market action plan
     * from being processed.
     */
    private final Map<ActionPlanInfo.TypeInfoCase, Long> lastActionWrite =
            new EnumMap<>(TypeInfoCase.class);

    ActionWriter(@Nonnull final Clock clock,
            @Nonnull final ExecutorService pool,
            @Nonnull final ActionsServiceBlockingStub actionService,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ActionConverter actionConverter,
            @Nonnull final ActionHashManager actionHashManager,
            final long actionWritingInterval,
            final TimeUnit actionWritingIntervalUnit,
            final boolean enableIngestion,
            final long realtimeContextId) {
        this.clock = clock;
        this.pool = pool;
        this.actionService = actionService;
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.actionConverter = actionConverter;
        this.actionHashManager = actionHashManager;
        this.actionWritingIntervalMillis = actionWritingIntervalUnit.toMillis(actionWritingInterval);
        this.realtimeContextId = realtimeContextId;
        this.enableIngestion = enableIngestion;
        logger.info("Initialized action writer. Ingestion {}", enableIngestion ? "enabled" : "disable");
    }

    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        final long contextId = ActionDTOUtil.getActionPlanContextId(actionsUpdated.getActionPlanInfo());
        if (contextId != realtimeContextId) {
            // Skip non-realtime action plans.
            return;
        }

        if (!enableIngestion) {
            logger.debug("Ingestion disabled, skipping action plan for context {}", contextId);
            return;
        }

        TypeInfoCase actionPlanType = actionsUpdated.getActionPlanInfo().getTypeInfoCase();
        long lastWriteForType = lastActionWrite.computeIfAbsent(actionPlanType, k -> 0L);
        final long now = clock.millis();
        final long nextUpdateTime = lastWriteForType + actionWritingIntervalMillis;
        if (nextUpdateTime <= now) {
            try {
                writeActions();
                lastActionWrite.put(actionPlanType, now);
                logger.info("Successfully wrote actions metrics. Next update in {} minutes",
                        TimeUnit.MILLISECONDS.toMinutes(now + actionWritingIntervalMillis - clock.millis()));
            } catch (StatusRuntimeException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("grpc_" + e.getStatus().getCode()).increment();
                logger.error("Failed to retrieve actions.", e);
            } catch (UnsupportedDialectException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("dialect").increment();
                // This shouldn't happen.
                logger.error("Failed to write action data due to invalid dialect.", e);
            } catch (SQLException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("sql_" + e.getErrorCode()).increment();
                logger.error("Failed to write action data due to SQL error.", e);
            } catch (InterruptedException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("interrupted").increment();
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for action data to write.", e);
            }
        } else {
            logger.info("Not writing action metrics for another {} minutes.",
                TimeUnit.MILLISECONDS.toMinutes(nextUpdateTime - now));
        }
    }

    @VisibleForTesting
    void writeActions()
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final MultiStageTimer timer = new MultiStageTimer(logger);
        final AsyncTimer elapsedTimer = timer.async("Total Elapsed");
        timer.stop();

        final Map<Long, Record> actionSpecRecords = new HashMap<>();
        final Map<Long, Record> actionRecords = new HashMap<>();
        timer.start("retrieval");
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(realtimeContextId)
                // stream all actions
                .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                .build();
        logger.debug("Retrieving actions for context {}", realtimeContextId);
        actionService.getAllActions(actionRequest).forEachRemaining(response -> {
            response.getActionChunk().getActionsList().forEach(aoAction -> {
                if (aoAction.hasActionSpec()) {
                    final ActionSpec actionSpec = aoAction.getActionSpec();
                    Record actionSpecRecord = actionConverter.makeActionSpecRecord(actionSpec);
                    if (actionSpecRecord != null) {
                        Record actionRecord = actionConverter.makeActionRecord(actionSpec,
                                actionSpecRecord);
                        final long specId = actionSpecRecord.get(ActionModel.ActionSpec.SPEC_OID);
                        actionSpecRecords.put(specId, actionSpecRecord);
                        actionRecords.put(specId, actionRecord);
                    }
                }
            });
        });
        timer.stop();
        logger.debug("Retrieved and mapped {} action specs and {} action records",
                actionSpecRecords.size(), actionRecords.size());

        // Done fetching, time to start retching.
        timer.start("processing");
        final long millis = clock.millis();
        final Timestamp timestamp = new Timestamp(millis);
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter actionSpecUpserter = ActionModel.ActionSpec.TABLE.open(
                getActionSpecUpsertSink(dsl, UPSERT_CONFLICTS, UPSERT_UPDATES));
             TableWriter actionSpecUpdater = ActionModel.ActionSpec.TABLE.open(
                getActionSpecUpdaterSink(dsl, UPDATE_INCLUDES, UPDATE_MATCHES, UPDATE_UPDATES));
             TableWriter actionInserter = ActionModel.ActionMetric.TABLE.open(
                     getActionInserterSink(dsl));
             SnapshotManager snapshotManager = actionHashManager.open(millis)) {
            actionSpecRecords.forEach((specId, record) -> {
                final Long newHash = snapshotManager.updateRecordHash(record);
                if (newHash != null) {
                    try (Record r = actionSpecUpserter.open(record)) {
                        r.set(ActionModel.ActionMetric.ACTION_SPEC_HASH, newHash);
                        snapshotManager.setRecordTimes(r);
                    }
                }
            });

            logger.debug("Finished writing {} action spec records", actionSpecRecords.size());

            actionRecords.forEach((specId, record) -> {
                try (Record r = actionInserter.open(record)) {
                    r.set(ActionModel.ActionMetric.TIME, timestamp);
                    r.set(ActionModel.ActionMetric.ACTION_SPEC_HASH,
                            actionHashManager.getEntityHash(specId));
                }
            });

            logger.debug("Finished writing {} action records", actionRecords.size());

            snapshotManager.processChanges(actionSpecUpdater);
        }

        timer.stop();

        elapsedTimer.close();
        timer.visit((stageName, stopped, totalDurationMs) -> {
            if (stopped) {
                Metrics.PROCESSING_HISTOGRAM.labels(stageName)
                    .observe((double)TimeUnit.MILLISECONDS.toSeconds(totalDurationMs));
            }
        });
        timer.info(FormattedString.format("Processed {} action specs and {} actions",
                actionSpecRecords.size(), actionRecords.size()), Detail.STAGE_SUMMARY);
    }

    @Nonnull
    DslRecordSink getActionInserterSink(final DSLContext dsl) {
        return new DslRecordSink(dsl, ActionModel.ActionMetric.TABLE, writerConfig, pool);
    }

    @Nonnull
    DslUpdateRecordSink getActionSpecUpdaterSink(final DSLContext dsl, final List<Column<?>> updateIncludes,
            final List<Column<?>> updateMatches, final List<Column<?>> updateUpdates) {
        return new DslUpdateRecordSink(dsl, ActionModel.ActionSpec.TABLE, writerConfig, pool,
                "update", updateIncludes, updateMatches, updateUpdates);
    }

    @Nonnull
    DslUpsertRecordSink getActionSpecUpsertSink(final DSLContext dsl,
            List<Column<?>> upsertConflicts, List<Column<?>> upsertUpdates) {
        return new DslUpsertRecordSink(dsl, ActionModel.ActionSpec.TABLE, writerConfig, pool, "upsert",
                upsertConflicts, upsertUpdates);
    }

    /**
     * Metrics relevant for action processing.
     */
    private static class Metrics {

        private Metrics() {}

        private static final DataMetricHistogram PROCESSING_HISTOGRAM =
            DataMetricHistogram.builder()
                .withName("xtr_action_processing_duration_seconds")
                .withHelp("Duration of action processing (fetching data and writing)" + " broken down by stage. Only for cycles that fully processed without errors.")
                .withLabelNames("stage")
                // 10s, 1min, 3min, 7min, 10min, 15min, 30min
                // The SLA is to process everything in under 10 minutes.
                .withBuckets(10, 60, 180, 420, 600, 900, 1800)
                .build()
                .register();

        private static final DataMetricCounter PROCESSING_ERRORS_CNT =
            DataMetricCounter.builder()
                .withName("xtr_action_processing_error_count")
                .withHelp("Errors encountered during action processing, broken down by high level type.")
                .withLabelNames("type")
                .build()
                .register();
    }
}
