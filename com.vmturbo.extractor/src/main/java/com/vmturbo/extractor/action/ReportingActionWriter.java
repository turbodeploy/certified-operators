package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.action.ActionWriter.IActionWriter;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Write action data related to reporting.
 */
class ReportingActionWriter implements IActionWriter {
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

    private final Map<Long, Record> actionSpecRecords = new HashMap<>();
    private final Map<Long, Record> actionRecords = new HashMap<>();

    /**
     * System clock.
     */
    private final Clock clock;

    /**
     * Thread pool for asynchronous writes to the database.
     */
    private final ExecutorService pool;

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

    /**
     * The minimum interval for writing action information to the database. We don't write actions
     * every broadcast, because for reporting purposes we don't need action information at 10-minute
     * intervals - especially because actions don't change as frequently as commodities.
     */
    private final long actionWritingIntervalMillis;

    ReportingActionWriter(@Nonnull final Clock clock,
            @Nonnull final ExecutorService pool,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ActionConverter actionConverter,
            @Nonnull final ActionHashManager actionHashManager,
            final long actionWritingIntervalMillis) {
        this.clock = clock;
        this.pool = pool;
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.actionConverter = actionConverter;
        this.actionHashManager = actionHashManager;
        this.actionWritingIntervalMillis = actionWritingIntervalMillis;
    }

    @Override
    public void accept(ActionOrchestratorAction aoAction) {
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

    @Override
    public void write(Map<ActionPlanInfo.TypeInfoCase, Long> lastActionWrite,
            TypeInfoCase actionPlanType, MultiStageTimer timer)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        logger.debug("Retrieved and mapped {} action specs and {} action records",
                actionSpecRecords.size(), actionRecords.size());
        // Done fetching, time to start retching.
        timer.start("Write action data for reporting");
        final long millis = clock.millis();
        final Timestamp timestamp = new Timestamp(millis);
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter actionSpecUpserter = ActionModel.ActionSpec.TABLE.open(
                     getActionSpecUpsertSink(dsl, UPSERT_CONFLICTS, UPSERT_UPDATES),
                     "Action Spec Upsert", logger);
             TableWriter actionSpecUpdater = ActionModel.ActionSpec.TABLE.open(
                     getActionSpecUpdaterSink(dsl, UPDATE_INCLUDES, UPDATE_MATCHES, UPDATE_UPDATES),
                     "Action Spec Update", logger);
             TableWriter actionInserter = ActionModel.ActionMetric.TABLE.open(
                     getActionInserterSink(dsl),
                     "Action Insert", logger);
             SnapshotManager snapshotManager = actionHashManager.open(millis)) {
            actionSpecRecords.forEach((specId, record) -> {
                final Long newHash = snapshotManager.updateRecordHash(record);
                if (newHash != null) {
                    try (Record r = actionSpecUpserter.open(record)) {
                        r.set(ActionModel.ActionSpec.HASH, newHash);
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

        // update state
        lastActionWrite.put(actionPlanType, millis);
        logger.info("Successfully wrote {} action specs and {} actions. Next update in {} minutes",
                actionSpecRecords.size(), actionRecords.size(),
                TimeUnit.MILLISECONDS.toMinutes(millis + actionWritingIntervalMillis - clock.millis()));
    }

    @VisibleForTesting
    DslRecordSink getActionInserterSink(final DSLContext dsl) {
        return new DslRecordSink(dsl, ActionModel.ActionMetric.TABLE, writerConfig, pool);
    }

    @VisibleForTesting
    DslUpdateRecordSink getActionSpecUpdaterSink(final DSLContext dsl, final List<Column<?>> updateIncludes,
            final List<Column<?>> updateMatches, final List<Column<?>> updateUpdates) {
        return new DslUpdateRecordSink(dsl, ActionModel.ActionSpec.TABLE, writerConfig, pool,
                "update", updateIncludes, updateMatches, updateUpdates);
    }

    @VisibleForTesting
    DslUpsertRecordSink getActionSpecUpsertSink(final DSLContext dsl,
            List<Column<?>> upsertConflicts, List<Column<?>> upsertUpdates) {
        return new DslUpsertRecordSink(dsl, ActionModel.ActionSpec.TABLE, writerConfig, pool, "upsert",
                upsertConflicts, upsertUpdates);
    }
}
