package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.ActionWriter.IActionWriter;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.PendingAction;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Write action data related to reporting.
 */
class ReportPendingActionWriter implements IActionWriter {
    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, Record> pendingActionRecords = new HashMap<>();

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
     * The minimum interval for writing action information to the database. We don't write actions
     * every broadcast, because for reporting purposes we don't need action information at 10-minute
     * intervals - especially because actions don't change as frequently as commodities.
     */
    private final long actionWritingIntervalMillis;

    ReportPendingActionWriter(@Nonnull final Clock clock,
            @Nonnull final ExecutorService pool,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ActionConverter actionConverter,
            final long actionWritingIntervalMillis) {
        this.clock = clock;
        this.pool = pool;
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.actionConverter = actionConverter;
        this.actionWritingIntervalMillis = actionWritingIntervalMillis;
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        final ActionSpec actionSpec = aoAction.getActionSpec();
        Record actionSpecRecord = actionConverter.makePendingActionRecord(actionSpec);
        if (actionSpecRecord != null) {
            // Actions that are not in READY state don't go into the pending actions table.
            if (aoAction.getActionSpec().getActionState() == ActionState.READY) {
                pendingActionRecords.put(aoAction.getActionId(), actionSpecRecord);
            }
        }
    }

    @Override
    public void write(Map<ActionPlanInfo.TypeInfoCase, Long> lastActionWrite,
            TypeInfoCase actionPlanType, MultiStageTimer timer)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        logger.debug("Retrieved and mapped {} action records", pendingActionRecords.size());
        // Done fetching, time to start retching.
        timer.start("Write action data for reporting");
        final long millis = clock.millis();
        final Timestamp timestamp = new Timestamp(millis);
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter actionSpecReplacer = ActionModel.PendingAction.TABLE.open(
                     getPendingActionReplacerSink(dsl),
                     "Action Spec Replacer", logger)) {
            pendingActionRecords.forEach((specId, record) -> {
                try (Record r = actionSpecReplacer.open(record)) {
                    // Nothing to change in the record.
                }
            });

            logger.debug("Finished writing {} action records", pendingActionRecords.size());
        }
        timer.stop();

        // update state
        lastActionWrite.put(actionPlanType, millis);
        logger.info("Successfully wrote {} actions. Next update in {} minutes",
                pendingActionRecords.size(),
                TimeUnit.MILLISECONDS.toMinutes(millis + actionWritingIntervalMillis - clock.millis()));
    }

    @VisibleForTesting
    DslReplaceRecordSink getPendingActionReplacerSink(final DSLContext dsl) {
        return new DslReplaceRecordSink(dsl, PendingAction.TABLE, writerConfig, pool, "replace");
    }
}
