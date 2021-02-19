package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
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

    private final List<ActionSpec> pendingActions = new ArrayList<>();

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

    private final TypeInfoCase actionPlanType;

    private final Map<TypeInfoCase, Long> lastActionWrite;

    ReportPendingActionWriter(@Nonnull final Clock clock,
            @Nonnull final ExecutorService pool,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ActionConverter actionConverter,
            final long actionWritingIntervalMillis,
            @Nonnull final TypeInfoCase actionPlanType,
            @Nonnull final Map<TypeInfoCase, Long> lastActionWrite) {
        this.clock = clock;
        this.pool = pool;
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.actionConverter = actionConverter;
        this.actionWritingIntervalMillis = actionWritingIntervalMillis;
        this.actionPlanType = actionPlanType;
        this.lastActionWrite = lastActionWrite;
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        // Actions that are not in READY state don't go into the pending actions table.
        if (aoAction.getActionSpec().getActionState() == ActionState.READY) {
            pendingActions.add(aoAction.getActionSpec());
        }
    }

    @Override
    public void write(MultiStageTimer timer)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        timer.start("Convert actions to records");
        final List<Record> records = actionConverter.makePendingActionRecords(pendingActions);
        logger.debug("Retrieved {} action specs, mapped to {} action records.",
                pendingActions.size(), records.size());
        // Done fetching, time to start retching.
        timer.start("Write action data for reporting");
        final long millis = clock.millis();
        final Timestamp timestamp = new Timestamp(millis);
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter actionSpecReplacer = ActionModel.PendingAction.TABLE.open(
                     getPendingActionReplacerSink(dsl),
                     "Action Spec Replacer", logger)) {
            records.forEach(record -> {
                try (Record r = actionSpecReplacer.open(record)) {
                    // Nothing to change in the record.
                }
            });

            logger.debug("Finished writing {} action records", records.size());
        }
        timer.stop();

        // update state
        lastActionWrite.put(actionPlanType, millis);
        logger.info("Successfully wrote {} actions. Next update in {} minutes",
                records.size(),
                TimeUnit.MILLISECONDS.toMinutes(millis + actionWritingIntervalMillis - clock.millis()));
    }

    @VisibleForTesting
    DslReplaceRecordSink getPendingActionReplacerSink(final DSLContext dsl) {
        return new DslReplaceRecordSink(dsl, PendingAction.TABLE, writerConfig, pool, "replace");
    }
}
