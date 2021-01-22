package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Responsible for writing executed actions to the database as they are reported by the action
 * orchestrator. Unlike the other types of ingestion this does not happen on a broadcast-driven
 * schedule, but in realtime in response to {@link ActionSuccess} and {@link ActionFailure}
 * notifications from the AO.
 *
 * <p/>The concurrency model is as follows: completed action notifications are converted to the
 * database record object and put into a queue. There is a background thread that collects these
 * records from the queue and writes them through to the database. This level of indirection
 * is to better handle bursts of actions in larger environments, since there is no batching of
 * notifications at the action orchestrator level.
 */
@ThreadSafe
public class CompletedActionWriter implements ActionsListener {
    private static final Logger logger = LogManager.getLogger();

    private final ActionConverter actionConverter;

    /**
     * The queue for actions the extractor knows about.
     *
     * <p/>TODO (roman, Jan 7 2021): Should we read the action notification topic from the beginning
     * on restart for a bit of extra resiliency if the extractor component goes down without processing
     * the queue?
     */
    private final LinkedBlockingDeque<CompletedAction> recordQueue = new LinkedBlockingDeque<>();

    CompletedActionWriter(@Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final ExecutorService recordBatcherExecutor,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ExecutorService dbWriterPool,
            @Nonnull final ActionConverter actionConverter) {
        this(dbEndpoint, recordBatcherExecutor, actionConverter,
            dsl -> {
                return new DslUpsertRecordSink(dsl, ActionModel.CompletedAction.TABLE, writerConfig, dbWriterPool,
                    "upsert",
                    Arrays.asList(ActionModel.CompletedAction.ACTION_OID, ActionModel.CompletedAction.COMPLETION_TIME),
                    // We don't expect overlaps.
                    Collections.emptyList());
            });
    }

    @VisibleForTesting
    CompletedActionWriter(@Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final ExecutorService recordBatcherExecutor,
            @Nonnull final ActionConverter actionConverter,
            @Nonnull final SinkFactory sinkFactory) {
        this.actionConverter = actionConverter;
        recordBatcherExecutor.submit(
                new RecordBatchWriter(recordQueue, dbEndpoint, sinkFactory));
    }

    /**
     * Responsible for taking {@link CompletedActionWriter.CompletedAction}s off the {@link CompletedActionWriter#recordQueue}
     * and inserting them into the database in batches.
     *
     * <p/>This batch writer is a single thread, but the upsert sink produced by the
     * {@link SinkFactory} may use multiple threads to actually insert the data.
     */
    static class RecordBatchWriter implements Runnable {
        private final LinkedBlockingDeque<CompletedActionWriter.CompletedAction> recordQueue;
        private final DbEndpoint dbEndpoint;
        private final SinkFactory sinkFactory;

        private RecordBatchWriter(LinkedBlockingDeque<CompletedActionWriter.CompletedAction> recordQueue,
                DbEndpoint dbEndpoint, SinkFactory sinkFactory) {
            this.recordQueue = recordQueue;
            this.dbEndpoint = dbEndpoint;
            this.sinkFactory = sinkFactory;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    runIteration();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Executed action writer interrupted.", e);
                    break;
                } catch (UnsupportedDialectException e) {
                    logger.error("Endpoint {} configured incorrectly."
                            + " Failed to write executed actions.", dbEndpoint, e);
                }
            }
        }

        @VisibleForTesting
        void runIteration() throws InterruptedException, UnsupportedDialectException {
            final List<CompletedActionWriter.CompletedAction> completedActionsBatch = new ArrayList<>();
            // Wait for a record to become available.
            final CompletedActionWriter.CompletedAction nextRecord = recordQueue.take();
            completedActionsBatch.add(nextRecord);
            // Drain any remaining records as well.
            recordQueue.drainTo(completedActionsBatch);
            logger.debug("Processing batch of {} completed actions.", completedActionsBatch.size());
            try (DSLContext dsl = dbEndpoint.dslContext();
                 TableWriter actionSpecReplacer = ActionModel.CompletedAction.TABLE.open(
                         sinkFactory.newSink(dsl), "Action Spec Replacer", logger)) {
                completedActionsBatch.forEach(nextAction -> {
                    try (Record r = actionSpecReplacer.open(nextAction.record)) {
                        // Nothing to change in the record.
                    }
                });
            } catch (SQLException e) {
                // Put the records back into the queue.
                // TODO - timed delay.
                logger.error("Failed to record actions {} to database.", completedActionsBatch.stream()
                        .map(CompletedActionWriter.CompletedAction::getActionId)
                        .map(Object::toString)
                        .collect(Collectors.joining(",")));
            }
        }
    }

    /**
     * Callback receiving a success update for an action.
     *
     * @param actionSuccess The description of the success update.
     */
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        if (!actionSuccess.hasActionSpec()) {
            // Log and exit.
            return;
        }

        queueAction(actionSuccess.getActionId(),
                actionConverter.makeExecutedActionSpec(actionSuccess.getActionSpec(),
                        actionSuccess.getSuccessDescription()));
    }

    /**
     * Callback receiving a failure update for an action.
     *
     * @param actionFailure The description of the success update.
     */
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        if (!actionFailure.hasActionSpec()) {
            // Log and exit
            return;
        }

        queueAction(actionFailure.getActionId(),
                actionConverter.makeExecutedActionSpec(actionFailure.getActionSpec(),
                        actionFailure.getErrorDescription()));

    }

    private void queueAction(final long actionId, @Nullable final Record convertedRecord) {
        if (convertedRecord == null) {
            // Log and exit
            return;
        }

        int queueSize = recordQueue.size();
        recordQueue.add(new CompletedAction(actionId, convertedRecord));
        logger.debug("Added action {} to the queue. The queue now has {} actions.",
            actionId, queueSize + 1);
    }

    /**
     * A completed action that has not yet been recorded to the database, along with any
     * metadata we may need during processing.
     */
    private static class CompletedAction {
        private final long id;
        private final Record record;

        private CompletedAction(final long id, @Nonnull final Record record) {
            this.id = id;
            this.record = record;
        }

        public long getActionId() {
            return id;
        }

        @Nonnull
        public Record getRecord() {
            return record;
        }
    }

    /**
     * Utility interface for dependency injection in tests, to mock out the database insertion.
     */
    @FunctionalInterface
    public interface SinkFactory {

        /**
         * Create a new upsert sink.
         *
         * @param context The {@link DSLContext}.
         * @return The {@link DslUpsertRecordSink}.
         */
        @Nonnull
        DslUpsertRecordSink newSink(@Nonnull DSLContext context);
    }
}