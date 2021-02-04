package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

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
    private final DataProvider dataProvider;
    private final ExtractorFeatureFlags featureFlags;
    private final DataExtractionFactory dataExtractionFactory;

    /**
     * The queue for actions the extractor knows about.
     *
     * <p/>TODO (roman, Jan 7 2021): Should we read the action notification topic from the beginning
     * on restart for a bit of extra resiliency if the extractor component goes down without processing
     * the queue?
     */
    private final LinkedBlockingDeque<CompletedAction> recordQueue = new LinkedBlockingDeque<>();
    private final ActionWriterFactory actionWriterFactory;

    CompletedActionWriter(@Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final ExecutorService recordBatcherExecutor,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final ExecutorService dbWriterPool,
            @Nonnull final ActionConverter actionConverter,
            @Nonnull final DataProvider dataProvider,
            @Nonnull final ExtractorFeatureFlags featureFlags,
            @Nonnull final ExtractorKafkaSender extractorKafkaSender,
            @Nonnull final DataExtractionFactory dataExtractionFactory,
            @Nonnull final ActionWriterFactory actionWriterFactory,
            @Nonnull final Clock clock) {
        this(dbEndpoint, recordBatcherExecutor, actionConverter, dataProvider, featureFlags,
            dsl -> {
                return new DslUpsertRecordSink(dsl, ActionModel.CompletedAction.TABLE, writerConfig, dbWriterPool,
                    "upsert",
                    Arrays.asList(ActionModel.CompletedAction.ACTION_OID, ActionModel.CompletedAction.COMPLETION_TIME),
                    // We don't expect overlaps.
                    Collections.emptyList());
            }, extractorKafkaSender, dataExtractionFactory, actionWriterFactory, clock);
    }

    @VisibleForTesting
    CompletedActionWriter(@Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final ExecutorService recordBatcherExecutor,
            @Nonnull final ActionConverter actionConverter,
            @Nonnull final DataProvider dataProvider,
            @Nonnull final ExtractorFeatureFlags featureFlags,
            @Nonnull final SinkFactory sinkFactory,
            @Nonnull final ExtractorKafkaSender extractorKafkaSender,
            @Nonnull final DataExtractionFactory dataExtractionFactory,
            @Nonnull final ActionWriterFactory actionWriterFactory,
            @Nonnull final Clock clock) {
        this.actionConverter = actionConverter;
        this.dataProvider = dataProvider;
        this.featureFlags = featureFlags;
        this.dataExtractionFactory = dataExtractionFactory;
        this.actionWriterFactory = actionWriterFactory;
        recordBatcherExecutor.submit(
                new RecordBatchWriter(recordQueue, dbEndpoint, sinkFactory, extractorKafkaSender, clock));
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
        private final ExtractorKafkaSender extractorKafkaSender;
        private final Clock clock;

        private RecordBatchWriter(LinkedBlockingDeque<CompletedAction> recordQueue,
                DbEndpoint dbEndpoint, SinkFactory sinkFactory,
                ExtractorKafkaSender extractorKafkaSender, Clock clock) {
            this.recordQueue = recordQueue;
            this.dbEndpoint = dbEndpoint;
            this.sinkFactory = sinkFactory;
            this.extractorKafkaSender = extractorKafkaSender;
            this.clock = clock;
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
                }
            }
        }

        @VisibleForTesting
        void runIteration() throws InterruptedException {
            final List<CompletedActionWriter.CompletedAction> completedActionsBatch = new ArrayList<>();
            // Wait for a record to become available.
            final CompletedActionWriter.CompletedAction nextRecord = recordQueue.take();
            completedActionsBatch.add(nextRecord);
            // Drain any remaining records as well.
            recordQueue.drainTo(completedActionsBatch);
            logger.debug("Processing batch of {} completed actions.", completedActionsBatch.size());
            exportActionBatch(completedActionsBatch);
            recordActionBatchForReporting(completedActionsBatch);
        }

        private void recordActionBatchForReporting(List<CompletedActionWriter.CompletedAction> batch)
                throws InterruptedException {
            final List<Record> executedActionRecords = batch.stream()
                    .map(CompletedAction::getRecord)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (executedActionRecords.size() != batch.size()) {
                logger.debug("Recording {} of {} actions from batch for reporting."
                    + "Either reporting is not enabled, or some actions did not pass validation.", executedActionRecords.size(), batch.size());
            }
            if (!executedActionRecords.isEmpty()) {
                try (DSLContext dsl = dbEndpoint.dslContext(); TableWriter actionSpecReplacer = ActionModel.CompletedAction.TABLE.open(
                        sinkFactory.newSink(dsl), "Action Spec Replacer", logger)) {
                    batch.forEach(nextAction -> {
                        try (Record r = actionSpecReplacer.open(nextAction.record)) {
                            // Nothing to change in the record.
                        }
                    });
                } catch (SQLException e) {
                    // TODO - Consider putting the records back into the queue with a timed delay.
                    logger.error("Failed to record actions {} to database.", batch.stream()
                            .map(CompletedActionWriter.CompletedAction::getActionId)
                            .map(Object::toString)
                            .collect(Collectors.joining(",")));
                } catch (UnsupportedDialectException e) {
                    logger.error("Endpoint {} configured incorrectly." + " Failed to write executed actions.", dbEndpoint, e);
                }
            }
        }

        /**
         * Collect all exported actions and send to Kafka.
         *
         * @param batch may contain actions that need be sent to Kafka
         */
        private void exportActionBatch(List<CompletedActionWriter.CompletedAction> batch) {
            final String timestamp = ExportUtils.getFormattedDate(clock.millis());
            final List<ExportedObject> exportedActions = batch.stream()
                    .map(CompletedAction::getExportedAction)
                    .filter(Objects::nonNull)
                    .map(action -> {
                        ExportedObject exportedObject = new ExportedObject();
                        exportedObject.setTimestamp(timestamp);
                        exportedObject.setAction(action);
                        return exportedObject;
                    }).collect(Collectors.toList());
            if (!exportedActions.isEmpty()) {
                int count = extractorKafkaSender.send(exportedActions);
                logger.info("Sent {} executed actions to Kafka", count);
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

        final TopologyGraph<SupplyChainEntity> graph = dataProvider.getTopologyGraph();
        if (graph == null) {
            logger.error("No topology graph available when processing action success for action {} (id: {}).",
                actionSuccess.getActionSpec().getDescription(), actionSuccess.getActionId());
        }

        final CompletedAction action = new CompletedAction(actionSuccess.getActionId());
        if (featureFlags.isReportingEnabled()) {
            action.setReportingRecord(actionConverter.makeExecutedActionSpec(actionSuccess.getActionSpec(),
                    actionSuccess.getSuccessDescription(), graph));
        }
        if (featureFlags.isExtractionEnabled()) {
            action.setExportedAction(actionConverter.makeExportedAction(
                    actionSuccess.getActionSpec(), graph, actionWriterFactory.getOrFetchPolicies(),
                    dataExtractionFactory.newRelatedEntitiesExtractor(dataProvider)));
        }

        queueAction(actionSuccess.getActionId(), action);
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

        final TopologyGraph<SupplyChainEntity> graph = dataProvider.getTopologyGraph();
        if (graph == null) {
            logger.error("No topology graph available when processing action failure for action {} (id: {}).",
                    actionFailure.getActionSpec().getDescription(), actionFailure.getActionId());
        }

        final CompletedAction action = new CompletedAction(actionFailure.getActionId());
        if (featureFlags.isReportingEnabled()) {
            action.setReportingRecord(actionConverter.makeExecutedActionSpec(actionFailure.getActionSpec(),
                    actionFailure.getErrorDescription(), graph));
        }
        if (featureFlags.isExtractionEnabled()) {
            action.setExportedAction(actionConverter.makeExportedAction(
                    actionFailure.getActionSpec(), graph, actionWriterFactory.getOrFetchPolicies(),
                    dataExtractionFactory.newRelatedEntitiesExtractor(dataProvider)));
        }

        queueAction(actionFailure.getActionId(), action);
    }

    private void queueAction(final long actionId, @Nonnull final CompletedAction completedAction) {
        int queueSize = recordQueue.size();
        recordQueue.add(completedAction);
        logger.debug("Added action {} to the queue. The queue now has {} actions.",
            actionId, queueSize + 1);
    }

    /**
     * A completed action that has not yet been recorded to the database, along with any
     * metadata we may need during processing.
     */
    private static class CompletedAction {
        private final long id;
        private Record record = null;
        private Action action = null;

        private CompletedAction(final long id) {
            this.id = id;
        }

        public long getActionId() {
            return id;
        }

        public void setReportingRecord(Record record) {
            this.record = record;
        }

        @Nullable
        public Record getRecord() {
            return record;
        }

        @Nullable
        public Action getExportedAction() {
            return action;
        }

        public void setExportedAction(Action action) {
            this.action = action;
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