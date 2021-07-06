package com.vmturbo.group.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;

/**
 * Class responsible for updating the severity for all groups.
 *
 * <p>Severity updates happen also on the following cases:
 * - After a new source topology has been announced - all groups should be updated since group
 * membership might have changed.
 * (See {@link com.vmturbo.group.pipeline.Stages.StoreSupplementaryGroupInfoStage})
 * - After a user group has been created or updated - this specific group should be updated.
 * (See {@link com.vmturbo.group.service.GroupRpcService})
 */
public class GroupSeverityUpdater {

    private static final Logger logger = LogManager.getLogger();

    private final CachingMemberCalculator memberCache;

    private final GroupSeverityCalculator severityCalculator;

    private final IGroupStore groupStore;

    private final TransactionProvider transactionProvider;

    private final int batchSize;

    private final ExecutorService executorService;

    /**
     * Constructor.
     *
     * @param memberCache group membership cache.
     * @param severityCalculator calculates severity for groups.
     * @param groupStore for database operations.
     * @param transactionProvider used to update group severities in transactions.
     * @param executorService thread pool for calculations & ingestion.
     * @param batchSize the maximum number of groups to be processed by each thread during group
     *                  severity update.
     */
    public GroupSeverityUpdater(@Nonnull final CachingMemberCalculator memberCache,
            @Nonnull final GroupSeverityCalculator severityCalculator,
            @Nonnull final IGroupStore groupStore,
            @Nonnull final TransactionProvider transactionProvider,
            @Nonnull final ExecutorService executorService,
            final int batchSize) {
        this.memberCache = memberCache;
        this.severityCalculator = severityCalculator;
        this.groupStore = groupStore;
        this.transactionProvider = transactionProvider;
        this.executorService = executorService;
        this.batchSize = batchSize;
    }

    /**
     * Updates severity for all groups.
     *
     * @return true if updating the severities of groups succeeded (either fully or partially),
     *         false if the ingestion failed for all groups.
     * @throws InterruptedException if the process is interrupted.
     */
    public boolean refreshGroupSeverities() throws InterruptedException {
        final StopWatch stopWatch = new StopWatch("Full group severity refresh");
        LongSet groupIds = memberCache.getCachedGroupIds();
        stopWatch.start("refresh");
        // split the groups into batches and assign each batch to a different thread.
        int numberOfFailedIngestions = 0;
        List<List<Long>> workloads = Lists.partition(new ArrayList<>(groupIds), this.batchSize);
        logger.trace("Splitting {} groups into {} batches.", groupIds::size, workloads::size);
        List<Future<SeverityIngestionResult>> futures = new ArrayList<>(workloads.size());
        for (List<Long> workload : workloads) {
            futures.add(executorService.submit(() -> calculateAndIngestSeverity(workload)));
        }
        SeverityIngestionResult totalResult = new SeverityIngestionResult();
        for (Future<SeverityIngestionResult> future : futures) {
            try {
                SeverityIngestionResult result = future.get();
                if (result != null) {
                    totalResult.mergeResult(result);
                } else {
                    numberOfFailedIngestions++;
                }
            } catch (ExecutionException e) {
                logger.warn("Retrieving the group severity update results from a worker thread "
                        + "failed. Error: ", e);
                numberOfFailedIngestions++;
            }
        }
        stopWatch.stop();
        logger.info(stopWatch);
        if (numberOfFailedIngestions == workloads.size()) {
            logger.warn("Group severity update failed.");
            return false;
        } else if (numberOfFailedIngestions > 0) {
            logger.warn("Group severity update succeeded only partially. {} out of {} "
                    + "batches failed.", numberOfFailedIngestions, workloads.size());
        }
        logger.info("Successfully refreshed severities for {} (out of {}) groups in {} ms.\n{}",
                totalResult.getUpdatedGroups(),
                groupIds.size(),
                stopWatch.getLastTaskTimeMillis(),
                totalResult.severitiesBreakdown());
        return true;
    }

    /**
     * Method executed by worker threads. Calculates and updates severity for the given list of
     * groups.
     *
     * @param groups the list of groups to calculate and ingest supplementary info for.
     * @return a {@link SeverityIngestionResult} object that contains the result of the worker's
     *         execution.
     * @throws InterruptedException if the thread gets interrupted
     */
    private SeverityIngestionResult calculateAndIngestSeverity(@Nonnull final List<Long> groups)
            throws InterruptedException {
        final SeverityIngestionResult result = new SeverityIngestionResult();
        Collection<GroupSupplementaryInfo> groupsToUpdate = new ArrayList<>();
        final MultiStageTimer timer = new MultiStageTimer(logger);
        timer.start("Severity calculation");
        for (long groupId : groups) {
            // get group's members
            final Set<Long> groupEntities;
            try {
                groupEntities = memberCache.getGroupMembers(groupStore,
                        Collections.singleton(groupId), true);
            } catch (RuntimeException | StoreOperationException e) {
                logger.error("Skipping severity update for group with uuid: " + groupId
                        + " due to failure to retrieve its entities. Error: ", e);
                continue;
            }
            // calculate severity based on members' severity
            Severity groupSeverity = severityCalculator.calculateSeverity(groupEntities);
            result.addSeverity(groupSeverity);
            // emptiness, environment & cloud type are ignored during severity updates, so here
            // we fill them with dummy values
            groupsToUpdate.add(new GroupSupplementaryInfo(groupId, false, 0, 0,
                    groupSeverity.getNumber()));
        }
        timer.start("Severity update");
        // update database records in a batch
        try {
            final int updatedGroups = transactionProvider.transaction(stores ->
                    stores.getGroupStore().updateBulkGroupsSeverity(groupsToUpdate));
            result.setUpdatedGroups(updatedGroups);
        } catch (StoreOperationException e) {
            if (logger.isTraceEnabled()) {
                logger.error(new ParameterizedMessage("Group severity update failed for a batch. "
                        + "List of groups in failed batch: {}", groups), e);
            } else {
                logger.error("Group severity update failed for a batch.", e);
            }
            return null;
        } finally {
            timer.stop();
        }
        timer.log(Level.DEBUG, "Group severity calculation & update worker results: ",
                Detail.STAGE_SUMMARY);
        return result;
    }

    /**
     * Class that holds the results of the execution of a single worker thread that calculates
     * and updates severity for a given list of groups.
     * It contains group counts by severity and number of updated groups.
     */
    @NotThreadSafe
    private static class SeverityIngestionResult {
        private final Map<Severity, Long> groupCountsBySeverity;
        private long updatedGroups;

        SeverityIngestionResult() {
            groupCountsBySeverity = new EnumMap<>(Severity.class);
            updatedGroups = 0;
        }

        void addSeverity(Severity severity) {
            // try to insert 1. if a value is already there, add 1 to it
            groupCountsBySeverity.merge(severity, 1L, Long::sum);
        }

        void setUpdatedGroups(long updatedGroups) {
            this.updatedGroups += updatedGroups;
        }

        long getUpdatedGroups() {
            return this.updatedGroups;
        }

        void mergeResult(SeverityIngestionResult input) {
            input.groupCountsBySeverity.forEach((severity, count) ->
                    this.groupCountsBySeverity.merge(severity, count, Long::sum));
            this.updatedGroups += input.updatedGroups;
        }

        /**
         * Creates a small summary of the update, reporting group counts by severity.
         *
         * @return the constructed string message.
         */
        public String severitiesBreakdown() {
            final StringBuilder result = new StringBuilder();
            result.append("Group counts by Severity:").append(System.lineSeparator());
            groupCountsBySeverity.forEach((severity, count) -> {
                result.append("    ")
                        .append(severity)
                        .append(" : ")
                        .append(count)
                        .append(System.lineSeparator());
            });
            return result.toString();
        }
    }
}
