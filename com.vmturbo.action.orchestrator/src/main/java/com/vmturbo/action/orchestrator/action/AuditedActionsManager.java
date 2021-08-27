package com.vmturbo.action.orchestrator.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Manager responsible for book keeping operations with audited actions.
 */
public class AuditedActionsManager {

    // TODO(OM-66069): clean actions if target discovered workflow was removed
    /**
     * Persist in memory actions which were already sent for audit (raw - actionOID, column -
     * workflowId, value - cleared time).
     * Action can be send for audit to several targets (defines by discovered workflow).
     */
    private final Table<Long, Long, AuditedActionInfo> sentActionsToWorkflowCache;

    /**
     * Queue containing new audited actions that we need to write into DB or CLEARED actions for
     * which we need to persist cleared_timestamp.
     */
    private final BlockingDeque<AuditedActionsUpdate> auditedActionsUpdateBatches;

    private final AuditActionsPersistenceManager auditActionsPersistenceManager;

    private final ScheduledExecutorService scheduledExecutorService;

    private final long minsRecoveryInterval;

    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor of {@link AuditedActionsManager}.
     *
     * @param auditActionsPersistenceManager DAO for CRUD operations on audited actions.
     * @param scheduledExecutorService executor service used for background sync up in-memory and
     * DB bookkeeping
     * @param minsRecoveryInterval in the event of failure, how long until we try again.
     */
    public AuditedActionsManager(
            @Nonnull final AuditActionsPersistenceManager auditActionsPersistenceManager,
            @Nonnull final ScheduledExecutorService scheduledExecutorService,
            final long minsRecoveryInterval) {
        this(
            auditActionsPersistenceManager,
            scheduledExecutorService,
            minsRecoveryInterval,
            new LinkedBlockingDeque<>());
    }

    @VisibleForTesting
    AuditedActionsManager(
            @Nonnull final AuditActionsPersistenceManager auditActionsPersistenceManager,
            @Nonnull final ScheduledExecutorService scheduledExecutorService,
            final long minsRecoveryInterval,
            BlockingDeque<AuditedActionsUpdate> auditedActionsUpdateBatches) {
        this.auditActionsPersistenceManager = Objects.requireNonNull(auditActionsPersistenceManager);
        this.scheduledExecutorService = scheduledExecutorService;
        this.minsRecoveryInterval = minsRecoveryInterval;
        this.auditedActionsUpdateBatches = auditedActionsUpdateBatches;
        this.sentActionsToWorkflowCache = restoreAuditedActionsFromDB();
        scheduledExecutorService.submit(this::updateDbBookkeeping);
    }

    private void updateDbBookkeeping() {
        try {
            updateDbBookkeepingInner();
        } catch (Exception e) {
            // We must record an exception for debugging. The default executor services die without
            // a trace without explicitly catching and printing the uncaught exception.
            logger.error("uncaught exception killed book keeping database processing", e);
        }
    }

    private void updateDbBookkeepingInner() {
        // As long as there are no database issues, we immediately wait for the next batch.
        final AuditedActionsUpdate auditedActionsUpdatesBatch;
        try {
            auditedActionsUpdatesBatch = auditedActionsUpdateBatches.take();
        } catch (InterruptedException e) {
            //     There might still be items in the queue when we got interrupted. To make sure
            // we don't lose data, we drain the queue and handle all the batches. For instance,
            // the interrupted could occur inbetween when we finished handling the latest batch
            // of actions from the market, but just before this runnable woke up from the
            // blocking .take() call.
            //     It handles some cases assuming that by catching interrupted exception,
            // blocking on a database connection will not be interrupted and that the JVM stays alive long enough to write to the database.
            logger.info("Operation of updating DB bookkeeping is interrupted because "
                    + "application is shutting down. This is our final attempt to drain the queue before shutting"
                    + " down.");
            final List<AuditedActionsUpdate> batchesToUpdate = new ArrayList<>();
            auditedActionsUpdateBatches.drainTo(batchesToUpdate);
            try {
                for (AuditedActionsUpdate batch : batchesToUpdate) {
                    handleBatch(batch);
                }
            } catch (ActionStoreOperationException ex) {
                logger.error("We tried our best to drain the queue when shutting down"
                        + " but could not contact the database. As a result "
                        + batchesToUpdate.size() + " batches could not be persisted", ex);
            }
            // We must break so that the application can shutdown. Without breaking, the
            // application will wait on this thread forever and the application process will have to be
            // killed with a dangerous interrupted level.
            return;
        }

        try {
            handleBatch(auditedActionsUpdatesBatch);
            // batch was handled successfully, we can immediately start the next batch if there is one
            // or block and wait for the next one.
            scheduledExecutorService.submit(this::updateDbBookkeeping);
        } catch (ActionStoreOperationException e) {
            // TODO(OM-66325): For now it's expected that it is safe to keep this unbounded because:
            // 1. Action store errors occuring often is a much larger problem that needs to be resolved.
            //    As a result, data store errors should not be frequent.
            // 2. Assuming frequent errors, there is at most one message add every 10 minutes.
            auditedActionsUpdateBatches.addFirst(auditedActionsUpdatesBatch);
            logger.error("We could not communicate with the database."
                    + " We added the work to the front of the queue."
                    + " When the scheduler schedules this task again we will try again."
                    + " There are " + auditedActionsUpdateBatches.size()
                    + " batches still in the queue.", e);
            // It's safe to break because this Runnable is scheduled with a fixed delay.
            // The delay is the retry interval. This way we avoid Thread.sleep() which is
            // difficult to unit test.
            scheduledExecutorService.schedule(this::updateDbBookkeeping, minsRecoveryInterval, TimeUnit.MINUTES);
        }
    }

    private void handleBatch(@Nonnull final AuditedActionsUpdate batch)
            throws ActionStoreOperationException {
        logger.trace("Persisting batch to data store: {}", batch);
        final Collection<AuditedActionInfo> combinedUpdates =
                new ArrayList<>(batch.addedAuditedActionInfos);
        combinedUpdates.addAll(batch.recentlyClearedAuditedActionInfos);
        if (!combinedUpdates.isEmpty()) {
            auditActionsPersistenceManager.persistActions(combinedUpdates);
        } else {
            logger.trace("No actions to persist.");
        }
        if (!batch.removedAuditedActionInfos.isEmpty()) {
            auditActionsPersistenceManager.removeActionWorkflows(batch.removedAuditedActionInfos.stream()
                    .map(auditedActionInfo -> new Pair<>(auditedActionInfo.getRecommendationId(),
                            auditedActionInfo.getWorkflowId()))
                    .collect(Collectors.toList()));
        } else {
            logger.trace("No actions to remove.");
        }
        if (!batch.removedActionRecommendationOids.isEmpty()) {
            auditActionsPersistenceManager.removeActionsByRecommendationOid(batch.removedActionRecommendationOids);
        }
    }

    private Table<Long, Long, AuditedActionInfo> restoreAuditedActionsFromDB() {
        final StopWatch stopWatch = StopWatch.createStarted();
        final Collection<AuditedActionInfo> auditedActionsInfo =
                auditActionsPersistenceManager.getActions();
        final Table<Long, Long, AuditedActionInfo> result = HashBasedTable.create();
        for (AuditedActionInfo auditedActionInfo : auditedActionsInfo) {
            result.put(auditedActionInfo.getRecommendationId(), auditedActionInfo.getWorkflowId(),
                    auditedActionInfo);
        }
        logger.info("Restored bookkeeping cache for {} audited actions from DB in {} ms.",
                result.size(), stopWatch.getTime(TimeUnit.MILLISECONDS));
        return result;
    }

    /**
     * Checks that the action with associated workflow were sent for audit and it is not CLEARED.
     *
     * @param recommendationId recommendation identifier
     * @param workflowId workflow identifier
     * @return true, if action were sent for audit and don't have cleared_timestamp, otherwise false
     */
    public boolean isAlreadySent(long recommendationId, long workflowId) {
        return sentActionsToWorkflowCache.get(recommendationId, workflowId) != null;
    }

    /**
     * Get all action that were previously sent for audit.
     *
     * @return collection of earlier audited actions represented by {@link AuditedActionInfo}s
     */
    public Collection<AuditedActionInfo> getAlreadySentActions() {
        return sentActionsToWorkflowCache.cellSet()
                .stream()
                .map(Cell::getValue)
                .collect(Collectors.toList());
    }

    /**
     * Get all action that were previously sent for audit to certain workflow.
     *
     * @param workflowId workflow identifier
     * @return collection of earlier audited actions in certain workflow represented by
     * {@link AuditedActionInfo}s
     */
    public Collection<AuditedActionInfo> getAlreadySentActions(long workflowId) {
        return sentActionsToWorkflowCache.cellSet()
                .stream()
                .filter(el -> el.getColumnKey() == workflowId)
                .map(Cell::getValue)
                .collect(Collectors.toList());
    }

    /**
     * The set of all entity ids involved in the actions already sent for auditing.
     *
     * @return the set of ids of all entities.
     */
    @Nonnull
    public Set<Long> getAuditedActionsTargetEntityIds() {
        return sentActionsToWorkflowCache.cellSet()
                .stream()
                .map(Cell::getValue)
                .map(AuditedActionInfo::getTargetEntityId)
                .collect(Collectors.toSet());
    }

    /**
     * We handle persistence in a separate thread so we do not delay when new actions become
     * visible.
     *
     * @param auditedActionsUpdates the batch of actions to persist in another thread. The other
     * thread will pick up this task immediately as long as there are no failures with the database.
     */
    public void persistAuditedActionsUpdates(
            @Nonnull final AuditedActionsUpdate auditedActionsUpdates) {
        updateInMemoryCache(auditedActionsUpdates);
        auditedActionsUpdateBatches.addLast(auditedActionsUpdates);
    }

    private void updateInMemoryCache(@Nonnull final AuditedActionsUpdate batch) {
        batch.addedAuditedActionInfos.forEach(
                act -> sentActionsToWorkflowCache.put(act.getRecommendationId(),
                        act.getWorkflowId(), act));
        batch.recentlyClearedAuditedActionInfos.forEach(
                act -> sentActionsToWorkflowCache.put(act.getRecommendationId(),
                        act.getWorkflowId(), act));
        batch.removedAuditedActionInfos.forEach(
                act -> sentActionsToWorkflowCache.remove(act.getRecommendationId(),
                        act.getWorkflowId()));
        batch.removedActionRecommendationOids.forEach(
                // We can clear everything related to this action oid by getting the row map and
                // clearing the map returned by row(). This removes all entries with the rowKey from
                // the table.
                // https://guava.dev/releases/19.0/api/docs/com/google/common/collect/HashBasedTable.html#row(R)
                // > Changes to the returned map will update the underlying table, and vice versa.
                recommendationOid -> sentActionsToWorkflowCache.row(recommendationOid).clear());
    }

    /**
     * Groups together all the differences from one market cycle to the next to update
     * in-memory bookkeeping cache and commit changes to the database.
     */
    public static class AuditedActionsUpdate {

        private final List<AuditedActionInfo> addedAuditedActionInfos;
        private final List<AuditedActionInfo> recentlyClearedAuditedActionInfos;
        private final List<AuditedActionInfo> removedAuditedActionInfos;
        private final List<Long> removedActionRecommendationOids;

        /**
         * Constructor of {@link AuditedActionsUpdate}.
         */
        public AuditedActionsUpdate() {
            this.addedAuditedActionInfos = new ArrayList<>();
            this.recentlyClearedAuditedActionInfos = new ArrayList<>();
            this.removedAuditedActionInfos = new ArrayList<>();
            this.removedActionRecommendationOids = new ArrayList<>();
        }

        /**
         * Adds action that we send for audit.
         *
         * @param actionInfo contains information about action id, workflow id and
         * cleared_timestamp if present
         */
        public void addAuditedAction(@Nonnull final AuditedActionInfo actionInfo) {
            addedAuditedActionInfos.add(actionInfo);
        }

        /**
         * Adds action that was audited recently, but now it is not recommended and we sent that
         * it is CLEARED.
         *
         * @param actionInfo contains information about action id, workflow id and
         * cleared_timestamp if present
         */
        public void addRecentlyClearedAction(@Nonnull final AuditedActionInfo actionInfo) {
            recentlyClearedAuditedActionInfos.add(actionInfo);
        }

        /**
         * Adds action that was audited recently, but it is not recommended during certain
         * time (TTL for CLEARED audited actions).
         *
         * <p>Could also be an audit that needs to be removed because the setting policy changed.</p>
         *
         * @param removedAudit contains information about action id, workflow id that needs to be
         *                     removed.
         */
        public void addAuditedActionForRemoval(@Nonnull final AuditedActionInfo removedAudit) {
            removedAuditedActionInfos.add(removedAudit);
        }

        /**
         * Adds the recommendation oid of an action that needs to be removed from book keeping.
         *
         * @param recommendationOid the recommendation oid of an action that needs to be removed
         *                          from book keeping.
         */
        public void addRemovedActionRecommendationOid(final long recommendationOid) {
            removedActionRecommendationOids.add(recommendationOid);
        }

        /**
         * Returns the new actions accumulated so far.
         *
         * @return the new actions accumulated so far.
         */
        public List<AuditedActionInfo> getAuditedActions() {
            return addedAuditedActionInfos;
        }

        /**
         * Returns the recently cleared actions accumulated so far.
         *
         * @return the recently cleared actions accumulated so far.
         */
        public List<AuditedActionInfo> getRecentlyClearedActions() {
            return recentlyClearedAuditedActionInfos;
        }

        /**
         * Returns the expired actions and audits that need to be removed.
         *
         * @return the expired actions and audits that need to be removed.
         */
        public List<AuditedActionInfo> getRemovedAudits() {
            return removedAuditedActionInfos;
        }

        /**
         * Returns the recommendation oid of an action that needs to be removed from book keeping.
         *
         * @return the recommendation oid of an action that needs to be removed from book keeping.
         */
        public List<Long> getRemovedActionRecommendationOid() {
            return removedActionRecommendationOids;
        }

        @Override
        public String toString() {
            return "AuditedActionsUpdate{"
                + "addedAuditedActionInfos=" + addedAuditedActionInfos
                + ", recentlyClearedAuditedActionInfos=" + recentlyClearedAuditedActionInfos
                + ", expiredClearedAuditedActionInfos=" + removedAuditedActionInfos
                + ", removedActionRecommendationOids=" + removedActionRecommendationOids
                + '}';
        }
    }
}
