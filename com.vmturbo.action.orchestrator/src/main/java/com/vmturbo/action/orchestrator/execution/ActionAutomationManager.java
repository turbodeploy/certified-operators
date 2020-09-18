package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.approval.ActionApprovalSender;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionTask;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * Manages the automated execution of actions.
 */
public class ActionAutomationManager {
    private static final Logger logger = LogManager.getLogger();

    private final AutomatedActionExecutor automatedExecutor;
    // Stores the task futures/promises of the actions which have been submitted for execution.
    private final List<ActionExecutionTask> actionExecutionFutures = new ArrayList<>();
    private final Object actionExecutionFuturesLock = new Object();

    private final ActionApprovalSender approvalRequester;

    /**
     * Create a new {@link ActionAutomationManager}.
     *
     * @param automatedActionExecutor Executes automated actions.
     * @param approvalRequester Requests approval for actions.
     */
    public ActionAutomationManager(@Nonnull final AutomatedActionExecutor automatedActionExecutor,
                            @Nonnull final ActionApprovalSender approvalRequester) {
        this.automatedExecutor = automatedActionExecutor;
        this.approvalRequester = Objects.requireNonNull(approvalRequester);
    }

    /**
     * Update the automated execution of actions.
     *
     * @param store The actions store containing actions to use to update automation.
     *
     * @throws InterruptedException If approval request for actions is interrupted.
     */
    public void updateAutomation(@Nonnull final ActionStore store) throws InterruptedException {
        try {
            synchronized (actionExecutionFuturesLock) {
                cancelActionsWithoutActiveExecutionWindow();
                actionExecutionFutures.removeIf(actionExecutionTask ->
                    actionExecutionTask.getFuture().isDone()
                        || actionExecutionTask.getAction().getState() == ActionState.CLEARED
                        || actionExecutionTask.getAction().getState() == ActionState.FAILED
                        || actionExecutionTask.getAction().getState() == ActionState.SUCCEEDED);
                actionExecutionFutures.addAll(automatedExecutor.executeAutomatedFromStore(store));
            }
            approvalRequester.sendApprovalRequests(store);
        } catch (RuntimeException e) {
            logger.info("Unable to execute automated actions: ", e);
        }
    }

    /**
     * Cancel actions which are waiting in the queue to be executed.
     *
     * @return The number of actions which were cancelled and removed from the queue.
     */
    public int cancelQueuedActions() {
        // Don't cancel actions in progress. Cancel only those tasks which are yet to be executed.
        logger.info("Cancelling all pending automated actions which are waiting to be executed");
        synchronized (actionExecutionFuturesLock) {
            int cancelledCount = actionExecutionFutures.stream()
                .filter(actionTask -> actionTask.getAction().getState() == ActionState.QUEUED)
                .map(actionTask -> {
                    Action action = actionTask.getAction();
                    actionTask.getFuture().cancel(false);
                    action.receive(new NotRecommendedEvent(action.getId()));
                    return 1;
                })
                .reduce(Integer::sum)
                .orElse(0);

            logger.info("Cancelled execution of {} queued automated actions. Total automated actions: {}",
                cancelledCount, actionExecutionFutures.size());
            actionExecutionFutures.clear();
            return cancelledCount;
        }
    }

    /**
     * Cancel actions with associated execution window, but this window is
     * not active right now.
     * NOTE: call this method from synchronised block guarded by
     * {@link #actionExecutionFuturesLock}.
     */
    @GuardedBy("actionExecutionFuturesLock")
    private void cancelActionsWithoutActiveExecutionWindow() {
        final AtomicInteger cancelledCount = new AtomicInteger();
        final Set<Long> cancelledRecommendationIds = new HashSet<>();
        actionExecutionFutures.stream()
            .filter(actionTask -> !isActiveExecutionWindow(actionTask))
            .forEach(actionTask -> {
                final Action action = actionTask.getAction();
                final boolean isCanceled = actionTask.getFuture().cancel(false);
                if (isCanceled) {
                    action.receive(new RollBackToAcceptedEvent());
                    cancelledCount.getAndIncrement();
                    cancelledRecommendationIds.add(action.getRecommendationOid());
                }
            });

        if (cancelledCount.get() != 0) {
            logger.info("Cancelled execution of {} queued actions which have no active execution "
                + "windows.", cancelledCount.get());
            if (logger.isDebugEnabled()) {
                logger.debug("Cancelled execution of following actions with recommendation ids: {}",
                    () -> StringUtils.join(cancelledRecommendationIds, ","));
            }
        }
    }

    /**
     * Check that submitted action has active execution window.
     *
     * @param actionTask execution task contains future for executed action
     * @return if execution window is active, otherwise false
     */
    private boolean isActiveExecutionWindow(@Nonnull ActionExecutionTask actionTask) {
        final Action action = actionTask.getAction();
        if (action.getState() == ActionState.QUEUED && action.getSchedule().isPresent()) {
            return action.getSchedule().get().isActiveScheduleNow();
        }
        return true;
    }
}
