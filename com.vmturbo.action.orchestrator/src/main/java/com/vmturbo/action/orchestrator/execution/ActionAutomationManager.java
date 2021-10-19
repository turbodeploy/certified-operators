package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.approval.ActionApprovalSender;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalFuture;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.components.api.FormattedString;

/**
 * Manages the automated execution of actions.
 */
public class ActionAutomationManager {
    private static final Logger logger = LogManager.getLogger();

    private final AutomatedActionExecutor automatedExecutor;
    // Stores the task futures/promises of the actions which have been submitted for execution.
    private final List<ConditionalFuture> actionExecutionFutures = new ArrayList<>();
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
                MutableInt removalCount = new MutableInt(0);
                actionExecutionFutures.removeIf(future -> {
                    final boolean shouldRemove = future.isDone()
                            || future.getOriginalTask().getActionList().stream()
                            .map(Action::getState)
                            .allMatch(state -> state == ActionState.CLEARED
                                    || state == ActionState.FAILED
                                    || state == ActionState.SUCCEEDED);
                    if (shouldRemove) {
                        removalCount.increment();
                    }
                    return shouldRemove;
                });
                final List<ConditionalFuture> newExecuted = automatedExecutor.executeAutomatedFromStore(store);
                actionExecutionFutures.addAll(newExecuted);

                final Map<String, MutableInt> byStatus = new HashMap<>();
                actionExecutionFutures.forEach(future -> {
                    if (future.isStarted()) {
                        byStatus.computeIfAbsent("Running", k -> new MutableInt()).increment();
                    } else {
                        byStatus.computeIfAbsent("Queued", k -> new MutableInt()).increment();
                    }
                });
                logger.info("{} completed action lists cleared from execution queue. {} action "
                        + "lists added. {}", removalCount.getAndDecrement(), newExecuted.size(),
                        actionExecutionFutures.isEmpty()
                            ? "Queue is empty."
                            : FormattedString.format("Queue has {} total action lists: {}.", actionExecutionFutures.size(), byStatus.entrySet()
                                .stream()
                                .map(entry -> entry.getValue().getValue() + " " + entry.getKey())
                                .collect(Collectors.joining(", "))));
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
                    .map(future -> {
                        final List<Action> actionList = future.getOriginalTask().getActionList();
                        if (actionList.stream().allMatch(action ->
                                action.getState() == ActionState.QUEUED)) {
                            future.cancel(false);
                            actionList.forEach(action ->
                                    action.receive(new NotRecommendedEvent(action.getId())));
                            return 1;
                        } else {
                            return 0;
                        }
                })
                .reduce(Integer::sum)
                .orElse(0);

            logger.info("Cancelled execution of {} queued automated action lists. Total automated "
                    + "action lists: {}", cancelledCount, actionExecutionFutures.size());
            actionExecutionFutures.clear();
            return cancelledCount;
        }
    }

    /**
     * Cancel actions with associated execution window, but this window is
     * not active right now.
     * NOTE: call this method from synchronized block guarded by
     * {@link #actionExecutionFuturesLock}.
     */
    @GuardedBy("actionExecutionFuturesLock")
    private void cancelActionsWithoutActiveExecutionWindow() {
        final AtomicInteger cancelledCount = new AtomicInteger();
        final Set<Long> cancelledRecommendationIds = new HashSet<>();
        actionExecutionFutures.stream()
            .filter(actionTask -> !isActiveExecutionWindow(actionTask))
            .forEach(future -> {
                final boolean isCanceled = future.cancel(false);
                if (isCanceled) {
                    future.getOriginalTask().getActionList().forEach(action -> {
                        action.receive(new RollBackToAcceptedEvent());
                        cancelledRecommendationIds.add(action.getRecommendationOid());
                    });
                    cancelledCount.getAndIncrement();
                }
            });

        if (cancelledCount.get() != 0) {
            logger.info("Cancelled execution of {} queued action lists which have no active "
                    + "execution windows.", cancelledCount.get());
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Cancelled execution of actions with the following recommendation ids: {}",
                        () -> StringUtils.join(cancelledRecommendationIds, ","));
            }
        }
    }

    /**
     * Check that submitted action has active execution window.
     *
     * @param future future for executed action
     * @return if execution window is active, otherwise false
     */
    private boolean isActiveExecutionWindow(@Nonnull ConditionalFuture future) {
        return future.getOriginalTask().getActionList().stream().allMatch(action -> {
            if (action.getState() == ActionState.QUEUED && action.getSchedule().isPresent()) {
                return action.getSchedule().get().isActiveScheduleNow();
            }
            return true;
        });
    }
}
