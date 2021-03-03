package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

/**
 * Action audit service for specific target.
 */
public class TargetActionAuditService {
    private final Logger logger = LogManager.getLogger(getClass());
    private final long targetId;
    private final Deque<Pair<ActionEvent, Runnable>> events = new ConcurrentLinkedDeque<>();
    private final ActionExecutionContextFactory contextFactory;
    private final IOperationManager operationManager;
    private final int batchSize;
    private final AtomicBoolean initialized;

    private final Semaphore semaphore = new Semaphore(1);
    private volatile ActionAudit runningOperation = null;
    private final ExecutorService threadPool;
    private final boolean isServiceNowTarget;
    private final Collection<Future<?>> actionAuditFutures = new ArrayList<>();

    /**
     * Constructs service.
     *
     * @param targetId target id to send audit events to
     * @param operationManager operation manager to execute operations with SDK probe
     * @param contextFactory action context factory used to convert actions from XL to SDK
     *         format
     * @param scheduler scheduled thread pool for periodical execution of sending operations
     * @param batchSize maximum batch size of events to send to SDK probe
     * @param sendIntervalSec interval to send the events to SDK probe
     * @param initialized whether this service is initialized. Only start processing events
     *         after this value is true. The only expected transition here is false -> true
     * @param thinTargetCache cache for simple target information
     */
    protected TargetActionAuditService(long targetId, @Nonnull IOperationManager operationManager,
            @Nonnull ActionExecutionContextFactory contextFactory,
            @Nonnull ScheduledExecutorService scheduler, long sendIntervalSec, int batchSize,
            @Nonnull AtomicBoolean initialized, @Nonnull final ThinTargetCache thinTargetCache) {
        this.targetId = targetId;
        this.isServiceNowTarget =
                isServiceNowTarget(Objects.requireNonNull(thinTargetCache), targetId);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.contextFactory = Objects.requireNonNull(contextFactory);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be a positive value");
        }
        this.batchSize = batchSize;
        this.threadPool = scheduler;
        this.initialized = Objects.requireNonNull(initialized);
        actionAuditFutures.add(
                scheduler.scheduleWithFixedDelay(this::runQueue, sendIntervalSec, sendIntervalSec,
                        TimeUnit.SECONDS));
    }

    private boolean isServiceNowTarget(@Nonnull ThinTargetCache thinTargetCache,
            long auditTargetId) {
        final Optional<ThinTargetInfo> auditTargetInfo =
                thinTargetCache.getTargetInfo(auditTargetId);
        if (auditTargetInfo.isPresent()) {
            final String auditTargetType = auditTargetInfo.get().probeInfo().type();
            return auditTargetType.equals(SDKProbeType.SERVICENOW.getProbeType());
        } else {
            // if there is no info for audit target then we couldn't do audit operation
            // because we have different logic depends of target type
            throw new UnsupportedOperationException(
                    "Failed to get target info and define type of audit target " + auditTargetId
                            + ". Audit operation is impossible");
        }
    }

    /**
     * Method to be called on new event arrives.
     *
     * @param event event appeared for this target
     * @param commit commit operation
     */
    public void onNewEvent(@Nonnull ActionEvent event, @Nonnull Runnable commit) {
        events.addLast(Pair.create(event, commit));
        logger.trace("New action event appeared for action {} from {} to {} at {}",
                event.getActionRequest().getActionId(), event.getOldState(), event.getNewState(),
                event.getTimestamp());
        if (events.size() >= batchSize) {
            // Try to schedule sending events as soon as we have full batch
            actionAuditFutures.add(threadPool.submit(this::runQueue));
        }
    }

    private void runQueue() {
        if (!initialized.get()) {
            logger.debug(
                    "Action events processing for target {} is not yet initialized, postponing",
                    targetId);
            return;
        }
        if (events.isEmpty()) {
            logger.trace("No new action events for target {}", targetId);
            return;
        }
        if (!semaphore.tryAcquire()) {
            logger.debug(
                    "Action audit sending operation is still running: {} for target {}. Skipping next turn",
                    runningOperation, targetId);
            return;
        }
        try {
            // We wrap everything into this catch block not to break scheduled execution
            runQueueInternal();
        } catch (RuntimeException e) {
            logger.error(
                    "Unexpected error occurred while running actions queue for target " + targetId,
                    e);
            semaphore.release();
        }
    }

    /**
     * Performs sending action audit events to SDK probes. Triggered periodically by a scheduler.
     *
     * <p>This method is the only one who can transition null -> nonnull, but we still need to
     * synchronize inside this method, as it itself could be called simultaneously:
     * from scheduler and from {@link ActionAuditCallback#onSuccess(ActionErrorsResponse)}.
     */
    private void runQueueInternal() {
        final List<Pair<ActionEvent, Runnable>> eventsRequested = new ArrayList<>(
                Math.max(events.size(), batchSize));
        final List<ActionEventDTO> actionsToSend = new ArrayList<>();
        while (actionsToSend.size() < batchSize && !events.isEmpty()) {
            // There is no race condition here as this thread is the only consumer of events
            // in this queue. Others can only add events.
            final Pair<ActionEvent, Runnable> event = events.poll();
            eventsRequested.add(event);
            try {
                actionsToSend.add(convert(event.getFirst()));
            } catch (ContextCreationException e) {
                logger.error("Action " + event.getFirst().getActionRequest().getActionId()
                        + " event will be skipped as could not be converted", e);
            }
        }
        final ActionAuditCallback callback = new ActionAuditCallback(eventsRequested, StopWatch.createStarted());
        if (actionsToSend.isEmpty()) {
            logger.debug("No actions available for audit.");
            semaphore.release();
            return;
        }
        try {
            logger.debug("Sending {} action events for external audit", actionsToSend::size);
            runningOperation = operationManager.sendActionAuditEvents(targetId, actionsToSend,
                    callback);
        } catch (TargetNotFoundException | ProbeException | CommunicationException e) {
            callback.onFailure(
                    "Failed sending action events to external action audit target " + targetId
                            + ": " + events.stream()
                            .map(Pair::getFirst)
                            .map(ActionEvent::getActionRequest)
                            .map(ExecuteActionRequest::getActionId)
                            .map(Objects::toString)
                            .collect(Collectors.joining(",")), e);
        } catch (InterruptedException e) {
            callback.onFailure("Thread interrupted sending action events to target " + targetId, e);
        }
    }

    @Nonnull
    private ActionEventDTO convert(@Nonnull ActionEvent event) throws ContextCreationException {
        final ExecuteActionRequest actionRequest = event.getActionRequest();
        final ActionExecutionDTO actionExecution;
        if (ActionResponseState.CLEARED != event.getNewState()) {
            actionExecution = contextFactory.getActionExecutionContext(event.getActionRequest())
                    .buildActionExecutionDto();
        } else {
            // when we are sending CLEARED action (we have only limited information about action
            // that we persist in DB and in-memory cache for audited actions), so we can't
            // initialize ExecuteActionRequest with detailed information about action
            actionExecution = ActionExecutionDTO.newBuilder()
                    .setActionOid(actionRequest.getActionId())
                    .setActionType(ActionType.NONE)
                    .build();
        }
        final ActionEventDTO.Builder builder = ActionEventDTO.newBuilder()
                .setAction(actionExecution)
                .setOldState(event.getOldState())
                .setNewState(event.getNewState())
                .setTimestamp(event.getTimestamp());
        if (event.hasAcceptedBy()) {
            builder.setAcceptedBy(event.getAcceptedBy());
        }
        return builder.build();
    }

    /**
     * Cancels all of the submitted tasks for sending audit message to external service related to
     * this target,commit in internal kafka all audit events from queue in order not resend them
     * again if target will be added again. Cleans queue with undelivered audit events.
     */
    public void purgeAuditEvents() {
        actionAuditFutures.forEach(future -> future.cancel(true));
        events.forEach(event -> event.getSecond().run());
        events.clear();
    }

    /**
     * Return queue with audit events.
     *
     * @return queue with audit events
     */
    @VisibleForTesting
    public Deque<Pair<ActionEvent, Runnable>> getEvents() {
        return events;
    }

    /**
     * Return collections of audit futures.
     *
     * @return collection of audit futures
     */
    @VisibleForTesting
    public Collection<Future<?>> getActionAuditFutures() {
        return actionAuditFutures;
    }

    /**
     * Operation callback to receive a response from action audit request processing in SDK probe.
     */
    private class ActionAuditCallback implements OperationCallback<ActionErrorsResponse> {

        private final List<Pair<ActionEvent, Runnable>> auditEvents;
        private final StopWatch stopWatch;

        ActionAuditCallback(@Nonnull List<Pair<ActionEvent, Runnable>> events, @Nonnull StopWatch started) {
            this.auditEvents = Objects.requireNonNull(events);
            this.stopWatch = Objects.requireNonNull(started);
        }

        @Override
        public void onSuccess(@Nonnull ActionErrorsResponse response) {
            try {
                if (!response.getErrorsList().isEmpty()) {
                    // If only one action failed from the batch we resend all of them. We intentionally
                    // made this decision to to simplify the error handling code. Kafka commits only use
                    // message offset unfortunately. Kafka doesn't implement a message by message
                    // commit. It will take extra effort to track the messages individually before
                    // committing back to Kafka. Additionally, if TP restarts, we'll still resend the
                    // batch even though we go through the extra effort to handle it in memory because
                    // kafka commits by offset.
                    // If you plan improve this, you must also fix the ActionStreakKafka probe since it
                    // fails fast and only sends the first oid that fails.
                    response.getErrorsList()
                            .forEach(error -> logger.warn(
                                    "Error reported auditing action {} event: {}",
                                    error.getActionOid(), error.getMessage()));
                    logger.info("Action audit failed. Action events for actions [{}] returned to "
                            + "queue.", this::getActionOids);
                    // TODO OM-64606 remove this ServiceNow specific logic after changing the ServiceNow app
                    //  and having generic audit logic for orchestration targets.
                    //  Now we need to have this ServiceNow-specific logic, because for ServiceNow we send all
                    //  actions for audit every market cycle and we don't need to return them to
                    //  the queue. For other orchestration targets (e.g. ActionStreamKafka) we
                    //  need to send audit event once without repetitions, so if actions is not
                    //  delivered to external system then we need to return them to queue and
                    //  resend later.
                    if (!isServiceNowTarget) {
                        returnActionEventToQueue();
                    }
                } else {
                    logger.debug("Successfully finished auditing batch with {} actions [{}] in {}"
                                    + " seconds.", auditEvents::size, this::getActionOids,
                            () -> stopWatch.getTime(TimeUnit.SECONDS));
                    for (Pair<ActionEvent, Runnable> event : auditEvents) {
                        event.getSecond().run();
                    }
                    logger.trace("Action audit events successfully committed");
                }
            } finally {
                // Unexpected runtime exceptions might come from returnActionEventToQueue() or
                // event.getSecond().run(), so ensure that we release the lock every time to
                // prevent a deadlock.
                semaphore.release();
            }

            // Send next batch of action events, if any
            actionAuditFutures.add(threadPool.submit(TargetActionAuditService.this::runQueue));
        }

        @Override
        public void onFailure(@Nonnull String error) {
            onFailure(error, null);
        }

        public void onFailure(@Nonnull String error, @Nullable Throwable exception) {
            final String message = String.format(
                    "\"Action audit failed in %ss. Action events for actions [%s] returned to "
                            + "queue: %s\"", stopWatch.getTime(TimeUnit.SECONDS), getActionOids(),
                    error);
            logger.info(message, exception);
            returnActionEventToQueue();
            semaphore.release();
        }

        /**
         * Returns failed sent action events to the front of the queue in order to resend them
         * again in order which they were initially added to the queue. We need to observe order
         * of sending action audit events, because otherwise we can send e.g. CLEARED event
         * earlier then ON_GEN which is wrong.
         */
        private void returnActionEventToQueue() {
            final ListIterator<Pair<ActionEvent, Runnable>> iterator =
                    auditEvents.listIterator(auditEvents.size());
            while (iterator.hasPrevious()) {
                final Pair<ActionEvent, Runnable> event = iterator.previous();
                events.addFirst(event);
            }
        }

        @Nonnull
        private String getActionOids() {
            return auditEvents.stream()
                    .map(Pair::getFirst)
                    .map(ActionEvent::getActionRequest)
                    .map(ExecuteActionRequest::getActionId)
                    .map(Objects::toString)
                    .collect(Collectors.joining(","));
        }
    }

}
