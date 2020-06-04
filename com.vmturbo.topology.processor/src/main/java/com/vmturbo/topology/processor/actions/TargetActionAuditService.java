package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
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
     */
    protected TargetActionAuditService(long targetId, @Nonnull IOperationManager operationManager,
            @Nonnull ActionExecutionContextFactory contextFactory,
            @Nonnull ScheduledExecutorService scheduler, long sendIntervalSec, int batchSize,
            @Nonnull AtomicBoolean initialized) {
        this.targetId = targetId;
        this.operationManager = Objects.requireNonNull(operationManager);
        this.contextFactory = Objects.requireNonNull(contextFactory);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be a positive value");
        }
        this.batchSize = batchSize;
        this.threadPool = scheduler;
        this.initialized = Objects.requireNonNull(initialized);
        scheduler.scheduleWithFixedDelay(this::runQueue, sendIntervalSec, sendIntervalSec,
                TimeUnit.SECONDS);
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
            threadPool.execute(this::runQueue);
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
        final List<ActionEventDTO> actionsToSend = new ArrayList<>(eventsRequested.size());
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
        final ActionAuditCallback callback = new ActionAuditCallback(eventsRequested);
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
    private ActionEventDTO convert(@Nonnull ActionEvent event) {
        // TODO conversion is very slow if executed one-by-one. We should batch the conversion.
        final ActionExecutionDTO actionExecution = contextFactory.getActionExecutionContext(
                event.getActionRequest()).buildActionExecutionDto();
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
     * Operation callback to receive a response from action audit request processing in SDK probe.
     */
    private class ActionAuditCallback implements OperationCallback<ActionErrorsResponse> {

        private final List<Pair<ActionEvent, Runnable>> events;

        ActionAuditCallback(@Nonnull List<Pair<ActionEvent, Runnable>> events) {
            this.events = Objects.requireNonNull(events);
        }

        @Override
        public void onSuccess(@Nonnull ActionErrorsResponse response) {
            semaphore.release();
            for (ActionErrorDTO error: response.getErrorsList()) {
                // Errors reported by a probe, but we cannot do anything else. So we committing
                // the messages after logging not to process them again.
                logger.warn("Error reported auditing action {} event: {}", error.getActionOid(),
                        error.getMessage());
            }
            logger.debug("Successfully finished auditing of actions [{}]", this::getActionOids);
            for (Pair<ActionEvent, Runnable> event: events) {
                event.getSecond().run();
            }
            logger.trace("Action audit events successfully committed");
            // Send next batch of action events, if any
            runQueue();
        }

        @Override
        public void onFailure(@Nonnull String error) {
            onFailure(error, null);
        }

        public void onFailure(@Nonnull String error, @Nullable Throwable exception) {
            final String message = String.format(
                    "\"Action audit failed. Action events for actions [%s] returned to queue: %s\"",
                    getActionOids(), error);
            logger.info(message, exception);
            final ListIterator<Pair<ActionEvent, Runnable>> iterator = events.listIterator(
                    events.size());
            while (iterator.hasPrevious()) {
                final Pair<ActionEvent, Runnable> event = iterator.previous();
                TargetActionAuditService.this.events.addFirst(event);
            }
            semaphore.release();
        }

        @Nonnull
        private String getActionOids() {
            return events.stream()
                    .map(Pair::getFirst)
                    .map(ActionEvent::getActionRequest)
                    .map(ExecuteActionRequest::getActionId)
                    .map(Objects::toString)
                    .collect(Collectors.joining(","));
        }
    }

}
