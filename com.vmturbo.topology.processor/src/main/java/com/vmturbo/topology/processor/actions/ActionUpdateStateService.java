package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;

import io.opentracing.SpanContext;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.AbstractActionApprovalService;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Service to update states of actions on external approval backend, depending on action state
 * change in action orchestrator.
 */
public class ActionUpdateStateService extends AbstractActionApprovalService {

    private final int updateBatchSize;
    /**
     * Max elements that {@link ActionUpdateStateService#stateUpdatesQueue} can contain.
     */
    private final int maxElementsInQueue;

    private final IOperationManager operationManager;

    /**
     * Queue containing action update events to send to an SDK probe.
     * We use double-ended queue because or message sending retrials.
     * If fails to deliver the message, it must put the
     * messages back into the queue preserving the order - in order to retry sending later with
     * the same order. In order to achieve it, if sender failed to send the messages, it will
     * prepend the queue (write messages back to the head of a queue).
     * New states updates we put in the tail of the queue.
     * Sends state updates (with defined batch size) from the head of the queue.
     */
    private final BlockingDeque<Pair<ActionResponse, Runnable>> stateUpdatesQueue =
            new LinkedBlockingDeque<>();
    /**
     * Currently running operation to update action states in external action approval backend.
     * This value could be reset only from {@link IOperationManager.OperationCallback}.
     * Value could be set only from {@link #sendStateUpdates()}.
     */
    private volatile ActionUpdateState actionUpdateStateOperation = null;

    /**
     * Constructs action update state service.
     *
     * @param targetStore target store
     * @param operationManager operation manager
     * @param actionStateUpdates message receiver to accept action state updates from
     * @param scheduler scheduler to execute regular operations - sending batches of state
     * updates
     * @param actionUpdatePeriod period of sending action state updates to external action
     * approval backend
     * @param updateBatchSize batch size to group all the state updates to send to external
     * @param maxSizeOfStateUpdatesQueue max elements is queue contains state updates to send to
     * external approval backend.
     */
    public ActionUpdateStateService(@Nonnull TargetStore targetStore,
            @Nonnull IOperationManager operationManager,
            @Nonnull IMessageReceiver<ActionResponse> actionStateUpdates,
            @Nonnull ScheduledExecutorService scheduler, long actionUpdatePeriod,
            int updateBatchSize, int maxSizeOfStateUpdatesQueue) {
        super(targetStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        if (updateBatchSize < 1) {
            throw new IllegalArgumentException(
                    "Could not use updateBatchSize less then 1. Found: " + updateBatchSize);
        }
        this.updateBatchSize = updateBatchSize;
        if (maxSizeOfStateUpdatesQueue < 1) {
            throw new IllegalArgumentException(
                    "Could not use maxSizeOfStateUpdatesQueue less " + "then 1. Found: "
                            + maxSizeOfStateUpdatesQueue);
        }
        this.maxElementsInQueue = maxSizeOfStateUpdatesQueue;
        actionStateUpdates.addListener(this::actionStateUpdateReceived);
        scheduler.scheduleWithFixedDelay(this::sendStateUpdates, actionUpdatePeriod,
                actionUpdatePeriod, TimeUnit.SECONDS);
    }

    private void sendStateUpdates() {
        final List<Pair<ActionResponse, Runnable>> batchToSend = new ArrayList<>(updateBatchSize);
        try {
            final Optional<Long> targetId = getTargetId();
            if (isAbleToSendStateUpdates(targetId)) {
                stateUpdatesQueue.drainTo(batchToSend, updateBatchSize);
                sendStateUpdates(batchToSend, targetId.get());
            }
        } catch (RuntimeException e) {
            // Do not rethrow the exception. This prevents rescheduling of all future
            // ScheduledExecutorService.scheduleWithFixedDelay(this::sendStateUpdates())
            // As a result, without this catch, this method would never run again.
            getLogger().error("Unable to send state updates due to exception", e);
            returnStateUpdatesToDeque(batchToSend);
        }
    }

    /**
     * Send states updates to external approval backend. If there are any problems with sending
     * updates then we return updates to deque.
     *
     * @param batchToSend contains states updates to send
     * @param targetId id of external approval backend target
     */
    private void sendStateUpdates(@Nonnull List<Pair<ActionResponse, Runnable>> batchToSend,
            long targetId) {
        try {
            final Collection<ActionResponse> stateUpdates =
                    Collections2.transform(batchToSend, Pair::getFirst);
            actionUpdateStateOperation =
                    operationManager.updateExternalAction(targetId, stateUpdates,
                            new ActionStateUpdateCallback(targetId, batchToSend));
        } catch (TargetNotFoundException | ProbeException | CommunicationException e) {
            getLogger().warn(
                    "Failed sending action state updates to external action approval target", e);
            returnStateUpdatesToDeque(batchToSend);
        } catch (InterruptedException ex) {
            getLogger().info(
                    "Thread interrupted while sending state updates to external orchestrator", ex);
            returnStateUpdatesToDeque(batchToSend);
        }
    }

    /**
     * Define ability to send state updates.
     * We send state updates if following conditions are true:
     * 1. there is no active operations of sending state updates
     * 2. external approval target is present
     * 3. there are some state updates in the queue
     *
     * @param targetId id of external approval backend target
     * @return true if we can send state updates, otherwise false
     */
    private boolean isAbleToSendStateUpdates(@Nonnull Optional<Long> targetId) {
        if (actionUpdateStateOperation != null) {
            getLogger().trace(
                    "ActionUpdateState operation {} is in progress, will wait until it is finished: {}",
                    actionUpdateStateOperation::getId, actionUpdateStateOperation::toString);
            return false;
        }
        if (stateUpdatesQueue.isEmpty()) {
            getLogger().debug("There is no action states to update.");
            return false;
        }
        if (!targetId.isPresent()) {
            getLogger().warn("Action state updates available but no external approval backend"
                    + " target found: [{}]", () -> stateUpdatesQueue.stream()
                    .map(el -> el.getFirst().getActionOid())
                    .collect(Collectors.toSet()));
            return false;
        }
        return true;
    }

    private void actionStateUpdateReceived(@Nonnull ActionResponse request,
            @Nonnull Runnable commitCommand, @Nonnull SpanContext tracingContext) {
        // We store the commit command. Message will be committed only after it will reach
        // action approval probe.
        if (stateUpdatesQueue.size() >= maxElementsInQueue) {
            // remove oldest element from queue in order to add new state update
            final ActionResponse removedElement = stateUpdatesQueue.pollFirst().getFirst();
            getLogger().warn(
                    "Queue for sending state updates to external orchestrator is full and have {} elements."
                            + "The oldest one ({} - {}) will be dropped.", maxElementsInQueue,
                    removedElement.getActionOid(), removedElement.getActionResponseState());
        }
        stateUpdatesQueue.add(Pair.create(request, commitCommand));
        getLogger().debug("Received action {} update to state {} progress {}",
                request::getActionOid, request::getActionResponseState, request::getProgress);
    }

    private void returnStateUpdatesToDeque(@Nonnull List<Pair<ActionResponse, Runnable>> request) {
        final ListIterator<Pair<ActionResponse, Runnable>> listIterator =
                request.listIterator(request.size());
        while (listIterator.hasPrevious()) {
            if (stateUpdatesQueue.size() < maxElementsInQueue) {
                stateUpdatesQueue.addFirst(listIterator.previous());
            } else {
                // don't return old state updates to the queue when it is full
                final ActionResponse elementToReturn = listIterator.previous().getFirst();
                getLogger().warn(
                        "Queue for sending state updates to external orchestrator is full and "
                                + "have {} elements. The oldest one ({} - {}) will not be "
                                + "returned to the queue", maxElementsInQueue,
                        elementToReturn.getActionOid(), elementToReturn.getActionResponseState());
            }
        }
        getLogger().debug(
                "State updates for following actions ({}) were returned to deque because of "
                        + "failure sending", () -> request.stream()
                        .map(el -> el.getFirst().getActionOid())
                        .collect(Collectors.toSet()));
    }

    /**
     * Operation callback to receive responses from external action approval probe.
     */
    private class ActionStateUpdateCallback implements OperationCallback<ActionErrorsResponse> {
        private final long targetId;
        private final List<Pair<ActionResponse, Runnable>> request;

        ActionStateUpdateCallback(long targetId,
                @Nonnull List<Pair<ActionResponse, Runnable>> request) {
            this.targetId = targetId;
            this.request = request;
        }

        @Override
        public void onSuccess(@Nonnull ActionErrorsResponse response) {
            getLogger().debug("Updating external action states {} finished successfully",
                    actionUpdateStateOperation.getId());
            actionUpdateStateOperation = null;
            for (ActionErrorDTO actionError : response.getErrorsList()) {
                getLogger().warn("Error reported updating action {} state at target {}: {}",
                        actionError.getActionOid(), targetId, actionError.getMessage());
            }
            getLogger().debug("Received response for updates of actions {}",
                    () -> Collections2.transform(request, el -> el.getFirst().getActionOid()));
            // Run commits outside of a synchronization lock
            for (Pair<ActionResponse, Runnable> actionState : request) {
                actionState.getSecond().run();
            }
            sendStateUpdates();
        }

        @Override
        public void onFailure(@Nonnull String error) {
            // This is not a failure of a remote probe, so we do not remove state updates here.
            getLogger().warn("Error updating action states {} at target {}: {}",
                    actionUpdateStateOperation.getId(), targetId, error);
            returnStateUpdatesToDeque(request);
            actionUpdateStateOperation = null;
        }
    }
}
