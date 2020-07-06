package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;

import org.apache.http.annotation.GuardedBy;

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

    private final IOperationManager operationManager;

    private final Object stateUpdatesLock = new Object();
    @GuardedBy("stateUpdatesLock")
    private final Map<Long, Pair<ActionResponse, Runnable>> stateUpdates = new LinkedHashMap<>();
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
     *         updates
     * @param actionUpdatePeriod period of sending action state updates to external action
     *         approval backend
     * @param updateBatchSize batch size to group all the state updates to send to external
     *         action approval backend
     */
    public ActionUpdateStateService(@Nonnull TargetStore targetStore,
            @Nonnull IOperationManager operationManager,
            @Nonnull IMessageReceiver<ActionResponse> actionStateUpdates,
            @Nonnull ScheduledExecutorService scheduler, long actionUpdatePeriod,
            int updateBatchSize) {
        super(targetStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        if (updateBatchSize < 1) {
            throw new IllegalArgumentException(
                    "Could not use updateBatchSize less then 1. Found: " + updateBatchSize);
        }
        this.updateBatchSize = updateBatchSize;
        actionStateUpdates.addListener(this::actionStateUpdateReceived);
        scheduler.scheduleWithFixedDelay(this::sendStateUpdates, actionUpdatePeriod,
                actionUpdatePeriod, TimeUnit.SECONDS);
    }

    private void sendStateUpdates() {
        try {
            if (actionUpdateStateOperation != null) {
                getLogger().trace(
                        "ActionUpdateState operation {} is in progress, will wait until it is finished: {}",
                        actionUpdateStateOperation::getId, actionUpdateStateOperation::toString);
                return;
            }
            final Collection<Pair<ActionResponse, Runnable>> batch =
                    new ArrayList<>(updateBatchSize);
            synchronized (stateUpdatesLock) {
                final Iterator<Pair<ActionResponse, Runnable>> iterator =
                        stateUpdates.values().iterator();
                for (int i = 0; iterator.hasNext() && i < updateBatchSize; i++) {
                    batch.add(iterator.next());
                }
            }
            final Collection<ActionResponse> actionStates =
                    Collections2.transform(batch, Pair::getFirst);
            final Optional<Long> targetId = getTargetId();
            if (!targetId.isPresent()) {
                getLogger().warn("Action state updates available but no external approval backend"
                                + " target found: [{}]",
                        Collections2.transform(actionStates, ActionResponse::getActionOid));
            } else {
                if (!actionStates.isEmpty()) {
                    try {
                        actionUpdateStateOperation =
                                operationManager.updateExternalAction(targetId.get(), actionStates,
                                        new ActionStateUpdateCallback(targetId.get(), batch));
                    } catch (TargetNotFoundException | InterruptedException | ProbeException | CommunicationException e) {
                        getLogger().warn(
                                "Failed sending action state updates to external action approval target",
                                e);
                    }
                } else {
                    getLogger().debug("There is no action states to update.");
                }
            }
        } catch (RuntimeException e) {
            // Do not rethrow the exception. This prevents rescheduling of all future
            // ScheduledExecutorService.scheduleWithFixedDelay(this::sendStateUpdates())
            // As a result, without this catch, this method would never run again.
            getLogger().error("Unable to send state updates due to exception", e);
        }
    }

    private void actionStateUpdateReceived(@Nonnull ActionResponse request,
            @Nonnull Runnable commitCommand) {
        // We store the commit command. Message will be committed only after it will reach
        // action approval probe.
        stateUpdates.put(request.getActionOid(), Pair.create(request, commitCommand));
        getLogger().debug("Received action {} update to state {} progress {}",
                request::getActionOid, request::getActionResponseState, request::getProgress);
    }

    /**
     * Operation callback to receive responses from external action approval probe.
     */
    private class ActionStateUpdateCallback implements OperationCallback<ActionErrorsResponse> {
        private final long targetId;
        private final Collection<Pair<ActionResponse, Runnable>> request;

        ActionStateUpdateCallback(long targetId,
                @Nonnull Collection<Pair<ActionResponse, Runnable>> request) {
            this.targetId = targetId;
            this.request = request;
        }

        @Override
        public void onSuccess(@Nonnull ActionErrorsResponse response) {
            getLogger().debug("Updating external action states {} finished successfully",
                    actionUpdateStateOperation.getId());
            actionUpdateStateOperation = null;
            final Collection<Pair<ActionResponse, Runnable>> commits = new ArrayList<>(
                    request.size());
            synchronized (stateUpdatesLock) {
                for (ActionErrorDTO actionError : response.getErrorsList()) {
                    getLogger().warn("Error reported updating action {} state at target {}: {}",
                            actionError.getActionOid(), targetId, actionError.getMessage());
                }
                final Collection<Long> receivedIds = Collections2.transform(request,
                        actionState -> actionState.getFirst().getActionOid());
                getLogger().debug("Received response for updates of actions {}", receivedIds);
                stateUpdates.keySet().removeAll(receivedIds);
            }
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
            actionUpdateStateOperation = null;
        }
    }
}
