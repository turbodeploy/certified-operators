package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import io.opentracing.SpanContext;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.ExternalActionInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.topology.processor.AbstractActionApprovalService;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Service responsible for approving actions. This service will send action approval requests
 * to an external action approval probe and will try getting states for all the actions, previously
 * sent for approval.
 */
public class ActionApprovalService extends AbstractActionApprovalService {

    private final IMessageSender<GetActionStateResponse> actionStateSender;
    private final IMessageSender<ActionApprovalResponse> approvalSender;

    private final IOperationManager operationManager;
    private final ActionExecutionContextFactory contextFactory;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Set of actions that we requested approval for. This set is a volatile unmodifiable set.
     * In this regard, it becomes thread-safe.
     */
    private volatile Set<Long> approvalsToCreate = Collections.emptySet();
    private volatile Set<Long> trackedApprovals = new HashSet<>();

    @GuardedBy("lock")
    private GetActionState actionStateOperation = null;

    @GuardedBy("lock")
    private ActionApproval actionApprovalOperation = null;

    /**
     * Constructs action approval service.
     *
     * @param actionApprovalRequests message receiver which will notify the service about
     *         new action approval requests
     * @param actionStateSender message sender to notify about external action state updates
     * @param approvalSender message sender to notify action approval results
     * @param actionExecutionContextFactory factory used to convert XL action execution to SDK
     * @param operationManager operation manager to operate with SDK probes
     * @param targetStore target store
     * @param scheduler scheduler to execute periodical checks for external action state
     *         updates
     * @param getActionStatePeriod period of external action state requests.
     */
    public ActionApprovalService(
            @Nonnull IMessageReceiver<ActionApprovalRequests> actionApprovalRequests,
            @Nonnull IMessageSender<GetActionStateResponse> actionStateSender,
            @Nonnull IMessageSender<ActionApprovalResponse> approvalSender,
            @Nonnull IOperationManager operationManager,
            @Nonnull ActionExecutionContextFactory actionExecutionContextFactory,
            @Nonnull TargetStore targetStore,
            @Nonnull ScheduledExecutorService scheduler, long getActionStatePeriod) {
        super(targetStore);
        this.actionStateSender = Objects.requireNonNull(actionStateSender);
        this.approvalSender = Objects.requireNonNull(approvalSender);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.contextFactory = Objects.requireNonNull(actionExecutionContextFactory);
        actionApprovalRequests.addListener(this::actionApprovalRequested);
        scheduler.scheduleWithFixedDelay(this::requestExternalStateUpdates, getActionStatePeriod,
                getActionStatePeriod, TimeUnit.SECONDS);
    }

    @Nonnull
    private static String getActionOidsString(@Nonnull ActionApprovalRequests request) {
        return request.getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    private void actionApprovalRequested(@Nonnull ActionApprovalRequests requests,
            @Nonnull Runnable commitCommand, @Nonnull SpanContext tracingContext) {
        final Optional<Long> targetId = getTargetId();
        if (!targetId.isPresent()) {
            getLogger().warn("No external approval backend targets found,"
                            + " while new action approval requests received: [{}]",
                    getActionOidsString(requests));
            commitCommand.run();
            return;
        }
        // We will send over to the external approval target all actions that require such approval.
        approvalsToCreate = Collections.unmodifiableSet(requests.getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .collect(Collectors.toSet()));
        if (approvalsToCreate.isEmpty()) {
            // Remote action approval backend is not expecting the complete pack of actions
            // It's the backend's responsibility to timeout the actions
            getLogger().debug("No actions reported. Skipping approving actions");
            return;
        }
        trackedApprovals.retainAll(approvalsToCreate);
        try {
            if (getActionApprovalOperation() == null) {
                final List<ActionExecutionDTO> actionExecutionList = new ArrayList<>(
                        requests.getActionsCount());
                for (ExecuteActionRequest actionRequest: requests.getActionsList()) {
                    try {
                        final ActionExecutionDTO actionExecutionDTO =
                                contextFactory.getActionExecutionContext(actionRequest)
                                        .buildActionExecutionDto();
                        actionExecutionList.add(actionExecutionDTO);
                    } catch (ContextCreationException e) {
                        getLogger().warn("Failed to create SDK action from " + actionRequest, e);
                    }
                }

                lock.writeLock().lock();
                try {
                    this.actionApprovalOperation = operationManager.approveActions(targetId.get(),
                            actionExecutionList, new ApproveActionsCallback(targetId.get()));
                } finally {
                    lock.writeLock().unlock();
                }

                getLogger().debug("Actions {} were successfully sent for approval; action approval operation = {}",
                        getActionOidsString(requests), actionApprovalOperation);
            } else {
                getLogger().warn(
                        "Another approval operation is running, skipping the new request for actions [{}]: {}",
                        getActionOidsString(requests), actionApprovalOperation);
            }
        } catch (TargetNotFoundException | InterruptedException | ProbeException | CommunicationException e) {
            getLogger().warn(
                    "Failed sending action approval request to external action approval target", e);
        } finally {
            // It is safe to omit some approval messages. On the next market cycle a new
            // complete version of the message will be generated.
            commitCommand.run();
        }
    }

    private ActionApproval getActionApprovalOperation() {
        lock.readLock().lock();
        try {
            return actionApprovalOperation;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void updateActionApprovalOperation(ActionApproval actionApproval) {
        lock.writeLock().lock();
        try {
            this.actionApprovalOperation = actionApproval;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void requestExternalStateUpdates() {
        try {
            if (trackedApprovals.isEmpty()) {
                getLogger().trace(
                    "There is no current action approvals. Will not request their states");
                return;
            }
            final Optional<Long> targetId = getTargetId();
            if (!targetId.isPresent()) {
                getLogger().warn(
                    "There is not external action approval target. Skipping external state updates for actions [{}]",
                    trackedApprovals);
                return;
            }
            getLogger().debug("Requesting action states for the following actions from target {}: {}",
                targetId::get, trackedApprovals::toString);
            if (getActionStateOperation() == null) {
                lock.writeLock().lock();
                try {
                    this.actionStateOperation = operationManager.getExternalActionState(targetId.get(),
                            trackedApprovals, new GetActionStatesCallback(targetId.get()));

                    getLogger().debug("The Get Action States operation invoked for {} actions; action state operation = {}",
                            trackedApprovals, actionStateOperation);
                } catch (InterruptedException | TargetNotFoundException | ProbeException | CommunicationException e) {
                    getLogger().warn("Error getting external action state", e);
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                getLogger().info("Previous getActionState operation is still processing: {}",
                    actionStateOperation);
            }
        } catch (Exception e) {
            // Do not rethrow the exception. This prevents rescheduling of all future
            // ScheduledExecutorService.scheduleWithFixedDelay(this::requestExternalStateUpdates...
            // As a result, without this catch, this method would never run again.
            getLogger().error("Unable to requestExternalStateUpdates due to exception", e);
        }
    }

    private GetActionState getActionStateOperation() {
        lock.readLock().lock();
        try {
            return this.actionStateOperation;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void updateActionStateOperation(GetActionState actionState) {
        lock.writeLock().lock();
        try {
            this.actionStateOperation = actionState;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Operation callback to receive results of action approval request to a action approval
     * backend.
     */
    private class ApproveActionsCallback implements OperationCallback<ActionApprovalResponse> {
        private final long targetId;

        ApproveActionsCallback(long targetId) {
            this.targetId = targetId;
        }

        @Override
        public void onSuccess(@Nonnull ActionApprovalResponse response) {
            updateActionApprovalOperation(null);
            getLogger().info("The Action Approval operation has been reset");
            if (getLogger().isDebugEnabled()) {
                for (ActionErrorDTO actionError : response.getErrorsList()) {
                    getLogger().debug("Error reported approving action {} at target {}: {}",
                            actionError.getActionOid(), targetId, actionError.getMessage());
                }
                for (Entry<Long, ExternalActionInfo> action : response.getActionStateMap()
                        .entrySet()) {
                    getLogger().debug(
                            "Successfully sent action {} to approval. Got an external Id: {}",
                            action.getKey(), action.getValue().getUrl());
                }
            }
            try {
                approvalSender.sendMessage(response);
                trackedApprovals.addAll(response.getActionStateMap().keySet());
            } catch (CommunicationException e) {
                getLogger().warn("Failed sending approval results", e);
            } catch (InterruptedException e) {
                getLogger().info("Thread interrupted while sending approval results", e);
            }
        }

        @Override
        public void onFailure(@Nonnull String error) {
            getLogger().warn("Error approving actions at target {}: {}", targetId, error);
            updateActionApprovalOperation(null);
        }
    }

    /**
     * Operation callback to receive action state updates from external action approval backend.
     */
    private class GetActionStatesCallback implements OperationCallback<GetActionStateResponse> {
        private final long targetId;

        GetActionStatesCallback(long targetId) {
            this.targetId = targetId;
        }

        @Override
        public void onSuccess(@Nonnull GetActionStateResponse response) {
            updateActionStateOperation(null);
            getLogger().info("The Action State operation has been reset");
            if (getLogger().isDebugEnabled()) {
                for (ActionErrorDTO actionError : response.getErrorsList()) {
                    getLogger().warn("Error reported updating action {} state at target {}: {}",
                            actionError.getActionOid(), targetId, actionError.getMessage());
                }
                for (Entry<Long, ActionResponseState> entry : response.getActionStateMap()
                        .entrySet()) {
                    getLogger().info(
                            "Received external state update for action {} with new state {}",
                            entry.getKey(), entry.getValue());
                }
            }
            try {
                actionStateSender.sendMessage(response);
                for (Entry<Long, ActionResponseState> entry : response.getActionStateMap()
                        .entrySet()) {
                    if (entry.getValue() == ActionResponseState.REJECTED) {
                        trackedApprovals.remove(entry.getKey());
                    }
                }
                getLogger().info("Successfully sent external states for actions {}",
                        response.getActionStateMap().keySet());
            } catch (CommunicationException e) {
                getLogger().warn("Failed sending action state updates", e);
            } catch (InterruptedException e) {
                getLogger().info("Thread interrupted while sending action state updates", e);
            }
        }

        @Override
        public void onFailure(@Nonnull String error) {
            getLogger().warn("Error updating action states at target {}: {}", targetId, error);
            updateActionStateOperation(null);
        }
    }
}
