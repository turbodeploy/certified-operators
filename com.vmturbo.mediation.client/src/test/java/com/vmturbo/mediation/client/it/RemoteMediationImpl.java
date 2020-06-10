package com.vmturbo.mediation.client.it;

import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.mockito.Mockito;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.common.tests.util.IRemoteMediation;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.SdkTarget;
import com.vmturbo.mediation.common.tests.util.TestConstants;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionAuditRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionUpdateStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportResult;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.TargetOperationException;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler.ActionOperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApprovalMessageHandler;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateStateMessageHandler;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionStateMessageHandler;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAuditMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Implementation of {@link IRemoteMediation}, Topology-processor specific.
 */
public class RemoteMediationImpl implements IRemoteMediation {

    private final RemoteMediation remoteMediation;
    private final ProbeStore probeStore;

    /**
     * Constructs test remote mediation instance for XL SDK.
     *
     * @param remoteMediation XL remote mediation instance
     * @param probeStore probe store
     */
    public RemoteMediationImpl(RemoteMediation remoteMediation, ProbeStore probeStore) {
        this.remoteMediation = remoteMediation;
        this.probeStore = probeStore;
    }

    @Nonnull
    private Target createTarget(@Nonnull SdkTarget target) {
        final Target targetMock = Mockito.mock(Target.class);
        Mockito.when(targetMock.getSerializedIdentifyingFields()).thenReturn(target.getTargetId());
        final long probeId = getProbeId(target.getProbe());
        Mockito.when(targetMock.getProbeId()).thenReturn(probeId);
        return targetMock;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull SdkTarget target)
            throws InterruptedException {
        final ValidationRequest request =
                        ValidationRequest.newBuilder().addAllAccountValue(target.getAccountValues())
                                        .setProbeType(target.getProbe().getType()).build();
        final Target targetMock = createTarget(target);
        final ValidationCallback callback = new ValidationCallback();
        final ValidationMessageHandler handler = new ValidationMessageHandler(
                Mockito.mock(Validation.class), Clock.systemUTC(), TestConstants.TIMEOUT * 1000,
                callback);
        try {
            remoteMediation.sendValidationRequest(targetMock, request, handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Error during validation", e);
        }
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull SdkTarget target) throws InterruptedException {
        final DiscoveryRequest request =
                        DiscoveryRequest.newBuilder()
                            .addAllAccountValue(target.getAccountValues())
                            .setDiscoveryType(DiscoveryType.FULL)
                            .setProbeType(target.getProbe().getType())
                            .build();
        final Target targetMock = createTarget(target);
        final DiscoveryCallback callback = new DiscoveryCallback();
        final DiscoveryMessageHandler handler = new DiscoveryMessageHandler(
                Mockito.mock(Discovery.class), Clock.systemUTC(), TestConstants.TIMEOUT * 1000,
                callback);
        try {
            remoteMediation.sendDiscoveryRequest(targetMock,
                request,
                handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Error during validation", e);
        }
    }

    @Override
    public void executeAction(@Nonnull SdkTarget target, @Nonnull ActionExecutionDTO action,
            @Nonnull ActionResultProcessor progressListener) throws InterruptedException {
        final Target targetMock = createTarget(target);
        final ActionCallback callback = new ActionCallback(progressListener);
        final ActionMessageHandler handler = new ActionMessageHandler(Mockito.mock(Action.class),
                Clock.systemUTC(), TestConstants.TIMEOUT * 1000, callback);
        try {
            remoteMediation.sendActionRequest(targetMock, ActionRequest.newBuilder()
                    .setActionExecutionDTO(action)
                    .setProbeType(target.getProbe().getType())
                    .addAllAccountValue(target.getAccountValues())
                    .build(), handler);
        } catch (CommunicationException | ProbeException e) {
            throw new RuntimeException("Error performing action on target " + target, e);
        }
    }

    @Nonnull
    @Override
    public ActionApprovalResponse approveActions(@Nonnull SdkTarget target,
            @Nonnull Collection<ActionExecutionDTO> actionItems)
            throws InterruptedException, TargetOperationException {
        final DefaultOperationCallback<ActionApprovalResponse> callback =
                new DefaultOperationCallback();
        final ActionApprovalMessageHandler handler = new ActionApprovalMessageHandler(
                Mockito.mock(ActionApproval.class), Clock.systemUTC(), TestConstants.TIMEOUT * 1000,
                callback);
        final ActionApprovalRequest request = ActionApprovalRequest.newBuilder().addAllAction(
                actionItems).setTarget(target.createTargetSpec()).build();
        final Target targetMock = createTarget(target);
        try {
            remoteMediation.sendActionApprovalsRequest(targetMock, request, handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | TimeoutException e) {
            throw new TargetOperationException(e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new TargetOperationException(e.getCause().getMessage(), e.getCause());
        }
    }

    @Nonnull
    @Override
    public ActionErrorsResponse updateActionItemStates(@Nonnull SdkTarget target,
            @Nonnull Collection<ActionResponse> actionItems)
            throws InterruptedException, TargetOperationException {
        final ActionUpdateStateRequest request =
                ActionUpdateStateRequest.newBuilder().addAllActionState(actionItems).setTarget(
                        target.createTargetSpec()).build();
        final DefaultOperationCallback<ActionErrorsResponse> callback =
                new DefaultOperationCallback<>();
        final ActionUpdateStateMessageHandler handler = new ActionUpdateStateMessageHandler(
                Mockito.mock(ActionUpdateState.class), Clock.systemUTC(),
                TestConstants.TIMEOUT * 1000, callback);
        final Target targetMock = createTarget(target);
        try {
            remoteMediation.sendActionUpdateStateRequest(targetMock, request, handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | TimeoutException e) {
            throw new TargetOperationException("Error updating action states", e);
        } catch (ExecutionException e) {
            throw new TargetOperationException(e.getCause().getMessage(), e.getCause());
        }
    }

    @Nonnull
    @Override
    public GetActionStateResponse getActionItemStates(@Nonnull SdkTarget target,
            @Nonnull Collection<Long> actionOids, boolean retrieveAllUnclosed)
            throws InterruptedException, TargetOperationException {
        final GetActionStateRequest request = GetActionStateRequest.newBuilder().addAllActionOid(
                actionOids).setIncludeAllActionsInTransition(retrieveAllUnclosed).setTarget(
                target.createTargetSpec()).build();
        final DefaultOperationCallback<GetActionStateResponse> callback =
                new DefaultOperationCallback<>();
        final GetActionStateMessageHandler handler = new GetActionStateMessageHandler(
                Mockito.mock(GetActionState.class), Clock.systemUTC(),
                TestConstants.TIMEOUT * 1000, callback);
        final Target targetMock = createTarget(target);
        try {
            remoteMediation.sendGetActionStatesRequest(targetMock, request, handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | TimeoutException e) {
            throw new TargetOperationException(e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new TargetOperationException(e.getCause().getMessage(), e.getCause());
        }
    }

    @Nonnull
    @Override
    public ActionErrorsResponse auditActions(@Nonnull SdkTarget target,
            @Nonnull Collection<ActionEventDTO> actionEvents)
            throws InterruptedException, TargetOperationException {
        final ActionAuditRequest request = ActionAuditRequest.newBuilder().addAllAction(
                actionEvents).setTarget(target.createTargetSpec()).build();
        final DefaultOperationCallback<ActionErrorsResponse> callback =
                new DefaultOperationCallback<>();
        final ActionAuditMessageHandler handler = new ActionAuditMessageHandler(
                Mockito.mock(ActionAudit.class), Clock.systemUTC(),
                TestConstants.TIMEOUT * 1000, callback);
        final Target targetMock = createTarget(target);
        try {
            remoteMediation.sendActionAuditRequest(targetMock, request, handler);
            return callback.getFuture().get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        } catch (ProbeException | CommunicationException | TimeoutException e) {
            throw new TargetOperationException(e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new TargetOperationException(e.getCause().getMessage(), e.getCause());
        }
    }

    @Nonnull
    @Override
    public PlanExportResult exportPlan(@Nonnull final SdkTarget target, @Nonnull final PlanExportDTO exportData, @Nonnull final NonMarketEntityDTO planDestination, @Nonnull final PlanExportProgressProcessor progressProcessor) throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    private long getProbeId(SdkProbe probe) {
        return probeStore.getProbeIdForType(probe.getType()).get();
    }

    @Nonnull
    private static ActionErrorDTO createError(@Nonnull String msg) {
        return ActionErrorDTO.newBuilder().setActionOid(-1).setMessage(msg).build();
    }

    /**
     * Abstract callback for operations.
     *
     * @param <R> type of operation result
     */
    private static class DefaultOperationCallback<R> implements OperationCallback<R> {
        private final CompletableFuture<R> future = new CompletableFuture<>();

        @Override
        public void onSuccess(@Nonnull R response) {
            future.complete(response);
        }

        @Override
        public void onFailure(@Nonnull String error) {
            future.completeExceptionally(new Exception(error));
        }

        @Nonnull
        public Future<R> getFuture() {
            return future;
        }

        @Nonnull
        protected CompletableFuture<R> getFutureInternal() {
            return future;
        }
    }

    /**
     * Operation callback abstraction for operations that translate the failure into a response.
     *
     * @param <R> type of an operation response.
     */
    private abstract static class TranslatingOperationCallback<R>
            extends DefaultOperationCallback<R> {
        @Override
        public void onFailure(@Nonnull String error) {
            getFutureInternal().complete(transformFailure(error));
        }

        @Nonnull
        protected abstract R transformFailure(@Nonnull String msg);
    }

    /**
     * Callback for discovery operation.
     */
    private static class DiscoveryCallback extends TranslatingOperationCallback<DiscoveryResponse> {
        @Nonnull
        @Override
        protected DiscoveryResponse transformFailure(@Nonnull String msg) {
            return DiscoveryResponse.newBuilder()
                    .addErrorDTO(SDKUtil.createCriticalError(msg))
                    .build();
        }
    }

    /**
     * Callback for validation operation.
     */
    private static class ValidationCallback
            extends TranslatingOperationCallback<ValidationResponse> {
        @Nonnull
        @Override
        protected ValidationResponse transformFailure(@Nonnull String msg) {
            return ValidationResponse.newBuilder()
                    .addErrorDTO(SDKUtil.createCriticalError(msg))
                    .build();
        }
    }

    /**
     * Callback for action execution operation.
     */
    private static class ActionCallback implements ActionOperationCallback {
        private final ActionResultProcessor actionResultProcessor;

        ActionCallback(@Nonnull ActionResultProcessor actionResultProcessor) {
            this.actionResultProcessor = Objects.requireNonNull(actionResultProcessor);
        }

        @Override
        public void onActionProgress(@Nonnull ActionProgress actionProgress) {
            final ActionResponse actionResponse = actionProgress.getResponse();
            actionResultProcessor.updateActionItemState(actionResponse.getActionResponseState(),
                    actionResponse.getResponseDescription(), actionResponse.getProgress());
        }

        @Override
        public void onSuccess(@Nonnull ActionResult response) {
            final ActionResponse actionResponse = response.getResponse();
            actionResultProcessor.updateActionItemState(actionResponse.getActionResponseState(),
                    actionResponse.getResponseDescription(), actionResponse.getProgress());
        }

        @Override
        public void onFailure(@Nonnull String error) {
            actionResultProcessor.updateActionItemState(ActionResponseState.FAILED, error, 100);
        }
    }
}
