package com.vmturbo.mediation.client.it;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.common.tests.util.IRemoteMediation;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.SdkTarget;
import com.vmturbo.mediation.common.tests.util.TestConstants;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportResult;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Implementation of {@link IRemoteMediation}, Topology-processor specific.
 */
public class RemoteMediationImpl implements IRemoteMediation {

    private final RemoteMediation remoteMediation;
    private final ProbeStore probeStore;

    public RemoteMediationImpl(RemoteMediation remoteMediation, ProbeStore probeStore) {
        this.remoteMediation = remoteMediation;
        this.probeStore = probeStore;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull SdkTarget target) {
        final ValidationRequest request =
                        ValidationRequest.newBuilder().addAllAccountValue(target.getAccountValues())
                                        .setProbeType(target.getProbe().getType()).build();
        final long probeId = getProbeId(target.getProbe());
        final ValidationCaptor captor = new ValidationCaptor();
        try {
            remoteMediation.sendValidationRequest(probeId, request, captor.getHandler());
            return captor.getResult();
        } catch (ProbeException | CommunicationException | InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error during validation", e);
        }
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull SdkTarget target) {
        final DiscoveryRequest request =
                        DiscoveryRequest.newBuilder()
                            .addAllAccountValue(target.getAccountValues())
                            .setDiscoveryType(DiscoveryType.FULL)
                            .setProbeType(target.getProbe().getType())
                            .build();
        final long probeId = getProbeId(target.getProbe());
        final DiscoveryCaptor captor = new DiscoveryCaptor();
        try {
            remoteMediation.sendDiscoveryRequest(probeId, request, captor.getHandler());
            return captor.getResult();
        } catch (ProbeException | CommunicationException | InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error during validation", e);
        }
    }

    @Override
    public void executeAction(@Nonnull SdkTarget target, @Nonnull ActionExecutionDTO action,
            @Nonnull ActionResultProcessor progressListener) {
        final long probeId = getProbeId(target.getProbe());
        final OperationManager opManager = Mockito.mock(OperationManager.class);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ActionResult result = invocation.getArgumentAt(1, ActionResult.class);
                progressListener.updateActionItemState(
                        result.getResponse().getActionResponseState(),
                        result.getResponse().getResponseDescription(),
                        result.getResponse().getProgress());
                return null;
            }
        })
                .when(opManager)
                .notifyActionResult(Mockito.any(Action.class), Mockito.any(ActionResult.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ActionProgress result = invocation.getArgumentAt(1, MediationClientMessage
                        .class).getActionProgress();
                progressListener.updateActionItemState(
                        result.getResponse().getActionResponseState(),
                        result.getResponse().getResponseDescription(),
                        result.getResponse().getProgress());
                return null;
            }
        })
                .when(opManager)
                .notifyProgress(Mockito.any(Operation.class),
                        Mockito.any(MediationClientMessage.class));
        final ActionMessageHandler handler =
                new ActionMessageHandler(opManager, Mockito.mock(Action.class), Clock.systemUTC(),
                        TestConstants.TIMEOUT * 1000);
        try {
            remoteMediation.sendActionRequest(probeId, ActionRequest.newBuilder()
                    .setActionExecutionDTO(action)
                    .setProbeType(target.getProbe().getType())
                    .addAllAccountValue(target.getAccountValues())
                    .build(), handler);
        } catch (CommunicationException | InterruptedException | ProbeException e) {
            throw new RuntimeException("Error performing action on target " + target, e);
        }
    }

    @Nonnull
    @Override
    public ActionApprovalResponse approveActions(@Nonnull SdkTarget target,
                                                 @Nonnull Collection<ActionExecutionDTO> actionItems) throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    @Nonnull
    @Override
    public ActionErrorsResponse updateActionItemStates(@Nonnull SdkTarget target,
            @Nonnull Collection<ActionResponse> actionItem) throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    @Nonnull
    @Override
    public GetActionStateResponse getActionItemStates(@Nonnull SdkTarget target,
            @Nonnull Collection<Long> actionOids, boolean retrieveAllUnclosed)
            throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    @Nonnull
    @Override
    public ActionErrorsResponse auditActions(@Nonnull SdkTarget target,
            @Nonnull Collection<ActionEventDTO> actionEvents) throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    @Nonnull
    @Override
    public PlanExportResult exportPlan(@Nonnull final SdkTarget target, @Nonnull final PlanExportDTO exportData, @Nonnull final NonMarketEntityDTO planDestination, @Nonnull final PlanExportProgressProcessor progressProcessor) throws InterruptedException {
        throw new NotImplementedException("Feature is not implemented for in XL now");
    }

    private long getProbeId(SdkProbe probe) {
        return probeStore.getProbeIdForType(probe.getType()).get();
    }

    /**
     * Class represents special message handler to capture responses from the SDK probe.
     *
     * @param <T> type of message to capture.
     */
    private abstract static class MessageCaptor<T, O extends Operation>  {

        private final CompletableFuture<T> future;
        private final OperationManager operationManager;

        protected MessageCaptor() {
            future = new CompletableFuture<T>();
            operationManager = Mockito.mock(OperationManager.class);
            Mockito.doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    final String message = invocationOnMock.getArgumentAt(1, String.class);
                    future.completeExceptionally(new RuntimeException(message));
                    return null;
                }
            })
                    .when(operationManager)
                    .notifyOperationCancelled(Mockito.any(Operation.class), Mockito.anyString());
            Mockito.doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    final Integer seconds = invocationOnMock.getArgumentAt(1, Integer.class);
                    future.completeExceptionally(
                            new RuntimeException("Timed out after " + seconds + " secs")); return
                            null;
                }
            })
                    .when(operationManager)
                    .notifyTimeout(Mockito.any(Operation.class), Mockito.anyInt());
        }


        protected OperationManager getOperationManager() {
            return operationManager;
        }

        protected CompletableFuture<T> getFuture() {
            return future;
        }


        public T getResult() throws InterruptedException, ExecutionException {
            return future.get();
        }
    }

    /**
     * Validation response message captor.
     */
    private static class ValidationCaptor extends MessageCaptor<ValidationResponse, Validation> {

        private IOperationMessageHandler<Validation> handler;

        public ValidationCaptor() {
            handler = new ValidationMessageHandler(getOperationManager(),
                    Mockito.mock(Validation.class), Clock.systemUTC(),
                    TestConstants.TIMEOUT * 1000);
            final Answer answer = (invocationOnMock) -> {
                final ValidationResponse result =
                        invocationOnMock.getArgumentAt(1, ValidationResponse.class);
                getFuture().complete(result);
                return null;
            };
            Mockito.doAnswer(answer)
                    .when(getOperationManager())
                    .notifyValidationResult(Mockito.any(), Mockito.any(ValidationResponse.class));
        }

        public IOperationMessageHandler<Validation> getHandler() {
            return handler;
        }
    }

    /**
     * Discovery response message captor.
     */
    private static class DiscoveryCaptor extends MessageCaptor<DiscoveryResponse, Discovery> {

        private IOperationMessageHandler<Discovery> handler;

        public DiscoveryCaptor() {
            handler = new DiscoveryMessageHandler(getOperationManager(),
                    Mockito.mock(Discovery.class), Clock.systemUTC(), TestConstants.TIMEOUT * 1000);
            final Answer answer = (invocationOnMock) -> {
                final DiscoveryResponse result =
                        invocationOnMock.getArgumentAt(1, DiscoveryResponse.class);
                getFuture().complete(result);
                return null;
            };
            Mockito.doAnswer(answer)
                    .when(getOperationManager())
                    .notifyDiscoveryResult(Mockito.any(), Mockito.any(DiscoveryResponse.class));
        }

        public IOperationMessageHandler<Discovery> getHandler() {
            return handler;
        }
    }
}
