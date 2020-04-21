package com.vmturbo.topology.processor.api;

import static org.mockito.Matchers.any;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Fake remote mediation server, responding with the pre-defined responses to its calls.
 */
public class FakeRemoteMediation implements RemoteMediation {

    /**
     * Target id.
     */
    public static final String TGT_ID = "targetId";

    private final Map<String, ValidationResponse> validationResponses = new HashMap<>();

    private final Map<String, DiscoveryResponse> discoveryResponses = new HashMap<>();

    private ActionMessageHandler actionMessageHandler;

    private final TargetStore targetStore;

    public FakeRemoteMediation(TargetStore targetStore) {
        this.targetStore = targetStore;
    }

    @Override
    public Set<ProbeInfo> getConnectedProbes() {
        return Collections.emptySet();
    }

    @Override
    public int sendDiscoveryRequest(Target target,
                                     DiscoveryRequest discoveryRequest,
            IOperationMessageHandler<Discovery> responseHandler)
            throws ProbeException, CommunicationException, InterruptedException {
        final DiscoveryResponse response = discoveryResponses.get(String.valueOf(target.getId()));
        Assert.assertNotNull(response);
        responseHandler.onReceive(
                        MediationClientMessage.newBuilder().setDiscoveryResponse(response).build());
        responseHandler.onReceive(MediationClientMessage.newBuilder()
                .setDiscoveryResponse(DiscoveryResponse.getDefaultInstance())
                .build());
        return 0;
    }

    @Override
    public void sendValidationRequest(Target target, ValidationRequest validationRequest,
            IOperationMessageHandler<Validation> validationMessageHandler)
                    throws InterruptedException, ProbeException, CommunicationException {
        final String targetId = validationRequest.getAccountValueList().stream()
                        .filter(av -> av.getKey().equals(TGT_ID)).findFirst().get()
                        .getStringValue();
        final ValidationResponse response = validationResponses.get(targetId);
        Assert.assertNotNull(response);
        validationMessageHandler.onReceive(MediationClientMessage.newBuilder()
                        .setValidationResponse(response).build());
    }

    @Override
    public void sendActionRequest(@Nonnull Target target,
                                  @Nonnull ActionRequest actionRequest,
                                  @Nonnull IOperationMessageHandler<Action> actionMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        this.actionMessageHandler = (ActionMessageHandler)actionMessageHandler;
    }

    @Override
    public void sendSetPropertiesRequest(long probeId, @Nonnull SetProperties setProperties)
            throws InterruptedException, ProbeException, CommunicationException {
    }

    @Override
    public void handleTargetRemoval(long probeId, long targetId, TargetUpdateRequest request)
                    throws CommunicationException, InterruptedException, ProbeException {
    }

    @Override
    public int checkForExpiredHandlers() {
        return 0;
    }

    @Override
    public Clock getMessageHandlerExpirationClock() {
        return Clock.systemUTC();
    }

    public void addDiscoveryResponse(long targetId, DiscoveryResponse response) {
        discoveryResponses.put(String.valueOf(targetId), response);
    }

    public void addValidationResponse(long targetId, ValidationResponse response) {
        validationResponses.put(getAvId(targetId), response);
    }

    @Nonnull
    public ActionMessageHandler getActionMessageHandler() {
        return Objects.requireNonNull(actionMessageHandler);
    }

    private String getAvId(long targetId) {
        final GroupScopeResolver groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());
        final Target target = targetStore.getTarget(targetId).get();
        final String tgtId = target.getMediationAccountVals(groupScopeResolver).stream()
                        .filter(av -> av.getKey().equals(TGT_ID)).findFirst().get()
                        .getStringValue();
        return tgtId;
    }
}
