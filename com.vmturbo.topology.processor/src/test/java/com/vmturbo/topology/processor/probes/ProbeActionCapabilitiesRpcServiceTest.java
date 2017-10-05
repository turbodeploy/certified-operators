package com.vmturbo.topology.processor.probes;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ProbeActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.SdkToProbeActionsConverter;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

/**
 * Tests for the service for getting action capabilities of probes.
 */
public class ProbeActionCapabilitiesRpcServiceTest {

    private static final long PROBE_ID = 1l;

    private static final long NOT_EXISTING_PROBE_ID = 2l;

    private static final TestObserver observer = new TestObserver();

    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);

    private final ProbeActionCapabilitiesRpcService service = new ProbeActionCapabilitiesRpcService(probeStore);

    private List<ActionPolicyDTO> sdkActionCapabilities;

    @Before
    public void setup() {
        initTestedProbeCapabilities();
        final ProbeInfo probeInfo =
                populateProbeInfo("ProbeType", "Category", sdkActionCapabilities);
        Mockito.when(probeStore.getProbe(PROBE_ID))
                .thenReturn(Optional.of(probeInfo));
        Mockito.when(probeStore.getProbe(NOT_EXISTING_PROBE_ID))
                .thenReturn(Optional.empty());
    }

    /**
     * Inits tested action policies of probe.
     */
    private void initTestedProbeCapabilities() {
        final ActionPolicyDTO changeActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.VIRTUAL_MACHINE,
                        ActionType.CHANGE);
        final ActionPolicyDTO startActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.PHYSICAL_MACHINE,
                        ActionType.START);
        final ActionPolicyDTO resizeActionPolicy =
                SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.PHYSICAL_MACHINE,
                        ActionType.RIGHT_SIZE);
        sdkActionCapabilities = Arrays.asList(changeActionPolicy, startActionPolicy, resizeActionPolicy);
    }

    /**
     * Tests getting all action capabilities of probe.
     */
    @Test
    public void testGetProbeActionCapabilities() {
        GetProbeActionCapabilitiesRequest request = GetProbeActionCapabilitiesRequest.newBuilder()
                .setProbeId(PROBE_ID).build();
        service.getProbeActionCapabilities(request, observer);
        Assert.assertEquals(SdkToProbeActionsConverter.convert(sdkActionCapabilities),
                observer.getActionCapabilities());
    }

    /**
     * Tests getting action capabilities of probe by not existing probeId
     */
    @Test
    public void testGetActionCapabilitiesOfNotExistingProbe() {
        GetProbeActionCapabilitiesRequest request = GetProbeActionCapabilitiesRequest.newBuilder()
                .setProbeId(NOT_EXISTING_PROBE_ID).build();
        service.getProbeActionCapabilities(request, observer);
    }

    /**
     * Tests getting action capabilities of probe filtered by entity type.
     */
    @Test
    public void testGetProbeActionCapabilitiesByEntityType() {
        GetProbeActionCapabilitiesRequest request = GetProbeActionCapabilitiesRequest.newBuilder()
                .setProbeId(PROBE_ID).setEntityType(EntityType.VIRTUAL_MACHINE.getNumber()).build();
        service.getProbeActionCapabilities(request, observer);
        ActionDTO.ProbeActionCapability expected = SdkToProbeActionsConverter.convert
                (SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED,
                        EntityType.VIRTUAL_MACHINE, ActionType.CHANGE));
        Assert.assertEquals(expected, observer.getActionCapabilities().get(0));
    }

    /**
     * Tests getting action capabilities of probe filtered by not exisitng in probe entity type
     */
    @Test
    public void testGetProbeActionCapabilitiesByNotExistingEntityType() {
        GetProbeActionCapabilitiesRequest request = GetProbeActionCapabilitiesRequest.newBuilder()
                .setProbeId(PROBE_ID).setEntityType(EntityType.BUSINESS.getNumber()).build();
        service.getProbeActionCapabilities(request, observer);
        Assert.assertEquals(ImmutableList.of(), observer.getActionCapabilities());
    }

    /**
     * Populates ProbeInfo builder by certain data.
     *
     * @param probeType Probe Type to set to ProbeInfo
     * @param category Probe category to set to probe
     * @param actionCapabilities policies to add to probeInfo.
     * @return populated builder
     */
    private static ProbeInfo populateProbeInfo(@Nonnull String probeType, @Nonnull String category,
            @Nonnull List<ActionPolicyDTO> actionCapabilities) {
        return ProbeInfo.newBuilder()
                .setProbeType(probeType)
                .setProbeCategory(category)
                .addAllActionPolicy(actionCapabilities)
                .build();
    }

    /**
     * Response observer for tests.
     */
    private static class TestObserver implements StreamObserver<GetProbeActionCapabilitiesResponse> {

        /**
         * Action capabilities from response.
         */
        private List<ActionDTO.ProbeActionCapability> actionCapabilities;

        @Override
        public void onNext(GetProbeActionCapabilitiesResponse getProbeActionCapabilitiesResponse) {
            actionCapabilities = getProbeActionCapabilitiesResponse.getActionCapabilitiesList();
        }

        @Override
        public void onError(Throwable throwable) {
            Assert.assertEquals(String.format("NOT_FOUND: There is no probe with probeId=%s",
                    NOT_EXISTING_PROBE_ID), throwable.getMessage());
        }

        @Override
        public void onCompleted() {}

        public List<ProbeActionCapability> getActionCapabilities() {
            return actionCapabilities;
        }

    }

}
