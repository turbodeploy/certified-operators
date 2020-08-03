package com.vmturbo.topology.processor.probes;

import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.Probe;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.GetProbeActionCapabilitiesResponse;
import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
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

    private static final long EMPTY_PROBE = 3l;

    private static final TestObserver observer = new TestObserver();

    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);

    private final ProbeActionCapabilitiesRpcService service = new ProbeActionCapabilitiesRpcService(probeStore);

    private List<ActionPolicyDTO> sdkActionCapabilities;

    @Before
    public void setup() {
        initTestedProbeCapabilities();
        final ProbeInfo probeInfo = populateProbeInfo(sdkActionCapabilities);
        final ProbeInfo emptyProbeInfo = populateProbeInfo(Collections.emptyList());
        when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(probeInfo));
        when(probeStore.getProbe(NOT_EXISTING_PROBE_ID)).thenReturn(Optional.empty());
        when(probeStore.getProbe(EMPTY_PROBE)).thenReturn(Optional.of(emptyProbeInfo));
        when(probeStore.getProbes()).thenReturn(
            ImmutableMap.of(PROBE_ID, probeInfo, EMPTY_PROBE, emptyProbeInfo));
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
        Probe.ProbeActionCapability expected = SdkToProbeActionsConverter.convert
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

    @Test
    public void testListProbeActionCapabilities() {
        ListProbeActionCapabilitiesRequest request = ListProbeActionCapabilitiesRequest
                .newBuilder().addProbeIds(PROBE_ID).addProbeIds(EMPTY_PROBE).build();
        ListProbeActionCapabilitiesTestObserver streamObserver
                = new ListProbeActionCapabilitiesTestObserver();
        service.listProbeActionCapabilities(request, streamObserver);
        Assert.assertEquals(SdkToProbeActionsConverter.convert(sdkActionCapabilities),
                streamObserver.getProbesCapabilities().get(PROBE_ID));
        Assert.assertEquals(Collections.emptyList(), streamObserver.getProbesCapabilities().get(
                EMPTY_PROBE));
    }

    @Test
    public void testListProbeActionCapabilitiesEmptyInputReturnsAll() {
        ListProbeActionCapabilitiesRequest request = ListProbeActionCapabilitiesRequest
            .newBuilder()
            .build();
        ListProbeActionCapabilitiesTestObserver streamObserver
            = new ListProbeActionCapabilitiesTestObserver();
        service.listProbeActionCapabilities(request, streamObserver);
        Assert.assertEquals(SdkToProbeActionsConverter.convert(sdkActionCapabilities),
            streamObserver.getProbesCapabilities().get(PROBE_ID));
        Assert.assertEquals(Collections.emptyList(), streamObserver.getProbesCapabilities().get(
            EMPTY_PROBE));
    }

    /**
     * Populates ProbeInfo builder by certain data.
     *
     * @param actionCapabilities policies to add to probeInfo.
     * @return populated builder
     */
    private static ProbeInfo populateProbeInfo(@Nonnull List<ActionPolicyDTO> actionCapabilities) {
        return ProbeInfo.newBuilder()
                .setProbeType("ProbeType")
                .setProbeCategory("Category")
                .setUiProbeCategory("uiProbeCat")
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
        private List<Probe.ProbeActionCapability> actionCapabilities;

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

    private static class ListProbeActionCapabilitiesTestObserver implements
            StreamObserver<ProbeActionCapabilities> {

        private final Map<Long, List<ProbeActionCapability>> probesCapabilities = new HashMap<>();

        public Map<Long, List<ProbeActionCapability>> getProbesCapabilities() {
            return probesCapabilities;
        }

        @Override
        public void onNext(ProbeActionCapabilities actionCapabilitiesOfProbe) {
            probesCapabilities.put(actionCapabilitiesOfProbe.getProbeId(),
                    actionCapabilitiesOfProbe.getActionCapabilitiesList());
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
    }

}
