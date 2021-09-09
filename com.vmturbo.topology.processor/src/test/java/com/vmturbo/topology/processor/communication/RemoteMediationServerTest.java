package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.kvstore.KeyValueStoreOperationException;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.RequestTargetId;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.planexport.PlanExportMessageHandler;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for the {@link RemoteMediationServer}.
 */
public class RemoteMediationServerTest {

    private final IdentityProvider identityProvider = new IdentityProviderImpl(
        new MapKeyValueStore(),
        new ProbeInfoCompatibilityChecker(),
        0L, mock(IdentityDatabaseStore.class), 10, 0, false, mock(StaleOidManagerImpl.class));

    private final ProbeStore probeStore = new TestProbeStore(identityProvider);

    private final RemoteMediationServer remoteMediationServer = Mockito.spy(
        new RemoteMediationServer(probeStore,
                                  Mockito.mock(ProbePropertyStore.class),
            new ProbeContainerChooserImpl(probeStore)));

    private final DiscoveryMessageHandler mockOperationMessageHandler =
        mock(DiscoveryMessageHandler.class);

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
        (ITransport<MediationServerMessage, MediationClientMessage>)mock(ITransport.class);

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transportToClose =
            (ITransport<MediationServerMessage, MediationClientMessage>)mock(ITransport.class);

    private long probeId;
    private String targetIdentifyingValues1 = "1";
    private String targetIdentifyingValues2 = "2";
    private Target target1;
    private Target target2;
    private final String probeType = "fooProbe";
    private final String probeCategory = "fooCategory";
    private final ContainerInfo containerInfo = buildContainerInfo(probeType, probeCategory);

    /**
     * Argument captor for simulating a transport getting closed.
     */
    @Captor
    private ArgumentCaptor<EventHandler<MediationClientMessage>> eventHandlerArgumentCaptor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        final ProbeInfo probeInfo = Probes.defaultProbe;
        probeStore.registerNewProbe(probeInfo, transport);
        probeId = identityProvider.getProbeId(probeInfo);
        Mockito.when(mockOperationMessageHandler.expirationTime()).thenReturn(Long.MAX_VALUE);
        target1 = mock(Target.class);
        target2 = mock(Target.class);
        final TargetSpec spec = TargetSpec.newBuilder().setProbeId(probeId).build();
        Mockito.when(target1.getProbeId()).thenReturn(probeId);
        Mockito.when(target1.getSpec()).thenReturn(spec);
        Mockito.when(target1.getSerializedIdentifyingFields()).thenReturn(targetIdentifyingValues1);
        Mockito.when(target2.getProbeId()).thenReturn(probeId);
        Mockito.when(target2.getSerializedIdentifyingFields()).thenReturn(targetIdentifyingValues2);
        Mockito.when(target2.getSpec()).thenReturn(spec);
    }

    /**
     * Test that a transport gets successfully registered with the probe when registerTransport is
     * called and that it gets removed when the transport is closed.
     *
     * @throws ProbeException when ProbeStore throws it.
     */
    @Test
    public void testTransportClosed() throws ProbeException {
        // capture the eventhandler for the transport
        remoteMediationServer.registerTransport(containerInfo, transportToClose);
        verify(transportToClose).addEventHandler(eventHandlerArgumentCaptor.capture());
        // verify that the transport is registered for the probeType "fooProbe"
        final Optional<Long> newProbeId = probeStore.getProbeIdForType(probeType);
        assertTrue(newProbeId.isPresent());
        final Long probeIdVal = newProbeId.get();
        // confirm that transport was registered with probe
        assertEquals(transportToClose, probeStore.getTransport(probeIdVal).iterator().next());
        // now simulate transport closed
        eventHandlerArgumentCaptor.getValue().onClose();
        // make sure transport was removed
        expectedException.expect(ProbeException.class);
        probeStore.getTransport(probeIdVal);
    }

    /**
     * Check that when a transport closes before the probe is registered, we don't register the
     * endpoint.
     *
     * @throws ProbeException when ProbeStore throws it.
     */
    @Test
    public void testTransportClosedBeforeRegistered() throws ProbeException {
        // simulate transport already being closed by the time it is registered
        doThrow(new IllegalStateException("transport closed"))
                .when(transportToClose).addEventHandler(any());
        remoteMediationServer.registerTransport(containerInfo, transportToClose);
        // verify that there are no transports registered for the probeType "fooProbe" even though
        // the probeType was registered (i.e. the probe has an ID)
        final Optional<Long> newProbeId = probeStore.getProbeIdForType(probeType);
        assertTrue(newProbeId.isPresent());
        expectedException.expect(ProbeException.class);
        probeStore.getTransport(newProbeId.get());
    }

    /**
     * Test that transport gets closed when an unexpected exception happens while registering a
     * probe.  This can happen, for example, if Consul is not up while a probe tries to register
     * with TP.
     *
     * @throws ProbeException when there is a problem with the ProbeInfo.
     */
    @Test
    public void testProbeStoreThrowsUncheckedException() throws ProbeException {
        final ProbeStore mockProbeStore = mock(ProbeStore.class);

        final RemoteMediationServer remoteMediationServer = Mockito.spy(
                new RemoteMediationServer(mockProbeStore,
                        Mockito.mock(ProbePropertyStore.class),
                        new ProbeContainerChooserImpl(mockProbeStore)));
        // Simulate an unchecked exception when we try to register the probe with the probeStore
        final String errorMessage = "Test Exception";
        final RuntimeException exception = new RuntimeException();
        doThrow(new KeyValueStoreOperationException(errorMessage, exception)).when(mockProbeStore)
                .registerNewProbe(any(), any());
        try {
            remoteMediationServer.registerTransport(containerInfo, transportToClose);
            fail();
        } catch (KeyValueStoreOperationException e) {
            assertEquals(errorMessage, e.getMessage());
            // verify that transport is closed on unchecked exception
            verify(transportToClose).close();
        }
    }

    @Test
    public void testOnTransportMessage() throws Exception {
        final DiscoveryRequest discoveryRequest = buildFullDiscoveryRequest();
        final MediationClientMessage mediationClientMessage = buildMediationClientMessage();
        Mockito.when(mockOperationMessageHandler.onMessage(mediationClientMessage))
                .thenReturn(HandlerStatus.IN_PROGRESS);
        remoteMediationServer.sendDiscoveryRequest(target1, discoveryRequest,
            mockOperationMessageHandler);
        remoteMediationServer.onTransportMessage(transport, mediationClientMessage);

        Mockito.verify(mockOperationMessageHandler).onReceive(mediationClientMessage);
    }

    @Test
    public void testSendDiscoveryRequestWithBadProbe() throws Exception {
        final DiscoveryRequest discoveryRequest = DiscoveryRequest.newBuilder()
            .setDiscoveryType(DiscoveryType.FULL)
            .setProbeType("probe type")
            .build();
        Target targetWithBadProbe = mock(Target.class);
        Mockito.when(targetWithBadProbe.getProbeId()).thenReturn(-1L);
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage("Probe -1 is not registered");
        remoteMediationServer.sendDiscoveryRequest(targetWithBadProbe, discoveryRequest,
            mockOperationMessageHandler);
    }

    @Test
    public void testRemoveMessageHandlers() throws Exception {
        final DiscoveryRequest discoveryRequest = buildFullDiscoveryRequest();
        final MediationClientMessage mediationClientMessage = buildMediationClientMessage();

        Discovery mockOperation = mock(Discovery.class);
        long targetId = 12L;
        Mockito.when(mockOperation.getTargetId()).thenReturn(targetId);
        Mockito.when(mockOperationMessageHandler.getOperation()).thenReturn(mockOperation);
        Mockito.when(mockOperationMessageHandler.onMessage(mediationClientMessage))
                .thenReturn(HandlerStatus.IN_PROGRESS);
        remoteMediationServer.sendDiscoveryRequest(target1, discoveryRequest,
            mockOperationMessageHandler);
        TargetUpdateRequest request = TargetUpdateRequest.newBuilder()
                        .setProbeType("qqq")
                        .setUpdateType(UpdateType.DELETED)
                        .build();
        remoteMediationServer.handleTargetRemoval(probeId, targetId, request);
        remoteMediationServer.onTransportMessage(transport, mediationClientMessage);

        Mockito.verify(mockOperationMessageHandler, Mockito.never()).onReceive(mediationClientMessage);
    }

    /**
     * Register two instances of the same probe. Both of them support incremental discovery. Test
     * that we assign the transport to the instances using getTransportForPersistentProbe, and
     * not round robin.
     * @throws Exception if the probe can't be found in the ProbeStore
     */
    @Test
    public void testDiscoveryMessageWithPersistentProbes() throws Exception {
        final ProbeInfo incrementalProbeInfo1 = Probes.incrementalProbe;
        final ProbeInfo incrementalProbeInfo2 = Probes.incrementalProbe;
        final ITransport<MediationServerMessage, MediationClientMessage> transport2 =
            (ITransport<MediationServerMessage, MediationClientMessage>)mock(ITransport.class);
        probeStore.registerNewProbe(incrementalProbeInfo1, transport);
        probeStore.registerNewProbe(incrementalProbeInfo2, transport2);

        long incrementalProbeId = identityProvider.getProbeId(incrementalProbeInfo1);

        assertEquals(2, probeStore.getTransport(incrementalProbeId).size());

        final DiscoveryRequest discoveryRequest = buildIncrementalDiscoveryRequest();
        Mockito.when(target1.getProbeId()).thenReturn(incrementalProbeId);
        Mockito.when(target2.getProbeId()).thenReturn(incrementalProbeId);

        remoteMediationServer.sendDiscoveryRequest(target1, discoveryRequest,
            mockOperationMessageHandler);
        remoteMediationServer.sendDiscoveryRequest(target2, discoveryRequest,
            mockOperationMessageHandler);

        verify(transport).send(any());
        verify(transport2).send(any());
    }

    private DiscoveryRequest buildFullDiscoveryRequest() {
        return DiscoveryRequest.newBuilder()
            .setProbeType("probe type")
            .setDiscoveryType(DiscoveryType.FULL)
            .build();
    }

    private DiscoveryRequest buildIncrementalDiscoveryRequest() {
        return DiscoveryRequest.newBuilder()
            .setProbeType("probe type")
            .setDiscoveryType(DiscoveryType.INCREMENTAL)
            .build();
    }

    private MediationClientMessage buildMediationClientMessage() {
        return MediationClientMessage.newBuilder()
            .setDiscoveryResponse(DiscoveryResponse.newBuilder()
                    .addEntityDTO(
                        EntityDTO.newBuilder()
                            .setEntityType(EntityType.VIRTUAL_MACHINE)
                            .setId("vm-1")
                    )
            ).build();
    }

    private ContainerInfo buildContainerInfo(String probeType, String probeCategory) {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeType(probeType)
                .setProbeCategory(probeCategory)
                .build();
        return ContainerInfo.newBuilder()
                .addProbes(probeInfo)
                .build();
    }

    /**
     * Test that initiating a plan export sends messages to the probe. This uses a plan
     * with a large number of actions to ensure that chunking happens, and verifies
     * that several messages are sent.
     *
     * @throws Exception if the probe can't be found in the ProbeStore
     */
    @Test
    public void testPlanExportRequest() throws Exception {
        final ProbeInfo probeInfo = Probes.defaultProbe;

        long probeId = identityProvider.getProbeId(probeInfo);

        final PlanExportRequest exportRequest = buildPlanExportRequest();
        Mockito.when(target1.getProbeId()).thenReturn(probeId);

        final PlanExportMessageHandler handler = mock(PlanExportMessageHandler.class);

        remoteMediationServer.sendPlanExportRequest(target1, exportRequest, handler);

        verify(transport, atLeast(3)).send(any());
    }

    private PlanExportRequest buildPlanExportRequest() {
        PlanExportDTO.Builder builder = PlanExportDTO.newBuilder()
            .setPlanName("foo")
            .setMarketId("bar");

        // Create lots of actions so that the message will be chunked
        for (int i = 0; i < 5000; i++) {
            builder.addActions(ActionExecutionDTO.newBuilder()
                .setActionType(ActionType.MOVE)
                .build());
        }

        return PlanExportRequest.newBuilder()
            .setPlanData(builder)
            .setTarget(RequestTargetId.newBuilder()
                .setProbeType(Probes.defaultProbe.getProbeType())
                .build())
            .setPlanDestination(NonMarketEntityDTO.newBuilder()
                .setId("doesn't matter")
                .setEntityType(NonMarketEntityType.PLAN_DESTINATION)
                .build())
            .build();
    }
}
