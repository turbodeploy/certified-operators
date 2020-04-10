package com.vmturbo.topology.processor.communication;

import static org.mockito.Mockito.mock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for the {@link RemoteMediationServer}.
 */
public class RemoteMediationServerTest {

    private final IdentityProvider identityProvider = new IdentityProviderImpl(
        new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
            mock(IdentityDatabaseStore.class)),
            new HeuristicsMatcher()),
        new MapKeyValueStore(),
        new ProbeInfoCompatibilityChecker(),
        0L);

    private final ProbeStore probeStore = new TestProbeStore(identityProvider);

    private final RemoteMediationServer remoteMediationServer = Mockito.spy(
        new RemoteMediationServer(probeStore,
                                  Mockito.mock(ProbePropertyStore.class)));

    private final DiscoveryMessageHandler mockOperationMessageHandler =
        mock(DiscoveryMessageHandler.class);

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
        (ITransport<MediationServerMessage, MediationClientMessage>)mock(ITransport.class);

    private long probeId;
    private long targetId1 = 1;
    private long targetId2 = 2;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        final ProbeInfo probeInfo = Probes.defaultProbe;
        probeStore.registerNewProbe(probeInfo, transport);
        probeId = identityProvider.getProbeId(probeInfo);
        Mockito.when(mockOperationMessageHandler.expirationTime()).thenReturn(Long.MAX_VALUE);
    }

    @Test
    public void testOnTransportMessage() throws Exception {
        final DiscoveryRequest discoveryRequest = buildFullDiscoveryRequest();
        final MediationClientMessage mediationClientMessage = buildMediationClientMessage();
        Mockito.when(mockOperationMessageHandler.onMessage(mediationClientMessage))
                .thenReturn(HandlerStatus.IN_PROGRESS);
        remoteMediationServer.sendDiscoveryRequest(probeId, targetId1, discoveryRequest,
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

        expectedException.expect(ProbeException.class);
        expectedException.expectMessage("Probe for requested type is not registered: -1");

        remoteMediationServer.sendDiscoveryRequest(-1, targetId1, discoveryRequest,
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
        remoteMediationServer.sendDiscoveryRequest(probeId, targetId1, discoveryRequest,
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

        Assert.assertEquals(2, probeStore.getTransport(incrementalProbeId).size());
        final DiscoveryRequest discoveryRequest = buildIncrementalDiscoveryRequest();

        remoteMediationServer.sendDiscoveryRequest(incrementalProbeId, targetId1, discoveryRequest,
            mockOperationMessageHandler);
        remoteMediationServer.sendDiscoveryRequest(incrementalProbeId, targetId2, discoveryRequest,
            mockOperationMessageHandler);
        Mockito.verify(remoteMediationServer).getOrCreateTransportForTarget(incrementalProbeId,
            targetId1);
        Mockito.verify(remoteMediationServer).getOrCreateTransportForTarget(incrementalProbeId,
            targetId2);
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
}
