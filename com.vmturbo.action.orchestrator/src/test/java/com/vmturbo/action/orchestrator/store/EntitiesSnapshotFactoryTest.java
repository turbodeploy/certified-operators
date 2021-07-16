package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.store.EntitiesSnapshotFactory.EntitiesSnapshot;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Metadata;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Tests for {@link EntitiesSnapshotFactory}.
 */
public class EntitiesSnapshotFactoryTest {
    private EntitiesSnapshotFactory entitiesSnapshotFactory;

    private ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    private SupplyChainCalculator supplyChainCalculator = mock(SupplyChainCalculator.class);

    private final long realtimeContextId = 7;
    private final long planContextId = 8;

    private final long timeToWaitMins = 1;

    private final long appServerId = 123L;

    private final SupplyChainNode scNode = SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.APPLICATION_COMPONENT.typeNumber())
            .putMembersByState(EntityState.POWERED_ON.getNumber(),
                    MemberList.newBuilder()
                            .addMemberOids(appServerId)
                            .build())
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        entitiesSnapshotFactory = new EntitiesSnapshotFactory(actionTopologyStore, realtimeContextId, timeToWaitMins, TimeUnit.MINUTES,
                supplyChainCalculator, clock);

    }

    /**
     * Test that realtime entities are collected from the topology graph and
     * included in the returned snapshot.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRealtimeEntities() throws Exception {
        Set<Long> targetEntities = Collections.singleton(10L);
        final ActionPartialEntity projEntity = ActionPartialEntity.newBuilder()
                .setOid(10L)
                .build();
        ActionRealtimeTopology actionRealtimeTopology = mock(ActionRealtimeTopology.class);
        when(actionTopologyStore.getSourceTopology(timeToWaitMins, TimeUnit.MINUTES)).thenReturn(Optional.of(actionRealtimeTopology));
        TopologyGraph<ActionGraphEntity> graph = mock(TopologyGraph.class);
        when(graph.entitiesOfType(any(EntityType.class))).thenReturn(Stream.empty());
        when(actionRealtimeTopology.entityGraph()).thenReturn(graph);
        ActionGraphEntity ge = mock(ActionGraphEntity.class);
        when(ge.getOid()).thenReturn(10L);
        when(ge.asPartialEntity()).thenReturn(projEntity);
        doAnswer(invocation -> Stream.of(ge)).when(graph).getEntities(targetEntities);

        EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(targetEntities, realtimeContextId);

        assertThat(entitiesSnapshot.getEntityMap(), is(Collections.singletonMap(10L, projEntity)));
    }

    /**
     * Test that plan entities are retrieved from repository and included in the returned snapshot.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPlanEntities() throws Exception {
        // ARRANGE
        TopologyEntityDTO host = TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                .setDisplayName("host")
                .setOid(2)
                .build();
        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setOid(3)
                .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setDisplayName("ba")
                .build();
        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setDisplayName("vm")
                .setOid(1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                        .setProviderId(host.getOid())
                        .build())
                .build();
        final RemoteIterator<ProjectedTopologyEntity> iterator = mockRemoteIterator(host, ba, vm);

        // Mock the supply chain of the business account to return the VM.
        when(supplyChainCalculator.getSupplyChainNodes(any(), eq(Collections.singleton(ba.getOid())), any(), any()))
                .thenReturn(Collections.singletonMap(ApiEntityType.VIRTUAL_MACHINE.typeNumber(), SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                            .addMemberOids(vm.getOid())
                            .build())
                    .build()));

        // ACT
        entitiesSnapshotFactory.onProjectedTopologyReceived(Metadata.newBuilder()
                // Only one entity actually involved in actions.
                .addEntitiesInvolvedInActions(1L)
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planContextId))
                .build(), iterator, null);

        // ASSERT
        final EntitiesSnapshot snapshot = entitiesSnapshotFactory.getEntitiesSnapshot(
                Sets.newHashSet(host.getOid(), vm.getOid()), planContextId);
        assertThat(snapshot.getTopologyType(), is(TopologyType.PROJECTED));
        // Assert that the host is not in the snapshot.
        assertThat(snapshot.getEntityMap().keySet(), containsInAnyOrder(vm.getOid(), host.getOid()));
        // Assert that the ownership graph was constructed correctly.
        assertThat(snapshot.getOwnershipGraph().getOwners(vm.getOid()).get(0).getOid(), is(ba.getOid()));
    }

    /**
     * Test that we clear constructed plan snapshots after enough time elapses to avoid caching
     * them indefinitely.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPlanSnapshotExpiry() throws Exception {
        TopologyEntityDTO host = TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                .setDisplayName("host")
                .setOid(2)
                .build();
        final RemoteIterator<ProjectedTopologyEntity> iterator = mockRemoteIterator(host);
        entitiesSnapshotFactory.onProjectedTopologyReceived(Metadata.newBuilder()
                // Only one entity actually involved in actions.
                .addEntitiesInvolvedInActions(1L)
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planContextId))
                .build(), iterator, null);

        // Clock hasn't moved forward, so cleanup shouldn't remove anything.
        assertThat(entitiesSnapshotFactory.cleanupQueuedSnapshots(), is(0));
        clock.addTime(timeToWaitMins, ChronoUnit.MINUTES);
        // We are one ms away from clearing
        assertThat(entitiesSnapshotFactory.cleanupQueuedSnapshots(), is(0));
        clock.addTime(1, ChronoUnit.MILLIS);

        assertThat(entitiesSnapshotFactory.cleanupQueuedSnapshots(), is(1));
    }

    private RemoteIterator<ProjectedTopologyEntity> mockRemoteIterator(TopologyEntityDTO... entities)
            throws InterruptedException, TimeoutException, CommunicationException {
        RemoteIterator<ProjectedTopologyEntity> entityRemoteIterator = mock(RemoteIterator.class);
        when(entityRemoteIterator.hasNext()).thenReturn(true, false);
        when(entityRemoteIterator.nextChunk()).thenReturn(Stream.of(entities)
                .map(e -> ProjectedTopologyEntity.newBuilder()
                        .setEntity(e)
                        .build())
                .collect(Collectors.toList()));
        return entityRemoteIterator;
    }

    /**
     * Test that the realtime ownership graph is properly constructed via the supply chain.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRealtimeOwnershipGraph() throws Exception {
        ActionGraphEntity ba = mock(ActionGraphEntity.class);
        when(ba.getOid()).thenReturn(10L);
        when(ba.asEntityWithConnections()).thenReturn(EntityWithConnections.newBuilder()
                .setOid(10L)
                .build());
        final long otherAppServerId = appServerId + 1;

        ActionRealtimeTopology actionRealtimeTopology = mock(ActionRealtimeTopology.class);
        when(actionTopologyStore.getSourceTopology(timeToWaitMins, TimeUnit.MINUTES)).thenReturn(Optional.of(actionRealtimeTopology));
        TopologyGraph<ActionGraphEntity> graph = mock(TopologyGraph.class);
        when(graph.entitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(invocation -> Stream.of(ba));
        when(graph.getEntities(any())).thenAnswer(invocation -> Stream.empty());

        when(supplyChainCalculator.getSupplyChainNodes(any(), any(), any(), any()))
                .thenReturn(Collections.singletonMap(ApiEntityType.APPLICATION_COMPONENT.typeNumber(), scNode));

        when(actionRealtimeTopology.entityGraph()).thenReturn(graph);

        EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(Sets.newHashSet(appServerId, otherAppServerId), realtimeContextId);

        verify(supplyChainCalculator).getSupplyChainNodes(eq(graph), eq(Collections.singleton(10L)), any(), any());

        assertThat(entitiesSnapshot.getOwnershipGraph().getOwners(appServerId), contains(ba.asEntityWithConnections()));
        assertThat(entitiesSnapshot.getOwnershipGraph().getOwners(otherAppServerId), is(Collections.emptyList()));
    }
}
