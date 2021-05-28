package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.store.EntitiesSnapshotFactory.EntitiesSnapshot;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.QueuedTopologyRequest;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Tests for {@link EntitiesSnapshotFactory}.
 */
public class EntitiesSnapshotFactoryTest {

    private RepositoryServiceMole repoServiceSpy = spy(RepositoryServiceMole.class);

    private SupplyChainServiceMole scSpy = spy(SupplyChainServiceMole.class);

    /**
     * GRPC test server.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(repoServiceSpy, scSpy);

    private EntitiesSnapshotFactory entitiesSnapshotFactory;

    private ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    private QueuedTopologyRequest queuedTopologyRequest = mock(QueuedTopologyRequest.class);
    private TopologyAvailabilityTracker topologyAvailabilityTracker = mock(TopologyAvailabilityTracker.class);

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

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        entitiesSnapshotFactory = new EntitiesSnapshotFactory(actionTopologyStore, realtimeContextId, timeToWaitMins, TimeUnit.MINUTES,
                grpcTestServer.getChannel(), topologyAvailabilityTracker, supplyChainCalculator);

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

        EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(targetEntities, Collections.emptySet(), realtimeContextId, null);

        assertThat(entitiesSnapshot.getEntityMap(), is(Collections.singletonMap(10L, projEntity)));
    }

    /**
     * Test that plan entities are retrieved from repository and included in the returned snapshot.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPlanEntities() {
        Set<Long> targetEntities = Collections.singleton(10L);
        Set<Long> nonProjectedEntities = Collections.singleton(20L);
        final ActionPartialEntity projEntity = ActionPartialEntity.newBuilder()
                .setOid(10L)
                .build();
        final ActionPartialEntity srcEntity = ActionPartialEntity.newBuilder()
                .setOid(20)
                .build();

        doAnswer(invocation -> {
            RetrieveTopologyEntitiesRequest req = invocation.getArgumentAt(0, RetrieveTopologyEntitiesRequest.class);
            if (req.getTopologyContextId() == planContextId) {
                return Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder()
                                .setAction(req.getTopologyType() == TopologyType.PROJECTED ? projEntity : srcEntity)
                                .build())
                        .build());
            } return Lists.newArrayList();
        }).when(repoServiceSpy).retrieveTopologyEntities(any());

        when(topologyAvailabilityTracker.queueTopologyRequest(planContextId, 1L)).thenReturn(queuedTopologyRequest);
        when(topologyAvailabilityTracker.queueAnyTopologyRequest(planContextId, TopologyType.SOURCE)).thenReturn(queuedTopologyRequest);
        EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(targetEntities, nonProjectedEntities, planContextId, 1L);

        verify(topologyAvailabilityTracker).queueTopologyRequest(planContextId, 1L);
        verify(topologyAvailabilityTracker).queueAnyTopologyRequest(planContextId, TopologyType.SOURCE);

        assertThat(entitiesSnapshot.getEntityMap().keySet(), containsInAnyOrder(projEntity.getOid(), srcEntity.getOid()));
        assertThat(entitiesSnapshot.getEntityMap().get(projEntity.getOid()), is(projEntity));
        assertThat(entitiesSnapshot.getEntityMap().get(srcEntity.getOid()), is(srcEntity));
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

        EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(Sets.newHashSet(appServerId, otherAppServerId), Collections.emptySet(), realtimeContextId, null);

        verify(supplyChainCalculator).getSupplyChainNodes(eq(graph), eq(Collections.singleton(10L)), any(), any());

        assertThat(entitiesSnapshot.getOwnershipGraph().getOwners(appServerId), contains(ba.asEntityWithConnections()));
        assertThat(entitiesSnapshot.getOwnershipGraph().getOwners(otherAppServerId), is(Collections.emptyList()));
    }

    /**
     * Test creating ownerShip graph and check that there is OWNS connection between entity and
     * associated business account.
     */
    @Test
    public void testPlanOwnershipGraph() {
        final long businessAccountId = 12L;
        final String businessAccountName = "Test Business Account";
        final long appServerIdNotRelatedToBA = 1234L;

        final EntityWithConnections businessAccountEntity = EntityWithConnections.newBuilder()
                .setDisplayName(businessAccountName)
                .setOid(businessAccountId)
                .build();

        final GetMultiSupplyChainsResponse supplyChainsResponse =
                GetMultiSupplyChainsResponse.newBuilder()
                        .setSeedOid(businessAccountId)
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(SupplyChainNode.newBuilder()
                                        .setEntityType(ApiEntityType.APPLICATION_COMPONENT.typeNumber())
                                        .putMembersByState(EntityState.POWERED_ON.getNumber(),
                                                MemberList.newBuilder()
                                                        .addMemberOids(appServerId)
                                                        .build())
                                        .build())
                                .build())
                        .build();

        when(repoServiceSpy.retrieveTopologyEntities(any())).thenReturn(
                Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder()
                                .setWithConnections(businessAccountEntity)
                                .build())
                        .build()));

        when(scSpy.getMultiSupplyChains(any())).thenReturn(
                Collections.singletonList(supplyChainsResponse));

        when(topologyAvailabilityTracker.queueTopologyRequest(planContextId, 1L)).thenReturn(queuedTopologyRequest);
        final EntitiesSnapshot snapshot = entitiesSnapshotFactory.getEntitiesSnapshot(
                Sets.newHashSet(Arrays.asList(appServerId, appServerIdNotRelatedToBA)),
                Collections.emptySet(), planContextId, 1L);

        assertThat(snapshot.getOwnershipGraph().getOwners(appServerId),
                contains(businessAccountEntity));
        assertThat(snapshot.getOwnershipGraph().getOwners(appServerIdNotRelatedToBA), is(Collections.emptyList()));
    }
}
