package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.QueuedTopologyRequest;

/**
 * Unit tests for {@link EntitiesAndSettingsSnapshotFactory}.
 */
public class EntitiesAndSettingsSnapshotFactoryTest {
    private static final long TOPOLOGY_ID = 7L;
    private static final long TOPOLOGY_CONTEXT_ID = 77L;
    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 77L;
    private static final long ENTITY_ID = 1L;
    private static final String VM_CLASSIC_ENTITY_TYPE = "VirtualMachine";
    private static final Long ASSOCIATED_RESOURCE_GROUP_ID = 123L;

    private SettingPolicyServiceMole spServiceSpy = spy(new SettingPolicyServiceMole());
    private RepositoryServiceMole repoServiceSpy = spy(new RepositoryServiceMole());
    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());
    private SupplyChainServiceMole supplyChainServiceSpy = spy(new SupplyChainServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(spServiceSpy, repoServiceSpy, groupServiceSpy,
                    supplyChainServiceSpy);

    private EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    private final QueuedTopologyRequest topologyRequest = mock(QueuedTopologyRequest.class);
    private TopologyAvailabilityTracker topologyAvailabilityTracker = mock(TopologyAvailabilityTracker.class);

    private static final long MIN_TO_WAIT = 1;

    @Before
    public void setup() {
        entitySettingsCache = new EntitiesAndSettingsSnapshotFactory(grpcTestServer.getChannel(),
            grpcTestServer.getChannel(),
            REALTIME_TOPOLOGY_CONTEXT_ID,
            topologyAvailabilityTracker, MIN_TO_WAIT, TimeUnit.MINUTES);

        when(topologyAvailabilityTracker.queueTopologyRequest(anyLong(), anyLong()))
            .thenReturn(topologyRequest);
    }

    /**
     * Test that creating a new snapshot makes the necessary remote calls and builds up the
     * inner maps.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNewSnapshot() throws Exception {
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("foo")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();

        final ServiceEntityApiDTO entityDto = new ServiceEntityApiDTO();
        entityDto.setUuid(Long.toString(ENTITY_ID));
        entityDto.setClassName(VM_CLASSIC_ENTITY_TYPE);

        when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
            .setTopologySelection(TopologySelection.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                .setTopologyId(TOPOLOGY_ID))
            .setSettingFilter(EntitySettingFilter.newBuilder()
                .addEntities(ENTITY_ID))
            .build()))
            .thenReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
                .addSettingGroup(EntitySettingGroup.newBuilder()
                    .setSetting(setting)
                    .addEntityOids(ENTITY_ID))
                .build()));
        when(groupServiceSpy.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
            .addEntityId(ENTITY_ID)
            .addGroupType(GroupType.RESOURCE)
            .build())).thenReturn(GetGroupsForEntitiesResponse.newBuilder()
            .putEntityGroup(ENTITY_ID,
                Groupings.newBuilder().addGroupId(ASSOCIATED_RESOURCE_GROUP_ID).build())
            .build());

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
            Collections.singleton(ENTITY_ID), REALTIME_TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        final Map<String, Setting> newSettings = snapshot.getSettingsForEntity(ENTITY_ID);
        assertTrue(newSettings.containsKey(setting.getSettingSpecName()));
        assertThat(newSettings.get(setting.getSettingSpecName()), is(setting));
        assertEquals(snapshot.getResourceGroupForEntity(ENTITY_ID).get(), ASSOCIATED_RESOURCE_GROUP_ID);

        verify(topologyAvailabilityTracker).queueTopologyRequest(REALTIME_TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);
        verify(topologyRequest).waitForTopology(MIN_TO_WAIT, TimeUnit.MINUTES);
    }

    /**
     * Test RPC error when creating a new snapshot.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNewSnapshotError() {
        when(spServiceSpy.getEntitySettingsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));
        when(groupServiceSpy.getGroupsForEntities(any())).thenReturn(
            GetGroupsForEntitiesResponse.getDefaultInstance());

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(Collections.singleton(ENTITY_ID),
            TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        assertTrue(snapshot.getSettingsForEntity(ENTITY_ID).isEmpty());
    }

    /**
     * Test creating ownerShip graph and check that there is OWNS connection between entity and
     * associated business account.
     */
    @Test
    public void testOwnershipGraph() {
        final long businessAccountId = 12L;
        final String businessAccountName = "Test Business Account";
        final long appServerId = 123L;
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
                                        .setEntityType(EntityType.APPLICATION_SERVER.name())
                                        .putMembersByState(EntityState.POWERED_ON.getNumber(),
                                                MemberList.newBuilder()
                                                        .addMemberOids(appServerId)
                                                        .build())
                                        .build())
                                .build())
                        .build();

        when(repoServiceSpy.retrieveTopologyEntities(Mockito.any())).thenReturn(
                Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder()
                                .setWithConnections(businessAccountEntity)
                                .build())
                        .build()));

        when(supplyChainServiceSpy.getMultiSupplyChains(Mockito.any())).thenReturn(
                Collections.singletonList(supplyChainsResponse));

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
                Sets.newHashSet(Arrays.asList(appServerId, appServerIdNotRelatedToBA)),
                Collections.emptySet(), REALTIME_TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        assertEquals(snapshot.getOwnerAccountOfEntity(appServerId).get(), businessAccountEntity);
        assertFalse(snapshot.getOwnerAccountOfEntity(appServerIdNotRelatedToBA).isPresent());
    }
}
