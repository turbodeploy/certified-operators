package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import io.grpc.Status;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
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
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoMoles.ScheduleServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
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
    private static final Long SCHEDULE_ID = 304L;
    private static final String SCHEDULE_DISPLAY_NAME = "TestSchedule";

    private SettingPolicyServiceMole spServiceSpy = Mockito.spy(new SettingPolicyServiceMole());
    private RepositoryServiceMole repoServiceSpy = Mockito.spy(new RepositoryServiceMole());
    private GroupServiceMole groupServiceSpy = Mockito.spy(new GroupServiceMole());
    private SupplyChainServiceMole supplyChainServiceSpy =
            Mockito.spy(new SupplyChainServiceMole());
    private ScheduleServiceMole scheduleServiceSpy = Mockito.spy(new ScheduleServiceMole());

    /**
     * Test gRPC server to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(spServiceSpy, repoServiceSpy, groupServiceSpy,
                    supplyChainServiceSpy, scheduleServiceSpy);

    private EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    private final QueuedTopologyRequest topologyRequest = Mockito.mock(QueuedTopologyRequest.class);
    private TopologyAvailabilityTracker topologyAvailabilityTracker =
            Mockito.mock(TopologyAvailabilityTracker.class);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);

    private static final long MIN_TO_WAIT = 1;

    @Before
    public void setup() {
        entitySettingsCache = new EntitiesAndSettingsSnapshotFactory(grpcTestServer.getChannel(),
            grpcTestServer.getChannel(),
            REALTIME_TOPOLOGY_CONTEXT_ID,
            topologyAvailabilityTracker, MIN_TO_WAIT, TimeUnit.MINUTES, acceptedActionsStore);

        Mockito.when(topologyAvailabilityTracker.queueTopologyRequest(Mockito.anyLong(),
                Mockito.anyLong()))
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
        final long associatedSettingsPolicyId = 11L;

        final ServiceEntityApiDTO entityDto = new ServiceEntityApiDTO();
        entityDto.setUuid(Long.toString(ENTITY_ID));
        entityDto.setClassName(VM_CLASSIC_ENTITY_TYPE);

        Mockito.when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addEntities(ENTITY_ID))
                .setIncludeSettingPolicies(true)
                .build()))
            .thenReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
                .addSettingGroup(EntitySettingGroup.newBuilder()
                        .setSetting(setting)
                        .addPolicyId(SettingPolicyId.newBuilder()
                                .setPolicyId(associatedSettingsPolicyId)
                                .build())
                        .addEntityOids(ENTITY_ID))
                .build()));
        Mockito.when(groupServiceSpy.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                .addEntityId(ENTITY_ID)
                .addGroupType(GroupType.RESOURCE)
                .build())).thenReturn(GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(ENTITY_ID,
                        Groupings.newBuilder().addGroupId(ASSOCIATED_RESOURCE_GROUP_ID).build())
                .build());

        Mockito.when(scheduleServiceSpy.getSchedules(ScheduleProto.GetSchedulesRequest.newBuilder().build()))
            .thenReturn(Collections.singletonList(ScheduleProto.Schedule.newBuilder().setId(SCHEDULE_ID)
                .setDisplayName(SCHEDULE_DISPLAY_NAME).build()));

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
            Collections.singleton(ENTITY_ID), Collections.emptySet(), REALTIME_TOPOLOGY_CONTEXT_ID,
                TOPOLOGY_ID);

        final Map<String, Setting> newSettings = snapshot.getSettingsForEntity(ENTITY_ID);
        Assert.assertTrue(newSettings.containsKey(setting.getSettingSpecName()));
        Assert.assertThat(newSettings.get(setting.getSettingSpecName()), CoreMatchers.is(setting));
        Assert.assertEquals(snapshot.getResourceGroupForEntity(ENTITY_ID).get(),
                ASSOCIATED_RESOURCE_GROUP_ID);

        final Map<String, Collection<Long>> settingPoliciesForEntity =
                snapshot.getSettingPoliciesForEntity(ENTITY_ID);
        Assert.assertEquals(1, settingPoliciesForEntity.entrySet().size());
        Assert.assertTrue(settingPoliciesForEntity.containsKey(setting.getSettingSpecName()));
        Assert.assertEquals(associatedSettingsPolicyId,
                settingPoliciesForEntity.get(setting.getSettingSpecName())
                        .iterator()
                        .next()
                        .longValue());

        Mockito.verify(topologyAvailabilityTracker).queueTopologyRequest(REALTIME_TOPOLOGY_CONTEXT_ID,
                TOPOLOGY_ID);
        Mockito.verify(topologyRequest).waitForTopology(MIN_TO_WAIT, TimeUnit.MINUTES);
    }

    /**
     * Test RPC error when creating a new snapshot.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNewSnapshotError() {
        Mockito.when(spServiceSpy.getEntitySettingsError(Mockito.any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));
        Mockito.when(groupServiceSpy.getGroupsForEntities(Mockito.any())).thenReturn(
                GetGroupsForEntitiesResponse.getDefaultInstance());

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(Collections.singleton(ENTITY_ID),
                Collections.emptySet(), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        Assert.assertTrue(snapshot.getSettingsForEntity(ENTITY_ID).isEmpty());
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
                                        .setEntityType(EntityType.APPLICATION_COMPONENT.name())
                                        .putMembersByState(EntityState.POWERED_ON.getNumber(),
                                                MemberList.newBuilder()
                                                        .addMemberOids(appServerId)
                                                        .build())
                                        .build())
                                .build())
                        .build();

        Mockito.when(repoServiceSpy.retrieveTopologyEntities(Mockito.any())).thenReturn(
                Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder()
                                .setWithConnections(businessAccountEntity)
                                .build())
                        .build()));

        Mockito.when(supplyChainServiceSpy.getMultiSupplyChains(Mockito.any())).thenReturn(
                Collections.singletonList(supplyChainsResponse));

        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
                Sets.newHashSet(Arrays.asList(appServerId, appServerIdNotRelatedToBA)),
                Collections.emptySet(), REALTIME_TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        Assert.assertEquals(snapshot.getOwnerAccountOfEntity(appServerId).get(),
                businessAccountEntity);
        Assert.assertFalse(snapshot.getOwnerAccountOfEntity(appServerIdNotRelatedToBA).isPresent());
    }
}
