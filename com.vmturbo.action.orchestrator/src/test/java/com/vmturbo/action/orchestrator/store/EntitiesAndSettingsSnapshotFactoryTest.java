package com.vmturbo.action.orchestrator.store;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.EntitiesSnapshotFactory.EntitiesSnapshot;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.graph.OwnershipGraph;

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
    private GroupServiceMole groupServiceSpy = Mockito.spy(new GroupServiceMole());
    private ScheduleServiceMole scheduleServiceSpy = Mockito.spy(new ScheduleServiceMole());

    /**
     * Test gRPC server to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(spServiceSpy, groupServiceSpy, scheduleServiceSpy);

    private EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    private final AcceptedActionsDAO acceptedActionsStore = mock(AcceptedActionsDAO.class);

    private final EntitiesSnapshotFactory entitiesSnapshotFactory = mock(EntitiesSnapshotFactory.class);

    private final Map<Long, ActionPartialEntity> entitySnapshotEntities = ImmutableMap.of(1L,
        ActionPartialEntity.newBuilder()
            .setOid(1L)
            .build());

    private final OwnershipGraph<EntityWithConnections> ownershipGraph = OwnershipGraph.newBuilder(EntityWithConnections::getOid)
            .build();

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        entitySettingsCache = new EntitiesAndSettingsSnapshotFactory(grpcTestServer.getChannel(),
            REALTIME_TOPOLOGY_CONTEXT_ID,
            acceptedActionsStore,
            entitiesSnapshotFactory,
            false);

        EntitiesSnapshot entitiesSnapshot = new EntitiesSnapshot(entitySnapshotEntities,
                ownershipGraph, TopologyType.SOURCE);
        when(entitiesSnapshotFactory.getEntitiesSnapshot(any(), any(), anyLong(), anyLong()))
                .thenReturn(entitiesSnapshot);
    }

    /**
     * Test that setting the "strict topology id match" flag for settings affects the call
     * out to the group component for per-entity settings.
     */
    @Test
    public void testSnapshotStrictMatchEnabled() {
        EntitiesAndSettingsSnapshotFactory newFact = new EntitiesAndSettingsSnapshotFactory(grpcTestServer.getChannel(),
                REALTIME_TOPOLOGY_CONTEXT_ID,
                acceptedActionsStore,
                entitiesSnapshotFactory,
                // The important change.
                true);

        newFact.newSnapshot(
                Collections.singleton(ENTITY_ID), Collections.emptySet(), REALTIME_TOPOLOGY_CONTEXT_ID,
                TOPOLOGY_ID);

        Mockito.verify(spServiceSpy).getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                        // Topology ID explicitly set.
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addEntities(ENTITY_ID))
                .setIncludeSettingPolicies(true)
                .build());
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

        when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID))
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
        when(groupServiceSpy.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                .addEntityId(ENTITY_ID)
                .addGroupType(GroupType.RESOURCE)
                .build())).thenReturn(GetGroupsForEntitiesResponse.newBuilder()
                .putEntityGroup(ENTITY_ID,
                        Groupings.newBuilder().addGroupId(ASSOCIATED_RESOURCE_GROUP_ID).build())
                .build());

        when(scheduleServiceSpy.getSchedules(ScheduleProto.GetSchedulesRequest.newBuilder().build()))
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
                Collections.emptySet(), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        Assert.assertTrue(snapshot.getSettingsForEntity(ENTITY_ID).isEmpty());
    }
}
