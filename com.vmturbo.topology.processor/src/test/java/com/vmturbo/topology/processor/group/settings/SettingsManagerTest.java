package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

public class SettingsManagerTest {

    private final TopologyFilterFactory topologyFilterFactory =
        Mockito.mock(TopologyFilterFactory.class);

    private final GroupResolver groupResolver = Mockito.mock(GroupResolver.class);

    private TopologyGraph topologyGraph = Mockito.mock(TopologyGraph.class);

    private GroupServiceBlockingStub groupServiceClient;

    private SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());

    private final SettingPolicyServiceMole testSettingService = spy(new SettingPolicyServiceMole());

    private SettingsManager settingsManager;

    private final Long entityOid1 = 111L;
    private final Long entityOid2 = 222L;
    private final Long entityOid3 = 333L;
    private final Set<Long> entities = new HashSet<Long>(
                Arrays.asList(entityOid1, entityOid2, entityOid3));
    private final Long groupId1 = 111L;
    private final Group group = createGroup(groupId1, "group1");
    private final Long clusterId1 = 222L;

    private final Setting setting1 = createSettings("setting1", 10);
    private final Setting setting2 = createSettings("setting2", 20);
    private final List<Setting> inputSettings1  =
        new LinkedList<Setting>(Arrays.asList(setting1, setting2));
    private final SettingPolicy settingPolicy1 =
        createSettingPolicy(1L, "sp1", SettingPolicy.Type.USER, inputSettings1,
            Collections.singletonList(groupId1));

    private final Setting setting3 = createSettings("setting1", 20);
    private final Setting setting4 = createSettings("setting4", 50);
    private final List<Setting> inputSettings2  =
        new LinkedList<Setting>(Arrays.asList(setting1, setting2));
    private final SettingPolicy settingPolicy2 = createUserSettingPolicy(2L, "sp2", inputSettings2);
    private long defaultSettingPolicyId = 3L;
    private final SettingPolicy settingPolicy3 =
        createSettingPolicy(defaultSettingPolicyId, "sp3", SettingPolicy.Type.DEFAULT,
            inputSettings1, Collections.singletonList(groupId1));

    private final TopologyEntityDTO.Builder entity1 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid1)
            .setEntityType(1);

    private final TopologyEntityDTO.Builder entity2 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid2)
            .setEntityType(1);

    private final Vertex vertex1 = new Vertex(entity1);
    private final Vertex vertex2 = new Vertex(entity2);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
            testSettingService);

    @Before
    public void setup() throws Exception {
        settingPolicyServiceClient = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingsManager = new SettingsManager(settingPolicyServiceClient, groupServiceClient,
                topologyFilterFactory);
    }

    @Test
    public void testApplyUserSettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1, vertex2).stream());
        when(testSettingService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        List<EntitySettings> entitiesSettings =
            settingsManager.applySettings(groupResolver, topologyGraph);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertEquals(entitiesSettings.size(), 2);
        assertThat(entitiesSettings, containsInAnyOrder(
            createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1)),
            createEntitySettings(entityOid2,
                Arrays.asList(setting2, setting1))));
    }

    @Test
    public void testApplyDefaultSettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1, vertex2).stream());
        when(testSettingService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy3));

        List<EntitySettings> entitiesSettings =
            settingsManager.applySettings(groupResolver, topologyGraph);

        verify(groupResolver, never()).resolve(groupArguments.capture(), eq(topologyGraph));
        assertEquals(entitiesSettings.size(), 2);
        assertThat(entitiesSettings, containsInAnyOrder(
            createEntitySettings(entityOid1, 3/*default SP Id*/),
            createEntitySettings(entityOid2, 3/*default SP Id*/)));
    }

    @Test
    public void testApplyUserSettingsOverridesDefault() throws Exception {

        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1, vertex2).stream());
        when(testSettingService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2, settingPolicy3));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        List<EntitySettings> entitiesSettings =
            settingsManager.applySettings(groupResolver, topologyGraph);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertEquals(entitiesSettings.size(), 2);
        assertThat(entitiesSettings, containsInAnyOrder(
             createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1), 3/*default SP Id*/),
             createEntitySettings(entityOid2,
                Arrays.asList(setting2, setting1), 3/*default SP Id*/)));
    }

    @Test
    public void testApplySettingsWhenSettingPolicyHasNoGroups() throws Exception {

        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1, vertex2).stream());
        when(testSettingService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        List<EntitySettings> entitiesSettings =
            settingsManager.applySettings(groupResolver, topologyGraph);

        // settingPolicy2 doesn't have groups or ids. So it should't be in the
        // final result
        assertThat(hasSetting("setting3", entitiesSettings.get(0)), is(false));
        assertThat(hasSetting("setting4", entitiesSettings.get(1)), is(false));
    }

    @Test
    public void testNoUserOrDefaultSettingPolicies() throws Exception {

        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(new HashSet(Arrays.asList(entityOid1)));
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1).stream());
        when(testSettingService.listSettingPolicies(any()))
           .thenReturn(Collections.emptyList());
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        List<EntitySettings> entitiesSettings =
            settingsManager.applySettings(groupResolver, topologyGraph);

        assertThat(entitiesSettings.size(), is(1));
        assertThat(entitiesSettings.get(0).getUserSettingsCount(), is(0));
        assertThat(entitiesSettings.get(0).hasDefaultSettingPolicyId(), is(false));
    }

    @Test
    public void testApply() {

        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap =
            new HashMap<>();

        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entityOid1)
                .values()
                .stream()
                .collect(Collectors.toList());
        assertThat(appliedSettings, containsInAnyOrder(setting1, setting2));
    }

    @Test
    public void testApplyConflictResolution() {

        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap =
            new HashMap<>();

        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entityOid1)
                .values()
                .stream()
                .collect(Collectors.toList());

        //now check if the conflict resolution is done correctly
        // current conflict resoultion is to pick the 1st one added to the map
        settingsManager.apply(entities,
            Collections.singletonList(settingPolicy2),
            entitySettingsBySettingNameMap);

        assertNotEquals(getSettingNumericValue(
            entitySettingsBySettingNameMap.get(entityOid2)
                .get(setting3.getSettingSpecName())), getSettingNumericValue(setting3));
        assertNotEquals(getSettingNumericValue(
            entitySettingsBySettingNameMap.get(entityOid1)
                .get(setting3.getSettingSpecName())), getSettingNumericValue(setting3));
    }

    @Test
    public void testSendEntitySettings() {

        long topologyId = 111;
        long topologyContexId = 777;
         ArgumentCaptor<UploadEntitySettingsRequest> requestCaptor =
            ArgumentCaptor.forClass(UploadEntitySettingsRequest.class);

        UploadEntitySettingsRequest request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId)
                .build();

        settingsManager.sendEntitySettings(topologyId, topologyContexId, Collections.emptyList());

        verify(testSettingService).uploadEntitySettings(requestCaptor.capture(),
            Matchers.<StreamObserver<UploadEntitySettingsResponse>>any());
    }

    private SettingPolicy createSettingPolicy(long id,
                                              String name,
                                              SettingPolicy.Type type,
                                              List<Setting> settings,
                                              List<Long> groupIds) {
        return  SettingPolicy.newBuilder()
                    .setId(id)
                    .setInfo(SettingPolicyInfo.newBuilder()
                        .setName(name)
                        .addAllSettings(settings)
                        .setEnabled(true)
                        .setEntityType(1)
                        .setScope(Scope.newBuilder()
                            .addAllGroups(groupIds)
                            .build())
                        .build())
                    .setSettingPolicyType(type)
                    .build();
    }

    private SettingPolicy createUserSettingPolicy(long id,
                                                  String name,
                                                  List<Setting> settings) {
        return  SettingPolicy.newBuilder()
                    .setId(id)
                    .setInfo(SettingPolicyInfo.newBuilder()
                        .setName(name)
                        .setEntityType(1)
                        .addAllSettings(settings)
                        .build())
                    .build();
    }

    private EntitySettings createEntitySettings(long oid,
                                                List<Setting> userSettings,
                                                long defaultSettingPolicyId) {
        return EntitySettings.newBuilder()
                    .setEntityOid(oid)
                    .addAllUserSettings(userSettings)
                    .setDefaultSettingPolicyId(defaultSettingPolicyId)
                    .build();
    }

    private EntitySettings createEntitySettings(long oid,
                                                List<Setting> userSettings) {
        return EntitySettings.newBuilder()
                    .setEntityOid(oid)
                    .addAllUserSettings(userSettings)
                    .build();
    }

    private EntitySettings createEntitySettings(long oid,
                                                long defaultSettingPolicyId) {
        return EntitySettings.newBuilder()
                    .setEntityOid(oid)
                    .setDefaultSettingPolicyId(defaultSettingPolicyId)
                    .build();
    }

    private Setting createSettings(String name, int val) {
        return Setting.newBuilder()
                    .setSettingSpecName(name)
                    .setNumericSettingValue(
                        NumericSettingValue.newBuilder()
                        .setValue(val)
                        .build())
                    .build();
    }

    private boolean hasSetting(String settingSpecName, EntitySettings entitySettings) {
        for (Setting setting : entitySettings.getUserSettingsList()) {
            if (setting.getSettingSpecName().equals(settingSpecName)) {
                return true;
            }
        }
        return false;
    }

    private float getSettingNumericValue(Setting setting) {
        return setting.getNumericSettingValue().getValue();
    }

    private Group createGroup(long groupId, String groupName) {
        return Group.newBuilder()
            .setId(groupId)
            .setGroup(GroupInfo.newBuilder()
                .setName(groupName))
            .build();
    }

}
