package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
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

    private SettingServiceBlockingStub settingServiceClient;

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());

    private final SettingPolicyServiceMole testSettingPolicyService =
        spy(new SettingPolicyServiceMole());

    private final SettingServiceMole testSettingService =
        spy(new SettingServiceMole());

    private SettingsManager settingsManager;

    private final Long entityOid1 = 111L;
    private final Long entityOid2 = 222L;
    private final Long entityOid3 = 333L;
    private final Set<Long> entities = new HashSet<Long>(
                Arrays.asList(entityOid1, entityOid2, entityOid3));
    private final Long groupId1 = 111L;
    private final Group group = createGroup(groupId1, "group1");
    private final Long clusterId1 = 222L;

    private final Setting setting1 = createSettings("settingSpec1", 10);
    private final Setting setting2 = createSettings("settingSpec2", 20);
    private final List<Setting> inputSettings1  =
        new LinkedList<Setting>(Arrays.asList(setting1, setting2));
    private final SettingPolicy settingPolicy1 =
        createSettingPolicy(1L, "sp1", SettingPolicy.Type.USER, inputSettings1,
            Collections.singletonList(groupId1));

    private final Setting setting3 = createSettings("settingSpec1", 20);
    private final Setting setting4 = createSettings("settingSpec4", 50);
    private final List<Setting> inputSettings2  =
        new LinkedList<Setting>(Arrays.asList(setting3, setting4));
    private final SettingPolicy settingPolicy2 =
        createUserSettingPolicy(2L, "sp2", inputSettings2);
    private long defaultSettingPolicyId = 3L;
    private final SettingPolicy settingPolicy3 =
        createSettingPolicy(defaultSettingPolicyId, "sp3", SettingPolicy.Type.DEFAULT,
            inputSettings1, Collections.singletonList(groupId1));

    private static final String SPEC_NAME = "settingSpec1";
    private static final SettingSpec SPEC_SMALLER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER);
    private static final SettingSpec SPEC_BIGGER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER);

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
            testSettingPolicyService, testSettingService);

    @Before
    public void setup() throws Exception {
        settingPolicyServiceClient = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingServiceClient = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingsManager = new SettingsManager(settingPolicyServiceClient, groupServiceClient,
                settingServiceClient, topologyFilterFactory);
    }

    @Test
    public void testApplyUserSettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.vertices()).thenReturn(Arrays.asList(vertex1, vertex2).stream());
        when(testSettingPolicyService.listSettingPolicies(any()))
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
        when(testSettingPolicyService.listSettingPolicies(any()))
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
        when(testSettingPolicyService.listSettingPolicies(any()))
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
        when(testSettingPolicyService.listSettingPolicies(any()))
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
        when(testSettingPolicyService.listSettingPolicies(any()))
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

        Map<String, SettingSpec> settingSpecs = new HashMap<>();
        Map<String, SettingSpec> specs = ImmutableMap.of(SPEC_NAME, SPEC_BIGGER_TIEBREAKER);
        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap, settingSpecs);

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

        Map<String, SettingSpec> settingSpecs =
            ImmutableMap.<String, SettingSpec>builder()
                .put("settingSpec1", SPEC_SMALLER_TIEBREAKER)
                .put("settingSpec2", SPEC_SMALLER_TIEBREAKER)
                .put("settingSpec3", SPEC_BIGGER_TIEBREAKER)
                .put("settingSpec4", SPEC_BIGGER_TIEBREAKER)
                .build();

        settingsManager.apply(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap, settingSpecs);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entityOid1)
                .values()
                .stream()
                .collect(Collectors.toList());

        assertThat(appliedSettings, containsInAnyOrder(setting1, setting2));

        // now check if the conflict resolution is done correctly
        settingsManager.apply(entities,
                Collections.singletonList(settingPolicy2),
                entitySettingsBySettingNameMap, settingSpecs);

        appliedSettings =
                entitySettingsBySettingNameMap.get(entityOid1)
                .values()
                .stream()
                .collect(Collectors.toList());

        // setting1 and setting3 both have same SettingSpecNames. Since
        // tieBreaker is smaller, setting1 should win
        assertThat(appliedSettings, hasItem(setting1));
    }


    @RunWith(Parameterized.class)
    public static class SettingsConflictTests {

        private static final String SPEC_NAME = "settingSpec1";
        private static final SettingSpec SPEC_SMALLER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER);
        private static final SettingSpec SPEC_BIGGER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER);
        private static final Setting BOOL_SETTING_BIGGER =
            createSettings(SPEC_NAME, ValueCase.BOOLEAN_SETTING_VALUE, true);
        private static final Setting BOOL_SETTING_SMALLER =
            createSettings(SPEC_NAME, ValueCase.BOOLEAN_SETTING_VALUE, false);

        private static final Setting NUMERIC_SETTING_BIGGER =
            createSettings(SPEC_NAME, ValueCase.NUMERIC_SETTING_VALUE, 20.0F);
        private static final Setting NUMERIC_SETTING_SMALLER =
            createSettings(SPEC_NAME, ValueCase.NUMERIC_SETTING_VALUE, 10.0F);

        private static final Setting STRING_SETTING_BIGGER =
            createSettings(SPEC_NAME, ValueCase.STRING_SETTING_VALUE, "bbb");
        private static final Setting STRING_SETTING_SMALLER =
            createSettings(SPEC_NAME, ValueCase.STRING_SETTING_VALUE, "aaa");

        private static final List<String> ENUM_VALUES = Arrays.asList("aaa", "bbb", "ccc", "ddd");
        private static final SettingSpec SPEC_ENUM_SMALLER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER, ENUM_VALUES);
        private static final SettingSpec SPEC_ENUM_BIGGER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER, ENUM_VALUES);
        private static final Setting ENUM_SETTING_BIGGER =
            createSettings(SPEC_NAME, ValueCase.ENUM_SETTING_VALUE, "ddd");
        static final Setting ENUM_SETTING_SMALLER =
            createSettings(SPEC_NAME, ValueCase.ENUM_SETTING_VALUE, "aaa");


        @Parameters(name="{index}: testResolveConflict(label={0}, specName={1}, " +
                "settingTiebreaker={2}, setting1={3}, setting={4}, expectedSetting={5}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    {"testBooleanSettingSmaller",
                        SPEC_NAME, SPEC_SMALLER_TIEBREAKER, BOOL_SETTING_BIGGER,
                        BOOL_SETTING_SMALLER, BOOL_SETTING_SMALLER},
                    {"testBooleanSettingBigger",
                        SPEC_NAME, SPEC_BIGGER_TIEBREAKER, BOOL_SETTING_BIGGER,
                        BOOL_SETTING_SMALLER, BOOL_SETTING_BIGGER},
                    {"testNumericSettingSmaller",
                        SPEC_NAME, SPEC_SMALLER_TIEBREAKER, NUMERIC_SETTING_BIGGER,
                        NUMERIC_SETTING_SMALLER, NUMERIC_SETTING_SMALLER},
                    {"testNumericSettingBigger",
                        SPEC_NAME, SPEC_BIGGER_TIEBREAKER, NUMERIC_SETTING_BIGGER,
                        NUMERIC_SETTING_SMALLER, NUMERIC_SETTING_BIGGER},
                    {"testStringSettingSmaller",
                        SPEC_NAME, SPEC_SMALLER_TIEBREAKER, STRING_SETTING_BIGGER,
                        STRING_SETTING_SMALLER, STRING_SETTING_SMALLER},
                    {"testStringSettingBigger",
                        SPEC_NAME, SPEC_BIGGER_TIEBREAKER, STRING_SETTING_BIGGER,
                        STRING_SETTING_SMALLER, STRING_SETTING_BIGGER},
                    {"testEnumSettingSmaller",
                        SPEC_NAME, SPEC_ENUM_SMALLER_TIEBREAKER, ENUM_SETTING_BIGGER,
                        ENUM_SETTING_SMALLER, ENUM_SETTING_SMALLER},
                    {"testEnumSettingBigger",
                        SPEC_NAME, SPEC_ENUM_BIGGER_TIEBREAKER, ENUM_SETTING_BIGGER,
                        ENUM_SETTING_SMALLER, ENUM_SETTING_BIGGER}
                });
        }

        @Parameter(0)
        public String testLabel;
        @Parameter(1)
        public String specName;
        @Parameter(2)
        public SettingSpec settingSpecTiebreaker;
        @Parameter(3)
        public Setting conflictSetting1;
        @Parameter(4)
        public Setting conflictSetting2;
        @Parameter(5)
        public Setting expectedSetting;

        @Test
        public void testResolveConflict() {
            Map<String, SettingSpec> specs = ImmutableMap.of(specName, settingSpecTiebreaker);
            Setting resolvedSetting =
                SettingsManager.resolveConflict(conflictSetting1, conflictSetting2, specs);
            assertThat(testLabel, resolvedSetting, is(expectedSetting));
        }

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

        settingsManager.sendEntitySettings(topologyId, topologyContexId,
            Arrays.asList(createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1))));

        verify(testSettingPolicyService, times(1)).uploadEntitySettings(requestCaptor.capture(),
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

    private static Setting createSettings(String name, int val) {
        return Setting.newBuilder()
                    .setSettingSpecName(name)
                    .setNumericSettingValue(
                        NumericSettingValue.newBuilder()
                        .setValue(val)
                        .build())
                    .build();
    }

    private static Setting createSettings(String name, ValueCase valueCase, Object val) {
        Setting.Builder setting = Setting.newBuilder()
                    .setSettingSpecName(name);

        switch (valueCase) {
            case BOOLEAN_SETTING_VALUE:
                return setting.setBooleanSettingValue(
                            BooleanSettingValue.newBuilder()
                                .setValue((boolean)val))
                                .build();
            case NUMERIC_SETTING_VALUE:
                return setting.setNumericSettingValue(
                            NumericSettingValue.newBuilder()
                                .setValue((float)val))
                                .build();
            case STRING_SETTING_VALUE:
                return setting.setStringSettingValue(
                            StringSettingValue.newBuilder()
                                .setValue((String)val))
                                .build();
            case ENUM_SETTING_VALUE:
                return setting.setEnumSettingValue(
                            EnumSettingValue.newBuilder()
                                .setValue((String)val))
                                .build();
            default:
                return setting.build();
        }
    }

    private static boolean hasSetting(String settingSpecName, EntitySettings entitySettings) {
        for (Setting setting : entitySettings.getUserSettingsList()) {
            if (setting.getSettingSpecName().equals(settingSpecName)) {
                return true;
            }
        }
        return false;
    }

    private static float getSettingNumericValue(Setting setting) {
        return setting.getNumericSettingValue().getValue();
    }

    private static Group createGroup(long groupId, String groupName) {
        return Group.newBuilder()
            .setId(groupId)
            .setGroup(GroupInfo.newBuilder()
                .setName(groupName))
            .build();
    }

    private static SettingSpec createSettingSpec(String specName, SettingTiebreaker tieBreaker) {
        return SettingSpec.newBuilder()
                    .setName(specName)
                    .setEntitySettingSpec(
                        EntitySettingSpec.newBuilder()
                            .setTiebreaker(tieBreaker)
                            .build())
                    .build();
    }

    private static SettingSpec createSettingSpec(String specName,
                                          SettingTiebreaker tieBreaker,
                                          List<String> enumValues) {
        return SettingSpec.newBuilder()
                    .setName(specName)
                    .setEntitySettingSpec(
                        EntitySettingSpec.newBuilder()
                            .setTiebreaker(tieBreaker)
                            .build())
                    .setEnumSettingValueType(
                        EnumSettingValueType.newBuilder()
                            .addAllEnumValues(enumValues)
                            .build())
                    .build();
    }
}
