package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;

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
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class EntitySettingsResolverTest {

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    private TopologyGraph topologyGraph = mock(TopologyGraph.class);

    private GroupServiceBlockingStub groupServiceClient;

    private SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private SettingServiceBlockingStub settingServiceClient;

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());

    private final SettingPolicyServiceMole testSettingPolicyService =
        spy(new SettingPolicyServiceMole());

    private final SettingServiceMole testSettingService =
        spy(new SettingServiceMole());

    private final SettingOverrides settingOverrides = mock(SettingOverrides.class);

    private EntitySettingsResolver entitySettingsResolver;

    private final Long entityOid1 = 111L;
    private final Long entityOid2 = 222L;
    private final Long entityOid3 = 333L;
    private final Set<Long> entities = ImmutableSet.of(entityOid1, entityOid2, entityOid3);
    private final Long groupId1 = 111L;
    private final Group group = createGroup(groupId1, "group1");
    private final Long clusterId1 = 222L;

    private final Setting setting1 = createSettings("settingSpec1", 10);
    private final Setting setting2 = createSettings("settingSpec2", 20);
    private final List<Setting> inputSettings1  = Arrays.asList(setting1, setting2);
    private final SettingPolicy settingPolicy1 =
        createSettingPolicy(1L, "sp1", SettingPolicy.Type.USER, inputSettings1,
            Collections.singletonList(groupId1));

    private final Setting setting3 = createSettings("settingSpec1", 20);
    private final Setting setting4 = createSettings("settingSpec4", 50);
    private final List<Setting> inputSettings2  = Arrays.asList(setting3, setting4);
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

    private final TopologyEntity topologyEntity1 = topologyEntity(entity1);
    private final TopologyEntity topologyEntity2 = topologyEntity(entity2);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
            testSettingPolicyService, testSettingService);

    @Before
    public void setup() throws Exception {
        settingPolicyServiceClient = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingServiceClient = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        entitySettingsResolver = new EntitySettingsResolver(settingPolicyServiceClient, groupServiceClient,
                settingServiceClient);
    }

    @Test
    public void testApplyUserSettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph, settingOverrides);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
            createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1)),
            createEntitySettings(entityOid2,
                Arrays.asList(setting2, setting1))));
    }

    @Test
    public void testApplyDefaultSettings() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(settingPolicy3));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph, settingOverrides);

        verify(groupResolver, never()).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
            createEntitySettings(entityOid1, 3/*default SP Id*/),
            createEntitySettings(entityOid2, 3/*default SP Id*/)));
    }

    @Test
    public void testApplyUserSettingsOverridesDefault() throws Exception {

        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2, settingPolicy3));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph, settingOverrides);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
             createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1), 3/*default SP Id*/),
             createEntitySettings(entityOid2,
                Arrays.asList(setting2, setting1), 3/*default SP Id*/)));
    }

    @Test
    public void testApplySettingsWhenSettingPolicyHasNoGroups() throws Exception {

        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph, settingOverrides);

        // settingPolicy2 doesn't have groups or ids. So it should't be in the
        // final result
        List<EntitySettings> settings = new ArrayList<>(entitiesSettings.getEntitySettings());
        assertThat(hasSetting("setting3", settings.get(0)), is(false));
        assertThat(hasSetting("setting4", settings.get(1)), is(false));
    }

    @Test
    public void testNoUserOrDefaultSettingPolicies() throws Exception {
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(ImmutableSet.of(entityOid1));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.emptyList());
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph, settingOverrides);

        assertThat(entitiesSettings.getEntitySettings().size(), is(1));
        List<EntitySettings> settings = new ArrayList<>(entitiesSettings.getEntitySettings());
        assertThat(settings.get(0).getUserSettingsCount(), is(0));
        assertThat(settings.get(0).hasDefaultSettingPolicyId(), is(false));
    }

    @Test
    public void testApply() {

        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap =
            new HashMap<>();

        Map<String, SettingSpec> settingSpecs = new HashMap<>();
        entitySettingsResolver.resolve(entities, Collections.singletonList(settingPolicy1),
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
                ImmutableMap.<String, SettingSpec>builder().put("settingSpec1",
                        SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER)
                                .setName("settingSpec1")
                                .build())
                        .put("settingSpec2", SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER)
                                .setName("settingSpec2")
                                .build())
                        .put("settingSpec3", SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER)
                                .setName("settingSpec3")
                                .build())
                        .put("settingSpec4", SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER)
                                .setName("settingSpec4")
                                .build())
                        .build();

        entitySettingsResolver.resolve(entities, Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap, settingSpecs);

        List<Setting> appliedSettings =
                entitySettingsBySettingNameMap.get(entityOid1)
                .values()
                .stream()
                .collect(Collectors.toList());

        assertThat(appliedSettings, containsInAnyOrder(setting1, setting2));

        // now check if the conflict resolution is done correctly
        entitySettingsResolver.resolve(entities,
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
                EntitySettingsResolver.resolveConflict(conflictSetting1, conflictSetting2, specs);
            assertThat(testLabel, resolvedSetting, is(expectedSetting));
        }

    }

    @Test
    public void testSendEntitySettings() {

        final TopologyInfo info = TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(111)
                .setTopologyType(TopologyType.REALTIME)
                .build();

        UploadEntitySettingsRequest request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(info.getTopologyId())
                .setTopologyContextId(info.getTopologyContextId())
                .build();

        entitySettingsResolver.sendEntitySettings(info, Collections.singletonList(
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1))));

        verify(testSettingPolicyService).uploadEntitySettings(any());
    }

    @Test
    public void testNoSendPlanEntitySettings() {
        final TopologyInfo info = TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(111)
                .setTopologyType(TopologyType.PLAN)
                .build();

        entitySettingsResolver.sendEntitySettings(info, Collections.singletonList(
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1))));

        verify(testSettingPolicyService, never()).updateSettingPolicy(any());
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
        return entitySettings.getUserSettingsList()
                .stream()
                .anyMatch(setting -> setting.hasSettingSpecName() &&
                        setting.getSettingSpecName().equals(settingSpecName));
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
