package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoMoles.ScheduleServiceMole;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingAndPolicyIdRecord;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingResolver;

/**
 * Unit tests for {@link EntitySettingsResolver}.
 *
 */
public class EntitySettingsResolverTest {

    private static final ApiEntityType TEST_ENTITY_TYPE = ApiEntityType.VIRTUAL_MACHINE; // arbitrary number

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    @SuppressWarnings("unchecked")
    private TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

    private TopologyInfo rtTopologyInfo = TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();

    private GroupServiceBlockingStub groupServiceClient;

    private SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private SettingPolicyServiceStub settingPolicyServiceClientAsync;

    private SettingServiceBlockingStub settingServiceClient;

    private ScheduleServiceBlockingStub scheduleServiceClient;

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());

    private final SettingPolicyServiceMole testSettingPolicyService =
        spy(new SettingPolicyServiceMole());

    private final SettingServiceMole testSettingService =
        spy(new SettingServiceMole());

    private final ScheduleServiceMole testScheduleService =
        spy(new ScheduleServiceMole());

    private final SettingOverrides settingOverrides = mock(SettingOverrides.class);

    private EntitySettingsResolver entitySettingsResolver;

    private static final Long entityOid1 = 111L;
    private static final Long entityOid2 = 222L;
    private static final Long entityOid3 = 333L;
    private static final Set<Long> entities = ImmutableSet.of(entityOid1, entityOid2, entityOid3);

    private static final Long groupId = 5001L;

    private static final String groupName = "groupName";
    private static final Grouping group = Grouping.newBuilder()
            .setId(groupId)
            .setDefinition(GroupDefinition.newBuilder().setDisplayName(groupName))
            .build();

    private static final String SPEC_1 = "settingSpec1";
    private static final String SPEC_2 = "settingSpec2";
    private static final String SPEC_3 = "settingSpec3";
    private static final String SPEC_4 = "settingSpec4";
    private static final String SPEC_VCPU_UP = "resizeVcpuUpInBetweenThresholds";
    private static final String SPEC_VCPU_UP_EXEC_SCHEDULE = "resizeVcpuUpInBetweenThresholdsExecutionSchedule";
    private static final Setting setting1 = createNumericSetting(SPEC_1, 10f);
    // Used to verify tie-breaker. It has the same spec as setting1 but different value.
    private static final Setting setting1a = createNumericSetting(SPEC_1, 15f);
    private static final Setting setting2 = createNumericSetting(SPEC_2, 20f);
    private static final Setting setting2a = createNumericSetting(SPEC_2, 18f);
    private static final Setting setting3 = createNumericSetting(SPEC_3, 30f);
    private static final Setting setting4 = createNumericSetting(SPEC_4, 50f);
    private static final Setting RECOMMEND_SETTING = createEnumSetting(SPEC_VCPU_UP,
        ActionMode.RECOMMEND.name());
    private static final Setting EXTERNAL_APPROVAL_SETTING = createEnumSetting(SPEC_VCPU_UP,
        ActionDTO.ActionMode.EXTERNAL_APPROVAL.name());
    private static final Setting manualActionModeSetting = createEnumSetting(SPEC_VCPU_UP,
            ActionMode.MANUAL.name());
    private static final Setting automaticActionModeSetting = createEnumSetting(SPEC_VCPU_UP,
            ActionMode.AUTOMATIC.name());
    private static final Setting executionScheduleSetting1 =
            createSortedSetOfOidSetting(SPEC_VCPU_UP_EXEC_SCHEDULE, Collections.singletonList(1L));
    private static final Setting executionScheduleSetting2 =
            createSortedSetOfOidSetting(SPEC_VCPU_UP_EXEC_SCHEDULE, Arrays.asList(2L, 3L));

    private static final long SP1_ID = 6001;
    private static final long SP1A_ID = 6002;
    private static final long SP2_ID = 6003;
    private static final long SP3_ID = 6004;
    private static final long SP4_ID = 6005;

    private static final long CSG_ENABLED_SP_ID = 7001L;
    private static final long CSG_DISABLED_SP_ID = 7002L;

    private static final long DEFAULT_POLICY_ID = 6101L;
    private static final List<Setting> inputSettings1  = Arrays.asList(setting1, setting2);
    private static final List<Setting> inputSettings1a  = Arrays.asList(setting1a, setting2a);
    private static final List<Setting> inputSettings2  = Arrays.asList(setting3, setting4);
    private static final SettingPolicy settingPolicy1 =
                    createSettingPolicy(SP1_ID, "sp1", SettingPolicy.Type.USER, inputSettings1,
                        Collections.singletonList(groupId));
    private static final List<TopologyProcessorSetting<?>> settingsInPolicy1 = Arrays.asList(
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting1)),
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting2)));
    private static final SettingPolicy settingPolicy1a =
                    createSettingPolicy(SP1A_ID, "sp1a", SettingPolicy.Type.USER, inputSettings1a,
                        Collections.singletonList(groupId));
    private static final List<TopologyProcessorSetting<?>> settingsInPolicy1a = Arrays.asList(
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting1a)),
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting2a)));
    // this policy has no scope (group)
    private static final SettingPolicy settingPolicy2 =
        createUserSettingPolicy(SP2_ID, "sp2", inputSettings2);
    private static final List<TopologyProcessorSetting<?>> settingsInPolicy2 = Arrays.asList(
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting3)),
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting4)));
    private static final SettingPolicy settingPolicy3 =
                    createSettingPolicy(SP3_ID, "sp3", SettingPolicy.Type.USER, inputSettings1,
                                    Collections.singletonList(groupId));
    private static final List<TopologyProcessorSetting<?>> settingsInPolicy3 = Arrays.asList(
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting1)),
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting2)));
    private static final SettingPolicy settingPolicy4 =
                    createSettingPolicy(SP4_ID, "sp4", SettingPolicy.Type.DISCOVERED, inputSettings1a,
                                    Collections.singletonList(groupId));
    private static final List<TopologyProcessorSetting<?>> settingsInPolicy4 = Arrays.asList(
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting1a)),
            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                    Collections.singletonList(setting2a)));
    private static final SettingPolicy defaultSettingPolicy =
        createSettingPolicy(DEFAULT_POLICY_ID, "sp_def", SettingPolicy.Type.DEFAULT,
                            inputSettings1, Collections.singletonList(groupId));

    private static final String SPEC_NAME = "settingSpecName";
    private static final SettingSpec SPEC_SMALLER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER);
    private static final SettingSpec SPEC_BIGGER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER);

    private static final SettingSpec ACTION_MODE_SETTING_SPEC =
            EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingSpec();

    private static final SettingSpec EXECUTION_SCHEDULE_SETTING_SPEC =
            createSettingSpec(SPEC_VCPU_UP_EXEC_SCHEDULE, SettingTiebreaker.UNION);

    private static final TopologyEntityDTO.Builder entity1 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid1)
            .setEntityType(TEST_ENTITY_TYPE.typeNumber());

    private static final TopologyEntityDTO.Builder entity2 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid2)
            .setEntityType(TEST_ENTITY_TYPE.typeNumber());

    private static final TopologyEntity topologyEntity1 = topologyEntity(entity1);
    private static final TopologyEntity topologyEntity2 = topologyEntity(entity2);

    private static final Map<String, SettingSpec> SPECS = ImmutableMap.<String, SettingSpec>builder()
            .put(SPEC_1, SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER).setName(SPEC_1).build())
            .put(SPEC_2, SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER).setName(SPEC_2).build())
            .put(SPEC_3, SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER).setName(SPEC_3).build())
            .put(SPEC_4, SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER).setName(SPEC_4).build())
            .build();
    private static final Long APPLIES_NOW_SCHEDULE_ID = 11L;

    //private static final ScheduleResolver scheduleResolver = mock(ScheduleResolver.class);
    private static final Schedule APPLIES_NOW =
        createOneTimeSchedule(APPLIES_NOW_SCHEDULE_ID, 4815162342L, 4815169500L)
        .toBuilder()
        .setActive(Active.getDefaultInstance())
        .build();
    private static final Schedule NOT_NOW = createOneTimeSchedule(22L, 11235813L,  11242980L);
    private static final int CHUNK_SIZE = 1;
    private static final ConsistentScalingManager consistentScalingManager =
            mock(ConsistentScalingManager.class);
    private static final String consistentScalingSpecName =
                    EntitySettingSpecs.EnableConsistentResizing.getSettingName();
    /*
    static {
        when(scheduleResolver.appliesAtResolutionInstant(eq(APPLIES_NOW.getId()),
            any(ScheduleServiceBlockingStub.class))).thenReturn(true);
        when(scheduleResolver.appliesAtResolutionInstant(eq(NOT_NOW.getId()),
            any(ScheduleServiceBlockingStub.class))).thenReturn(false);
    }*/
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
            testSettingPolicyService, testSettingService, testScheduleService);

    @Before
    public void setup() {
        settingPolicyServiceClient = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingServiceClient = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        scheduleServiceClient = ScheduleServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingPolicyServiceClientAsync = SettingPolicyServiceGrpc.newStub(grpcServer.getChannel());
        entitySettingsResolver = new EntitySettingsResolver(settingPolicyServiceClient,
            groupServiceClient, settingServiceClient, settingPolicyServiceClientAsync,
            scheduleServiceClient, CHUNK_SIZE);
    }


    private ResolvedGroup resolvedGroup(Grouping group, Set<Long> memberIds) {
        return new ResolvedGroup(group, Collections.singletonMap(TEST_ENTITY_TYPE, memberIds));
    }


    /**
     * Verify that policy1 wins when using SMALLER tie-breaker on both settings in the policy.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testApplyUserSettings() throws Exception {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, entities));
        // returns only entities 1 and 2 even though group contains 3 entities
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings = entitySettingsResolver.resolveSettings(groupResolver,
                topologyGraph, settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        // Both entities expected to resolve to policy1 for both settings
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1),
                        Collections.singletonList(SP1_ID), Collections.singletonList(SP1_ID)),
                createEntitySettings(entityOid2, Arrays.asList(setting2, setting1),
                        Collections.singletonList(SP1_ID), Collections.singletonList(SP1_ID)));
    }

    /**
     * Verify that when there are no user or discovered policies, default policies are used.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testApplyDefaultSettings() throws Exception {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        // Only default setting policy used
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(defaultSettingPolicy));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(
                groupResolver, topologyGraph, settingOverrides, rtTopologyInfo,
                consistentScalingManager, null);

        verify(groupResolver, never()).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
            createDefaultEntitySettings(entityOid1, DEFAULT_POLICY_ID),
            createDefaultEntitySettings(entityOid2, DEFAULT_POLICY_ID)));
    }

    /**
     * Verify that when there are user policies as well as default policies - user policies win.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testApplyUserSettingsOverridesDefault() throws Exception {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2, defaultSettingPolicy));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1),
                                     Collections.singletonList(SP1_ID), DEFAULT_POLICY_ID),
                createEntitySettings(entityOid2, Arrays.asList(setting2, setting1),
                                     Collections.singletonList(SP1_ID), DEFAULT_POLICY_ID));
    }

    /**
     * Verify that when a policy is not associated with a group, it is not applied.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testApplySettingsWhenSettingPolicyHasNoGroups() throws Exception {
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        // settingPolicy2 doesn't have groups or ids. So it should't be in the final result
        assertTrue(entitiesSettings.getEntitySettings().stream()
            .allMatch(setting -> setting.getUserSettingsList().isEmpty()));
    }

    /**
     * Test the case when there are no policies at all.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNoUserOrDefaultSettingPolicies() throws Exception {
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, ImmutableSet.of(entityOid1)));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.emptyList());
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        assertThat(entitiesSettings.getEntitySettings().size(), is(1));
        List<EntitySettings> settings = new ArrayList<>(entitiesSettings.getEntitySettings());
        assertThat(settings.get(0).getUserSettingsCount(), is(0));
        assertThat(settings.get(0).hasDefaultSettingPolicyId(), is(false));
    }

    /**
     * Test ExcludedTemplates settings. Note that ExcludedTemplates setting doesn't support schedule.
     * In this test, there are:
     * Three entities: {entityOid1, entityOid2, entityOid3},
     * Four tiers: {tier1, tier2, tier3, tier4},
     * Three consumer groups:
     *     group1 = {entityOid1, entityOid2}, group2 = {entityOid2, entityOid3} and group3 = {entityOid3},
     * Two ExcludedTemplates settings:
     *     setting1 with id SP1_ID excludes {tier1, tier2} for group1,
     *     setting2 with id SP2_ID excludes {tier2, tier3} for group2 and
     *     setting3 with id SP3_ID excludes {tier3, tier4} for group3.
     *
     * After resolving settings, we should have:
     *     entityOid1 excludes {tier1, tier2} with setting id {SP1_ID},
     *     entityOid2 excludes {tier1, tier2, tier3} with setting id {SP1_ID, SP2_ID},
     *     entityOid3 excludes {tier2, tier3, tier4} with setting id {SP2_ID, SP3_ID}.
     * Note that tiers should be in natural order.
     */
    @Test
    public void testExcludedTemplatesSettings() {
        final String specName = EntitySettingSpecs.ExcludedTemplates.getSettingName();
        final long tier1 = 1L;
        final long tier2 = 2L;
        final long tier3 = 3L;
        final long tier4 = 4L;

        final List<Long> excludedTiers1 = Arrays.asList(tier1, tier2);
        final Setting setting1 = createSortedSetOfOidSetting(specName, excludedTiers1);
        final SettingPolicy settingPolicy1 = createSettingPolicy(SP1_ID, "sp1",
            SettingPolicy.Type.DISCOVERED, Collections.singletonList(setting1),
            Collections.singletonList(10L));
        final List<TopologyProcessorSetting<?>> topologyProcessorSettings1 =
                Collections.singletonList(
                        TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                Collections.singletonList(setting1)));

        List<Long> excludedTiers2 = Arrays.asList(tier2, tier3);
        final Setting setting2 = createSortedSetOfOidSetting(specName, excludedTiers2);
        final SettingPolicy settingPolicy2 = createSettingPolicy(SP2_ID, "sp2",
            SettingPolicy.Type.USER, Collections.singletonList(setting2),
            Collections.singletonList(11L));
        final List<TopologyProcessorSetting<?>> topologyProcessorSettings2 =
                Collections.singletonList(
                        TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                Collections.singletonList(setting2)));

        List<Long> excludedTiers3 = Arrays.asList(tier3, tier4);
        final Setting setting3 = createSortedSetOfOidSetting(specName, excludedTiers3);
        final SettingPolicy settingPolicy3 = createSettingPolicy(SP3_ID, "sp3",
            SettingPolicy.Type.DISCOVERED, Collections.singletonList(setting3),
            Collections.singletonList(12L));
        final List<TopologyProcessorSetting<?>> topologyProcessorSettings3 =
                Collections.singletonList(
                        TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                Collections.singletonList(setting3)));

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();
        final Map<String, SettingSpec> settingNameToSettingSpecs =
            Collections.singletonMap(specName, EntitySettingSpecs.ExcludedTemplates.getSettingSpec());
        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        entitySettingsResolver.resolveAllEntitySettings(settingPolicy2, topologyProcessorSettings2,
            Collections.singletonMap(11L, resolvedGroup(group, Sets.newHashSet(entityOid2, entityOid3))),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap(),
            entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy1, topologyProcessorSettings1,
            Collections.singletonMap(10L, resolvedGroup(group, Sets.newHashSet(entityOid1, entityOid2))),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap(),
            entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy3, topologyProcessorSettings3,
            Collections.singletonMap(12L, resolvedGroup(group, Sets.newHashSet(entityOid3))),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap(),
            entityToPolicySettings);

        SettingAndPolicyIdRecord record1 = entitySettingsBySettingNameMap.get(entityOid1).get(specName);
        assertEquals(SettingPolicy.Type.DISCOVERED, record1.getType());
        assertEquals(Collections.singleton(SP1_ID), record1.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier1, tier2)),
            TopologyProcessorSettingsConverter.toProtoSettings(record1.getSetting()).iterator().next());

        SettingAndPolicyIdRecord record2 = entitySettingsBySettingNameMap.get(entityOid2).get(specName);
        assertEquals(Type.DISCOVERED, record2.getType());
        assertEquals(ImmutableSet.of(SP2_ID, SP1_ID), record2.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier1, tier2, tier3)),
                TopologyProcessorSettingsConverter.toProtoSettings(record2.getSetting()).iterator().next());

        SettingAndPolicyIdRecord record3 = entitySettingsBySettingNameMap.get(entityOid3).get(specName);
        assertEquals(Type.DISCOVERED, record3.getType());
        assertEquals(ImmutableSet.of(SP2_ID, SP3_ID), record3.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier2, tier3, tier4)),
                TopologyProcessorSettingsConverter.toProtoSettings(record3.getSetting()).iterator().next());
    }

    /**
     * Test consistent scaling setting application.
     *
     * <p/>Group 1 contains VMs 101, 102, and 103
     * Policy "enable-csg" enables consistent resizing on Group 1
     * Group 2 contains VM 102
     * Policy "disable-csg" disables consistent resizing on Group 2
     *
     * <p/>Since disabling consistent scaling has higher priority than enabling it, only VMs 101 and 103
     * should have consistent scaling enabled.
     */
    @Test
    public void testConsistentScalingSettings() {
        final Map<String, SettingSpec> csgSettingNameToSettingSpecs =
                        Collections.singletonMap(consistentScalingSpecName,
                                     EntitySettingSpecs.EnableConsistentResizing.getSettingSpec());
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap = new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        Pair<SettingPolicy, List<TopologyProcessorSetting<?>>> enabledSettingPolicy =
                        createConsistentScalingPolicy(CSG_ENABLED_SP_ID, "enable-csg", true, 1L);
        entitySettingsResolver.resolveAllEntitySettings(
                enabledSettingPolicy.getFirst(), enabledSettingPolicy.getSecond(),
                Collections.singletonMap(1L, resolvedGroup(group, Sets.newHashSet(101L, 102L, 103L))),
                entitySettingsBySettingNameMap, csgSettingNameToSettingSpecs,
                Collections.emptyMap(), entityToPolicySettings);

        Pair<SettingPolicy, List<TopologyProcessorSetting<?>>> disabledSettingPolicy =
                        createConsistentScalingPolicy(CSG_DISABLED_SP_ID, "disable-csg", false, 2L);
        entitySettingsResolver.resolveAllEntitySettings(
                        disabledSettingPolicy.getFirst(), disabledSettingPolicy.getSecond(),
                        Collections.singletonMap(2L, resolvedGroup(group, Sets.newHashSet(102L))),
                        entitySettingsBySettingNameMap, csgSettingNameToSettingSpecs,
                        Collections.emptyMap(), entityToPolicySettings);

        // Expected results:
        // Entity 101: policy ID = 7001L, consistent scaling = true
        // Entity 102: policy ID = 7002L, consistent scaling = false
        // Entity 103: policy ID = 7001L, consistent scaling = true
        long[] expectedResults = { 7001L, 7002L, 7001L };

        for (int i = 0; i < 3; i++) {
            Long oid = 101L + i;
            SettingAndPolicyIdRecord record =
                            entitySettingsBySettingNameMap.get(oid).get(consistentScalingSpecName);
            assertEquals(SettingPolicy.Type.DISCOVERED, record.getType());
            assertEquals(Collections.singleton(expectedResults[i]), record.getSettingPolicyIdList());
            assertEquals(createBooleanSetting(consistentScalingSpecName, expectedResults[i] == 7001L),
                         TopologyProcessorSettingsConverter
                                         .toProtoSettings(record.getSetting()).iterator().next());
        }
    }

    /**
     * Helper function: Create a setting policy and topology processor setting for consistent
     * scaling.
     * @param policyId ID of policy to create
     * @param policyName name of policy
     * @param csgSetting whether to enable consistent resizing
     * @param groupId group ID to apply the policy to
     * @return A pair: first = the created SettingPolicy, second = list of TopologyProcessorSettings
     */
    private Pair<SettingPolicy, List<TopologyProcessorSetting<?>>> createConsistentScalingPolicy(
                    long policyId, String policyName, boolean csgSetting, long groupId) {
        final Setting setting = createBooleanSetting(consistentScalingSpecName, csgSetting);

        final SettingPolicy settingPolicy = createSettingPolicy(policyId, policyName,
                    SettingPolicy.Type.DISCOVERED, Collections.singletonList(setting),
                    Collections.singletonList(groupId));

        final List<TopologyProcessorSetting<?>> tpSettings = Collections.singletonList(
                        TopologyProcessorSettingsConverter
                                .toTopologyProcessorSetting(Collections.singletonList(setting)));
        return new Pair<>(settingPolicy, tpSettings);
    }

    /**
     * Policy 1 (Policy without execution windows) - MANUAL.
     *                                                          -> MANUAL
     * Policy 2 (Policy without execution windows) - AUTOMATIC
     *
     * <p>Expected results: action mode - MANUAL(as less aggressive value)<p/>
     * @throws Exception if something goes wrong
     */
    @Test
    public void testConflictResolutionForPoliciesWithoutExecutionWindows() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
                Collections.singletonList(manualActionModeSetting),
                Collections.singletonList(groupId));

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
                Collections.singletonList(automaticActionModeSetting),
                Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
                resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
                Arrays.asList(policy1, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
                Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));

        final GraphWithSettings entitiesSettings =
                entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                        settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
                createEntitySettings(entityOid1, Collections.singletonList(manualActionModeSetting),
                        Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)),
                createEntitySettings(entityOid2, Collections.singletonList(manualActionModeSetting),
                        Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)));
    }

    /**
     * Policy 1 (Policy with execution window) - MANUAL  (exWindow1)
     *                                                                              -> MANUAL (exWindow1, exWindow2, exWindow3)
     * Policy 2 (Policy with execution window) - MANUAL  (exWindow2, exWindow3)
     *
     * <p>Expected results: action mode - MANUAL; execution windows - merge all because they related
     * to the same value of actionMode setting.</p>
     * @throws Exception if something goes wrong
     */
    @Test
    public void testConflictResolutionForPoliciesWithTheSameActionMode() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
                Arrays.asList(manualActionModeSetting, executionScheduleSetting1),
                Collections.singletonList(groupId));

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
                Arrays.asList(manualActionModeSetting, executionScheduleSetting2),
                Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
                resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
                Arrays.asList(policy1, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
                Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));

        final GraphWithSettings entitiesSettings =
                entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                        settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        final Setting mergedExecutionScheduleSetting =
                createSortedSetOfOidSetting(SPEC_VCPU_UP_EXEC_SCHEDULE, Arrays.asList(1L, 2L, 3L));
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
                createEntitySettings(entityOid1,
                        Arrays.asList(manualActionModeSetting, mergedExecutionScheduleSetting),
                        Arrays.asList(SP1_ID, SP2_ID), Arrays.asList(SP1_ID, SP2_ID)), createEntitySettings(entityOid2,
                        Arrays.asList(manualActionModeSetting, mergedExecutionScheduleSetting),
                        Arrays.asList(SP1_ID, SP2_ID), Arrays.asList(SP1_ID, SP2_ID)));
    }

    /**
     * Policy 1 (Policy with execution window) - MANUAL + (exWindow1)
     *                                                                                  -> MANUAL (exWindow1)
     * Policy 2 (Policy with execution window) - AUTOMATIC + (exWindow2, exWindow3)
     *
     * <p>Expected results: action mode - MANUAL (as less aggressive); execution windows -
     * exWindow1 (we use from policy contains winner actionMode setting)</p>
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testConflictResolutionForPoliciesWithDifferentActionModesAndExecutionWindows() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
                Arrays.asList(manualActionModeSetting, executionScheduleSetting1),
                Collections.singletonList(groupId));

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
                Arrays.asList(automaticActionModeSetting, executionScheduleSetting2),
                Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
                resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
                Arrays.asList(policy1, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
                Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));

        final GraphWithSettings entitiesSettings =
                entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                        settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
                createEntitySettings(entityOid1,
                        Arrays.asList(manualActionModeSetting, executionScheduleSetting1),
                        Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)), createEntitySettings(entityOid2,
                        Arrays.asList(manualActionModeSetting, executionScheduleSetting1),
                        Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)));
    }

    /**
     * Policy 1 (Active schedule policy) - AUTOMATIC
     *                                                                              -> AUTOMATIC
     * Policy 2 (Policy with execution window) - MANUAL  (exWindow2, exWindow3)
     *
     * <p>Expected results: action mode = AUTOMATIC because schedule policy has
     * priority and execution windows should be ignored and doesn't applied to entities.</p>
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testIgnoreExecutionWindowsIfSchedulePolicyIsActive() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
                Collections.singletonList(automaticActionModeSetting),
                Collections.singletonList(groupId));
        final SettingPolicy policy1Now = addSchedule(policy1, APPLIES_NOW);

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
                Arrays.asList(manualActionModeSetting, executionScheduleSetting2),
                Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
                resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
                Arrays.asList(policy1Now, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
                Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));
        when(testScheduleService.getSchedules(Mockito.any())).thenReturn(
                Collections.singletonList(APPLIES_NOW));

        final GraphWithSettings entitiesSettings =
                entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                        settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);

        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(), createEntitySettings(entityOid1,
            Collections.singletonList(automaticActionModeSetting),
            Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)), createEntitySettings(entityOid2,
            Collections.singletonList(automaticActionModeSetting),
            Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)));

    }


    /**
     * Test the {@link EntitySettingsResolver#resolveAllEntitySettings} method.
     */
    @Test
    public void testApply() {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();
        Map<String, SettingSpec> settingSpecs = new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        entitySettingsResolver.resolveAllEntitySettings(settingPolicy1, settingsInPolicy1,
                Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
                entitySettingsBySettingNameMap, settingSpecs, Collections.emptyMap(), entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());
        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));
    }

    /**
     * Test that a policy with a schedule that doesn't apply now is not used.
     */
    @Test
    public void testScheduledSettingForDifferentTimeIsNotApplied() {
        final SettingPolicy settingPolicyNotNow = addSchedule(settingPolicy1, NOT_NOW);

        final Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();
        final Map<String, SettingSpec> settingSpecs = new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNotNow, settingsInPolicy1,
                Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
                entitySettingsBySettingNameMap, settingSpecs, Collections.emptyMap(),
                entityToPolicySettings);

        assertTrue(settingSpecs.isEmpty());
        entitySettingsBySettingNameMap.values().forEach(map -> assertTrue(map.isEmpty()));
    }

    /**
     * Verify that we resolve two scheduled policies.
     */
    @Test
    public void testTwoScheduledSettingsConflictResolved() {
        final SettingPolicy settingPolicyNow1 = addSchedule(settingPolicy1, APPLIES_NOW);
        final SettingPolicy settingPolicyNow2 = addSchedule(settingPolicy2, APPLIES_NOW);

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNow1, settingsInPolicy1,
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
            entitySettingsBySettingNameMap, SPECS, getSchedules(), entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));
        // Verify that policy1 wins
        assertEquals(appliedSettings.get(0).getSettingPolicyIdList(),
            Collections.singleton(settingPolicyNow1.getId()));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNow2, settingsInPolicy2,
                Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
                entitySettingsBySettingNameMap, SPECS, getSchedules(), entityToPolicySettings);

        appliedSettings = new ArrayList<>(entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), hasItem(setting1));
        entitySettingsBySettingNameMap.forEach((id, specNameToRecord) -> specNameToRecord.forEach(
            (spec, record) -> assertTrue(record.isScheduled())));
    }

    /**
     * Verify that if one policy is in schedule and the other is out of schedule then we
     * pick the scheduled one.
     */
    @Test
    public void testTwoScheduledOneInapplicable() {

        final SettingPolicy settingPolicyNotNow = addSchedule(settingPolicy1, NOT_NOW);
        final SettingPolicy settingPolicyNow = addSchedule(settingPolicy3, APPLIES_NOW);

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNotNow, settingsInPolicy1,
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
            entitySettingsBySettingNameMap, SPECS, getSchedules(), entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNow, settingsInPolicy3,
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
            entitySettingsBySettingNameMap, SPECS, getSchedules(), entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNow, settingsInPolicy3,
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities)),
            entitySettingsBySettingNameMap, SPECS, getSchedules(), entityToPolicySettings);

        entitySettingsBySettingNameMap.forEach((id, map) ->
            assertFalse(map.containsKey("settingSpec4")));
        entitySettingsBySettingNameMap.forEach((id, specNameToRecord) -> specNameToRecord.forEach(
            (spec, record) -> assertTrue(record.isScheduled())));
    }

    /**
     * Verify that if one policy is in schedule and the other doesn't have a schedule then
     * the one with a schedule wins.
     */
    @Test
    public void testOneScheduledOneUnscheduled() {
        final SettingPolicy settingPolicyWithSchedule = addSchedule(settingPolicy1, APPLIES_NOW);

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        final Map<Long, ResolvedGroup> resolvedGroups = Collections.singletonMap(
            group.getId(), resolvedGroup(group, entities));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyWithSchedule,
                settingsInPolicy1, resolvedGroups, entitySettingsBySettingNameMap, SPECS,
                getSchedules(), entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy3, settingsInPolicy3,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, getSchedules(),
                entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicy2, settingsInPolicy2,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, getSchedules(),
                entityToPolicySettings);

        appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), hasItem(setting1));
        assertTrue(entitySettingsBySettingNameMap.get(entityOid1).get(SPEC_1).isScheduled());
        assertTrue(entitySettingsBySettingNameMap.get(entityOid1).get(SPEC_2).isScheduled());
    }

    /**
     * One policy with schedule that is out of schedule and one without a schedule.
     */
    @Test
    public void testOneInapplicableScheduledOneUnscheduled() {

        final SettingPolicy settingPolicyNotNow = addSchedule(settingPolicy1, NOT_NOW);

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        final Map<Long, ResolvedGroup> resolvedGroups =
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicyNotNow, settingsInPolicy1,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy3, settingsInPolicy3,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertFalse(appliedSettings.isEmpty());
        assertTrue(appliedSettings.stream().allMatch(setting ->
            setting.getSettingPolicyIdList().equals(Collections.singleton(SP3_ID))));
    }

    /**
     * Verify that tie breaker is used to resolve conflicts properly.
     */
    @Test
    public void testApplyConflictResolution() {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        final Map<Long, ResolvedGroup> resolvedGroups =
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicy1, settingsInPolicy1,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        // Now check if the conflict resolution is done correctly. Use a policy that has
        // settings with the same specs but different values.
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy1a, settingsInPolicy1a,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);

        appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        // setting1 and setting1a both have same spec. Since
        // tieBreaker is smaller, setting1 should win
        assertThat(getSettings(appliedSettings), hasItem(setting1));
        // setting2 and setting2a both have same spec. Since
        // tieBreaker is smaller, setting2a should win.
        assertThat(getSettings(appliedSettings), hasItem(setting2a));
        entitySettingsBySettingNameMap.forEach((id, specNameToRecord) -> specNameToRecord.forEach(
            (spec, record) -> assertFalse(record.isScheduled())));
    }

    /**
     * Verify that user policy wins over discovered policy.
     */
    @Test
    public void testUserAndDiscovered() {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
                        new HashMap<>();

        final Multimap<Long, Pair<Long, Boolean>> entityToPolicySettings =
            ArrayListMultimap.create();

        final Map<Long, ResolvedGroup> resolvedGroups =
            Collections.singletonMap(group.getId(), resolvedGroup(group, entities));

        entitySettingsResolver.resolveAllEntitySettings(settingPolicy1, settingsInPolicy1,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);
        entitySettingsResolver.resolveAllEntitySettings(settingPolicy4, settingsInPolicy4,
                resolvedGroups, entitySettingsBySettingNameMap, SPECS, Collections.emptyMap(),
                entityToPolicySettings);

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
                entitySettingsBySettingNameMap.get(entityOid1).values());

        // Even though policy4 has setting2a which has a lower value, since the policy
        // is discovered, policy1 (which is a USER policy) wins.
        assertTrue(appliedSettings.stream().allMatch(setting ->
            setting.getSettingPolicyIdList().equals(Collections.singleton(SP1_ID))));
        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));
    }

    /**
     * Unit tests for tie-breakers.
     */
    @RunWith(Parameterized.class)
    public static class SettingsConflictTests {

        private static final String SPEC_NAME = "anotherSpec";
        private static final SettingSpec SPEC_SMALLER_TIEBREAKER =
            EntitySettingsResolverTest.createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER);
        private static final SettingSpec SPEC_BIGGER_TIEBREAKER =
            EntitySettingsResolverTest.createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER);
        private static final Setting BOOL_SETTING_BIGGER = createBooleanSetting(SPEC_NAME, true);
        private static final Setting BOOL_SETTING_SMALLER = createBooleanSetting(SPEC_NAME, false);

        private static final Setting NUMERIC_SETTING_BIGGER = createNumericSetting(SPEC_NAME, 20.0F);
        private static final Setting NUMERIC_SETTING_SMALLER = createNumericSetting(SPEC_NAME, 10.0F);

        private static final Setting STRING_SETTING_BIGGER = createStringSetting(SPEC_NAME, "bbb");
        private static final Setting STRING_SETTING_SMALLER = createStringSetting(SPEC_NAME, "aaa");

        private static final List<String> ENUM_VALUES = Arrays.asList("aaa", "bbb", "ccc", "ddd");
        private static final SettingSpec SPEC_ENUM_SMALLER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER, ENUM_VALUES);
        private static final SettingSpec SPEC_ENUM_BIGGER_TIEBREAKER =
            createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER, ENUM_VALUES);
        private static final Setting ENUM_SETTING_BIGGER = createEnumSetting(SPEC_NAME, "ddd");
        private static final Setting ENUM_SETTING_SMALLER = createEnumSetting(SPEC_NAME, "aaa");

        @Parameters(name = "{index}: testResolveConflict(label={0}, specName={1}, " +
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

        /**
         * Test conflict resolution for different cases.
         */
        @Test
        public void testResolveConflict() {
            final Map<String, SettingSpec> specs = ImmutableMap.of(specName, settingSpecTiebreaker);
            final TopologyProcessorSetting<?> conflictTPSetting1 =
                    TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                            Collections.singletonList(conflictSetting1));
            final TopologyProcessorSetting<?> conflictTPSetting2 =
                    TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                            Collections.singletonList(conflictSetting2));
            final TopologyProcessorSetting<?> resolvedTPSetting =
                    SettingResolver.applyTiebreaker(conflictTPSetting1, conflictTPSetting2, specs)
                            .getFirst();
            assertThat(testLabel,
                    TopologyProcessorSettingsConverter.toProtoSettings(resolvedTPSetting)
                            .iterator()
                            .next(), is(expectedSetting));
        }

        private static SettingSpec createSettingSpec(
                        String specName, SettingTiebreaker tieBreaker, List<String> enumValues) {
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

    /**
     * Verify that the setting policies are sent in the case of realtime topology.
     */
    @Test
    public void testSendEntitySettings() {

        final TopologyInfo info = TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(123456)
                .setTopologyType(TopologyType.REALTIME)
                .build();

        entitySettingsResolver.sendEntitySettings(info, Collections.singletonList(
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1),
                        Collections.singletonList(444444L), Collections.singletonList(444444L))), null);

        verify(testSettingPolicyService).uploadEntitySettings(any(StreamObserver.class));
    }

    /**
     * Verify that the setting policies are sent in the case of realtime topology.
     */
    @Test
    public void testStreamEntitySettingsRequest() {

        final TopologyInfo info = TopologyInfo.newBuilder()
            .setTopologyContextId(777)
            .setTopologyId(123456)
            .setTopologyType(TopologyType.REALTIME)
            .build();
        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            mock(StreamObserver.class);

        entitySettingsResolver.streamEntitySettingsRequest(info, Arrays.asList(
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1),
                        Collections.singletonList(444444L), Collections.singletonList(444444L)),
                createEntitySettings(entityOid2, Arrays.asList(setting2, setting1),
                        Collections.singletonList(444444L), Collections.singletonList(444444L))), requestObserver);
        verify(requestObserver, times(3)).onNext(any());
    }

    /**
     * Verify that the setting policies are not sent in the case of plan topology.
     */
    @Test
    public void testNoSendPlanEntitySettings() {
        final TopologyInfo info = TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(123456)
                .setTopologyType(TopologyType.PLAN)
                .build();

        entitySettingsResolver.sendEntitySettings(info, Collections.singletonList(
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1),
                        Collections.singletonList(444444L), Collections.singletonList(444444L))), null);

        verify(testSettingPolicyService, never()).updateSettingPolicy(any());
    }

    /**
     * EXTERNAL_APPROVAL action mode should be selected over MANUAL.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testExternalMoreConservativeThanManual() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
            Collections.singletonList(manualActionModeSetting),
            Collections.singletonList(groupId));

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
            Collections.singletonList(EXTERNAL_APPROVAL_SETTING),
            Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
            resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
            Arrays.asList(policy1, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
            SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
            Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));

        final GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
            createEntitySettings(entityOid1, Collections.singletonList(EXTERNAL_APPROVAL_SETTING),
                Collections.singletonList(SP2_ID), Arrays.asList(SP1_ID, SP2_ID)),
            createEntitySettings(entityOid2, Collections.singletonList(EXTERNAL_APPROVAL_SETTING),
                Collections.singletonList(SP2_ID), Arrays.asList(SP1_ID, SP2_ID)));
    }

    /**
     * RECOMMEND action mode should be selected over EXTERNAL_APPROVAL.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testRecommendMoreConservativeThanExternal() throws Exception {
        final SettingPolicy policy1 = createSettingPolicy(SP1_ID, "sp1", Type.USER,
            Collections.singletonList(RECOMMEND_SETTING),
            Collections.singletonList(groupId));

        final SettingPolicy policy2 = createSettingPolicy(SP2_ID, "sp2", Type.USER,
            Collections.singletonList(EXTERNAL_APPROVAL_SETTING),
            Collections.singletonList(groupId));

        final ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(
            resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any())).thenReturn(
            Arrays.asList(policy1, policy2));
        when(testGroupService.getGroups(any())).thenReturn(Collections.singletonList(group));

        when(testSettingService.searchSettingSpecs(
            SearchSettingSpecsRequest.getDefaultInstance())).thenReturn(
            Arrays.asList(ACTION_MODE_SETTING_SPEC, EXECUTION_SCHEDULE_SETTING_SPEC));

        final GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager, null);

        verify(groupResolver).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        verifyEntitySettingIsSame(entitiesSettings.getEntitySettings(),
            createEntitySettings(entityOid1, Collections.singletonList(RECOMMEND_SETTING),
                Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)),
            createEntitySettings(entityOid2, Collections.singletonList(RECOMMEND_SETTING),
                Collections.singletonList(SP1_ID), Arrays.asList(SP1_ID, SP2_ID)));
    }

    private static SettingPolicy createSettingPolicy(
                    long policyId, String name, SettingPolicy.Type type,
                    List<Setting> settings, List<Long> groupIds) {
        return  SettingPolicy.newBuilder()
            .setId(policyId)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setName(name)
                .addAllSettings(settings)
                .setEntityType(TEST_ENTITY_TYPE.typeNumber())
                .setScope(Scope.newBuilder()
                    .addAllGroups(groupIds)
                    .build())
                .build())
            .setSettingPolicyType(type)
            .build();
    }


    private static void verifyEntitySettingIsSame(Collection<EntitySettings> entitySettings,
                                           EntitySettings... expectedEntitySettings) {
        for (EntitySettings expectedEntitySetting : expectedEntitySettings) {
            final EntitySettings entitySetting =
                getEntitySettings(entitySettings, expectedEntitySetting.getEntityOid());

            assertThat(entitySetting.getUserSettingsList(),
                containsInAnyOrder(expectedEntitySetting.getUserSettingsList().toArray()));

            assertThat(entitySetting.getEntityPoliciesList(),
                containsInAnyOrder(expectedEntitySetting.getEntityPoliciesList().toArray()));
        }
    }

    private static EntitySettings getEntitySettings(Collection<EntitySettings> entitySettings,
                                                    long oid) {
        return entitySettings.stream()
            .filter(e -> e.getEntityOid() == oid)
            .findAny()
            .get();
    }

    private static SettingPolicy createUserSettingPolicy(long policyId, String name, List<Setting> settings) {
        return  SettingPolicy.newBuilder()
            .setId(policyId)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setName(name)
                .addAllSettings(settings)
                .setEntityType(TEST_ENTITY_TYPE.typeNumber())
                .build())
            .setSettingPolicyType(SettingPolicy.Type.USER)
            .build();
    }

    private static Schedule createOneTimeSchedule(long schduleId, long startTime, long endTime) {
        return Schedule.newBuilder()
                .setId(schduleId)
                .setOneTime(OneTime.getDefaultInstance())
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setTimezoneId("timeZoneId")
                .build();
    }

    private static EntitySettings createEntitySettings(
                    long entityOid, List<Setting> userSettings, Collection<Long> policyIds,
                    Collection<Long> associatedPolicy) {
        return EntitySettings.newBuilder()
            .setEntityOid(entityOid)
            .addAllUserSettings(createUserSettings(userSettings, policyIds))
            .addAllEntityPolicies(createEntityPolicy(associatedPolicy))
            .build();
    }

    private static EntitySettings createEntitySettings(long entityOid, List<Setting> userSettings,
                                                       Collection<Long> policyIds, long defaultPolicyId) {
        return EntitySettings.newBuilder()
            .setEntityOid(entityOid)
            .addAllUserSettings(createUserSettings(userSettings, policyIds))
            .setDefaultSettingPolicyId(defaultPolicyId)
            .addAllEntityPolicies(createEntityPolicy(policyIds))
            .build();
    }

    private static Collection<EntitySettings.EntitySettingsPolicy> createEntityPolicy(Collection<Long> policyIds) {
        return policyIds.stream()
            .map(id -> EntitySettings.EntitySettingsPolicy.newBuilder().setPolicyId(id).setActive(true).build())
            .collect(Collectors.toList());
    }

    private static EntitySettings createDefaultEntitySettings(long entityOid, long policyId) {
        return EntitySettings.newBuilder()
                        .setEntityOid(entityOid)
                        .setDefaultSettingPolicyId(policyId)
                        .build();
    }

    private static List<SettingToPolicyId> createUserSettings(List<Setting> userSettings,
            Collection<Long> policyIds) {
        return userSettings.stream()
                .map(setting -> SettingToPolicyId.newBuilder()
                        .setSetting(setting)
                        .addAllSettingPolicyId(policyIds)
                        .build())
                .collect(Collectors.toList());
    }

    private static Setting createNumericSetting(String name, float val) {
        return Setting.newBuilder().setSettingSpecName(name)
            .setNumericSettingValue(
                NumericSettingValue.newBuilder()
                    .setValue(val))
            .build();
    }

    private static Setting createBooleanSetting(String name, boolean val) {
        return Setting.newBuilder().setSettingSpecName(name)
            .setBooleanSettingValue(
                BooleanSettingValue.newBuilder()
                    .setValue(val))
            .build();
    }

    private static Setting createStringSetting(String name, String val) {
        return Setting.newBuilder().setSettingSpecName(name)
            .setStringSettingValue(
                StringSettingValue.newBuilder()
                    .setValue(val))
            .build();
    }

    private static Setting createEnumSetting(String name, String val) {
        return Setting.newBuilder().setSettingSpecName(name)
            .setEnumSettingValue(
                EnumSettingValue.newBuilder()
                    .setValue(val))
            .build();
    }

    private static Setting createSortedSetOfOidSetting(String name, Iterable<Long> val) {
        return Setting.newBuilder().setSettingSpecName(name)
            .setSortedSetOfOidSettingValue(
                SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(val))
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

    @Nonnull
    private static SettingSpec createActionModeSettingSpec(@Nonnull String specName,
            @Nonnull SettingTiebreaker tieBreaker, @Nonnull List<String> enumValues) {
        return SettingSpec.newBuilder()
                .setName(specName)
                .setEntitySettingSpec(
                        EntitySettingSpec.newBuilder().setTiebreaker(tieBreaker).build())
                .setEnumSettingValueType(
                        EnumSettingValueType.newBuilder().addAllEnumValues(enumValues))
                .build();
    }

    private static List<Setting> getSettings(List<SettingAndPolicyIdRecord> settingAndPolicyIdRecords) {
        return settingAndPolicyIdRecords.stream()
                .map(SettingAndPolicyIdRecord::getSetting)
                .map(TopologyProcessorSettingsConverter::toProtoSettings).flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private SettingPolicy addSchedule(SettingPolicy policy, Schedule schedule) {
        return policy.toBuilder().setInfo(policy.getInfo().toBuilder().setScheduleId(
            schedule.getId())).build();
    }

    private Map<Long, Schedule> getSchedules() {
        Map<Long, Schedule> schedules = Maps.newHashMap();
        schedules.put(APPLIES_NOW.getId(), APPLIES_NOW);
        schedules.put(NOT_NOW.getId(), NOT_NOW);
        return schedules;
    }

    /**
     * Test that settings policy editors are invoked, and in the correct order.
     *
     * @throws Exception should not happen, indicates a test failure.
     */
    @Test
    public void testApplySettingsPolicyEdits() throws Exception {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(resolvedGroup(group, entities));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        // Only default setting policy used
        when(testSettingPolicyService.listSettingPolicies(any()))
            .thenReturn(Collections.singletonList(defaultSettingPolicy));

        TestSettingPolicyEditor editor1 = new TestSettingPolicyEditor();
        TestSettingPolicyEditor editor2 = new TestSettingPolicyEditor();

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(
                groupResolver, topologyGraph, settingOverrides, rtTopologyInfo,
                consistentScalingManager, Arrays.asList(editor1, editor2));

        // Proves the editors were invoked
        assertNotNull(editor1.incomingList);
        assertNotNull(editor1.returnedList);
        assertNotNull(editor2.incomingList);
        assertNotNull(editor2.returnedList);

        // Proves the editors were invoked in order
        assertNotSame(editor1.incomingList, editor1.returnedList);
        assertSame(editor1.returnedList, editor2.incomingList);
        assertNotSame(editor2.incomingList, editor2.returnedList);
    }

    /**
     * Capture SettingPolicyEditor calls by EntitySettingsResolver for testing.
     */
    private class TestSettingPolicyEditor implements SettingPolicyEditor {
        public List<SettingPolicy> incomingList = null;
        public List<SettingPolicy> returnedList = null;
        public ResolvedGroup resolvedGroup = null;

        @Nonnull
        @Override
        public List<SettingPolicy> applyEdits(@Nonnull final List<SettingPolicy> settingPolicies,
                                              @Nonnull final Map<Long, ResolvedGroup> groups) {
            // Record the incoming list and return a different instance so that we can check
            // the chained processing of the lists by multiple editors.

            incomingList = settingPolicies;
            returnedList = new ArrayList<>();

            return returnedList;
        }
    }
}
