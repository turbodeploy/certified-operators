package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingAndPolicyIdRecord;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingResolver;

/**
 * Unit tests for {@link EntitySettingsResolver}.
 *
 */
public class EntitySettingsResolverTest {

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
    private static final Setting setting1 = createNumericSetting(SPEC_1, 10f);
    // Used to verify tie-breaker. It has the same spec as setting1 but different value.
    private static final Setting setting1a = createNumericSetting(SPEC_1, 15f);
    private static final Setting setting2 = createNumericSetting(SPEC_2, 20f);
    private static final Setting setting2a = createNumericSetting(SPEC_2, 18f);
    private static final Setting setting3 = createNumericSetting(SPEC_3, 30f);
    private static final Setting setting4 = createNumericSetting(SPEC_4, 50f);
    private static final long SP1_ID = 6001;
    private static final long SP1A_ID = 6002;
    private static final long SP2_ID = 6003;
    private static final long SP3_ID = 6004;
    private static final long SP4_ID = 6005;
    private static final long DEFUALT_POLICY_ID = 6101L;
    private static final List<Setting> inputSettings1  = Arrays.asList(setting1, setting2);
    private static final List<Setting> inputSettings1a  = Arrays.asList(setting1a, setting2a);
    private static final List<Setting> inputSettings2  = Arrays.asList(setting3, setting4);
    private static final SettingPolicy settingPolicy1 =
                    createSettingPolicy(SP1_ID, "sp1", SettingPolicy.Type.USER, inputSettings1,
                        Collections.singletonList(groupId));
    private static final SettingPolicy settingPolicy1a =
                    createSettingPolicy(SP1A_ID, "sp1a", SettingPolicy.Type.USER, inputSettings1a,
                        Collections.singletonList(groupId));
    // this policy has no scope (group)
    private static final SettingPolicy settingPolicy2 =
        createUserSettingPolicy(SP2_ID, "sp2", inputSettings2);
    private static final SettingPolicy settingPolicy3 =
                    createSettingPolicy(SP3_ID, "sp3", SettingPolicy.Type.USER, inputSettings1,
                                    Collections.singletonList(groupId));
    private static final SettingPolicy settingPolicy4 =
                    createSettingPolicy(SP4_ID, "sp4", SettingPolicy.Type.DISCOVERED, inputSettings1a,
                                    Collections.singletonList(groupId));
    private static final SettingPolicy defaultSettingPolicy =
        createSettingPolicy(DEFUALT_POLICY_ID, "sp_def", SettingPolicy.Type.DEFAULT,
            inputSettings1, Collections.singletonList(groupId));

    private static final String SPEC_NAME = "settingSpecName";
    private static final SettingSpec SPEC_SMALLER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.SMALLER);
    private static final SettingSpec SPEC_BIGGER_TIEBREAKER =
        createSettingSpec(SPEC_NAME, SettingTiebreaker.BIGGER);

    private static final int TEST_ENTITY_TYPE = 73; // arbitrary number

    private static final TopologyEntityDTO.Builder entity1 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid1)
            .setEntityType(TEST_ENTITY_TYPE);

    private static final TopologyEntityDTO.Builder entity2 =
        TopologyEntityDTO.newBuilder()
            .setOid(entityOid2)
            .setEntityType(TEST_ENTITY_TYPE);

    private static final TopologyEntity topologyEntity1 = topologyEntity(entity1);
    private static final TopologyEntity topologyEntity2 = topologyEntity(entity2);

    private static final Map<String, SettingSpec> SPECS = ImmutableMap.<String, SettingSpec>builder()
            .put(SPEC_1, SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER).setName(SPEC_1).build())
            .put(SPEC_2, SettingSpec.newBuilder(SPEC_SMALLER_TIEBREAKER).setName(SPEC_2).build())
            .put(SPEC_3, SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER).setName(SPEC_3).build())
            .put(SPEC_4, SettingSpec.newBuilder(SPEC_BIGGER_TIEBREAKER).setName(SPEC_4).build())
            .build();

    //private static final ScheduleResolver scheduleResolver = mock(ScheduleResolver.class);
    private static final Schedule APPLIES_NOW =
        createOneTimeSchedule(11L, 4815162342L, 4815169500L)
        .toBuilder()
        .setActive(Active.getDefaultInstance())
        .build();
    private static final Schedule NOT_NOW = createOneTimeSchedule(22L, 11235813L,  11242980L);
    private static final int CHUNK_SIZE = 1;
    private static final ConsistentScalingManager consistentScalingManager =
            mock(ConsistentScalingManager.class);
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

    /**
     * Verify that policy1 wins when using SMALLER tie-breaker on both settings in the policy.
     */
    @Test
    public void testApplyUserSettings() {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        // returns only entities 1 and 2 even though group contains 3 entities
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));
        when(consistentScalingManager.getPoliciesStream()).thenReturn(Stream.empty());

        GraphWithSettings entitiesSettings = entitySettingsResolver.resolveSettings(groupResolver,
                topologyGraph, settingOverrides, rtTopologyInfo, consistentScalingManager);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        // Both entities expected to resolve to policy1 for both settings
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
            createEntitySettings(entityOid1, Arrays.asList(setting2, setting1), SP1_ID),
            createEntitySettings(entityOid2, Arrays.asList(setting2, setting1), SP1_ID)));
    }

    /**
     * Verify that when there are no user or discovered policies, default policies are used.
     */
    @Test
    public void testApplyDefaultSettings() {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        // Only default setting policy used
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(defaultSettingPolicy));
        when(consistentScalingManager.getPoliciesStream()).thenReturn(Stream.empty());

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(
                groupResolver, topologyGraph, settingOverrides, rtTopologyInfo,
                consistentScalingManager);

        verify(groupResolver, never()).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
            createDefaultEntitySettings(entityOid1, DEFUALT_POLICY_ID),
            createDefaultEntitySettings(entityOid2, DEFUALT_POLICY_ID)));
    }

    /**
     * Verify that when there are user policies as well as default policies - user policies win.
     */
    @Test
    public void testApplyUserSettingsOverridesDefault() {
        ArgumentCaptor<Grouping> groupArguments = ArgumentCaptor.forClass(Grouping.class);
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Arrays.asList(settingPolicy1, settingPolicy2, defaultSettingPolicy));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));
        when(consistentScalingManager.getPoliciesStream()).thenReturn(Stream.empty());

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager);

        verify(groupResolver, times(1)).resolve(groupArguments.capture(), eq(topologyGraph));
        verify(settingOverrides, times(2)).overrideSettings(any(), any());
        assertEquals(entitiesSettings.getEntitySettings().size(), 2);
        assertThat(entitiesSettings.getEntitySettings(), containsInAnyOrder(
             createEntitySettings(entityOid1,
                Arrays.asList(setting2, setting1), SP1_ID, DEFUALT_POLICY_ID),
             createEntitySettings(entityOid2,
                Arrays.asList(setting2, setting1), SP1_ID, DEFUALT_POLICY_ID)));
    }

    /**
     * Verify that when a policy is not associated with a group, it is not applied.
     */
    @Test
    public void testApplySettingsWhenSettingPolicyHasNoGroups() {
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(entities);
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1, topologyEntity2));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.singletonList(settingPolicy2));
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));
        when(consistentScalingManager.getPoliciesStream()).thenReturn(Stream.empty());

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager);

        // settingPolicy2 doesn't have groups or ids. So it should't be in the final result
        assertTrue(entitiesSettings.getEntitySettings().stream()
            .allMatch(setting -> setting.getUserSettingsList().isEmpty()));
    }

    /**
     * Test the case when there are no policies at all.
     */
    @Test
    public void testNoUserOrDefaultSettingPolicies() {
        when(groupResolver.resolve(group, topologyGraph)).thenReturn(ImmutableSet.of(entityOid1));
        when(topologyGraph.entities()).thenReturn(Stream.of(topologyEntity1));
        when(testSettingPolicyService.listSettingPolicies(any()))
           .thenReturn(Collections.emptyList());
        when(testGroupService.getGroups(any()))
            .thenReturn(Collections.singletonList(group));
        when(consistentScalingManager.getPoliciesStream()).thenReturn(Stream.empty());

        GraphWithSettings entitiesSettings =
            entitySettingsResolver.resolveSettings(groupResolver, topologyGraph,
                settingOverrides, rtTopologyInfo, consistentScalingManager);

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

        List<Long> excludedTiers2 = Arrays.asList(tier2, tier3);
        final Setting setting2 = createSortedSetOfOidSetting(specName, excludedTiers2);
        final SettingPolicy settingPolicy2 = createSettingPolicy(SP2_ID, "sp2",
            SettingPolicy.Type.USER, Collections.singletonList(setting2),
            Collections.singletonList(11L));

        List<Long> excludedTiers3 = Arrays.asList(tier3, tier4);
        final Setting setting3 = createSortedSetOfOidSetting(specName, excludedTiers3);
        final SettingPolicy settingPolicy3 = createSettingPolicy(SP3_ID, "sp3",
            SettingPolicy.Type.DISCOVERED, Collections.singletonList(setting3),
            Collections.singletonList(12L));

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();
        final Map<String, SettingSpec> settingNameToSettingSpecs =
            Collections.singletonMap(specName, EntitySettingSpecs.ExcludedTemplates.getSettingSpec());

        entitySettingsResolver.resolveAllEntitySettings(Sets.newHashSet(entityOid2, entityOid3),
            Collections.singletonList(settingPolicy2),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap());
        entitySettingsResolver.resolveAllEntitySettings(Sets.newHashSet(entityOid1, entityOid2),
            Collections.singletonList(settingPolicy1),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap());
        entitySettingsResolver.resolveAllEntitySettings(Sets.newHashSet(entityOid3),
            Collections.singletonList(settingPolicy3),
            entitySettingsBySettingNameMap, settingNameToSettingSpecs, Collections.emptyMap());

        SettingAndPolicyIdRecord record1 = entitySettingsBySettingNameMap.get(entityOid1).get(specName);
        assertEquals(SettingPolicy.Type.DISCOVERED, record1.getType());
        assertEquals(Collections.singleton(SP1_ID), record1.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier1, tier2)),
            record1.getSetting());

        SettingAndPolicyIdRecord record2 = entitySettingsBySettingNameMap.get(entityOid2).get(specName);
        assertEquals(SettingPolicy.Type.USER, record2.getType());
        assertEquals(ImmutableSet.of(SP2_ID, SP1_ID), record2.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier1, tier2, tier3)),
            record2.getSetting());

        SettingAndPolicyIdRecord record3 = entitySettingsBySettingNameMap.get(entityOid3).get(specName);
        assertEquals(SettingPolicy.Type.USER, record3.getType());
        assertEquals(ImmutableSet.of(SP2_ID, SP3_ID), record3.getSettingPolicyIdList());
        assertEquals(createSortedSetOfOidSetting(specName, Arrays.asList(tier2, tier3, tier4)),
            record3.getSetting());
    }

    /**
     * Test the {@link EntitySettingsResolver#resolveAllEntitySettings} method.
     */
    @Test
    public void testApply() {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();
        Map<String, SettingSpec> settingSpecs = new HashMap<>();
        List<SettingPolicy> policies = Collections.singletonList(settingPolicy1);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, settingSpecs, Collections.emptyMap());

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
        List<SettingPolicy> policies = Collections.singletonList(settingPolicyNotNow);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, settingSpecs, Collections.emptyMap());

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

        entitySettingsResolver.resolveAllEntitySettings(entities,
            Collections.singletonList(settingPolicyNow1),
            entitySettingsBySettingNameMap, SPECS, getSchedules());

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));
        // Verify that policy1 wins
        assertEquals(appliedSettings.get(0).getSettingPolicyIdList(),
            Collections.singleton(settingPolicyNow1.getId()));

        entitySettingsResolver.resolveAllEntitySettings(entities,
            Collections.singletonList(settingPolicyNow2),
            entitySettingsBySettingNameMap, SPECS, getSchedules());

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

        List<SettingPolicy> policies = Lists.newArrayList(settingPolicyNotNow, settingPolicyNow);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, SPECS, getSchedules());

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        entitySettingsResolver.resolveAllEntitySettings(entities,
            Collections.singletonList(settingPolicyNow),
            entitySettingsBySettingNameMap, SPECS, getSchedules());

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

        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        List<SettingPolicy> policies = Lists.newArrayList(settingPolicyWithSchedule, settingPolicy3);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, SPECS,getSchedules());

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        entitySettingsResolver.resolveAllEntitySettings(entities,
            Collections.singletonList(settingPolicy2),
            entitySettingsBySettingNameMap, SPECS, getSchedules());

        appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), hasItem(setting1));
        assertFalse(entitySettingsBySettingNameMap.get(entityOid1).get(SPEC_4).isScheduled());
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

        List<SettingPolicy> policies = Lists.newArrayList(settingPolicyNotNow, settingPolicy2);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, SPECS, Collections.emptyMap());

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertFalse(appliedSettings.isEmpty());
        assertTrue(appliedSettings.stream().allMatch(setting ->
            setting.getSettingPolicyIdList().equals(Collections.singleton(SP2_ID))));
    }

    /**
     * Verify that tie breaker is used to resolve conflicts properly.
     */
    @Test
    public void testApplyConflictResolution() {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> entitySettingsBySettingNameMap =
            new HashMap<>();

        List<SettingPolicy> policies = Collections.singletonList(settingPolicy1);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, SPECS, Collections.emptyMap());

        List<SettingAndPolicyIdRecord> appliedSettings = new ArrayList<>(
            entitySettingsBySettingNameMap.get(entityOid1).values());

        assertThat(getSettings(appliedSettings), containsInAnyOrder(setting1, setting2));

        // Now check if the conflict resolution is done correctly. Use a policy that has
        // settings with the same specs but different values.
        entitySettingsResolver.resolveAllEntitySettings(entities,
            Collections.singletonList(settingPolicy1a),
            entitySettingsBySettingNameMap, SPECS, Collections.emptyMap());

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

        List<SettingPolicy> policies = Lists.newArrayList(settingPolicy1, settingPolicy4);

        entitySettingsResolver.resolveAllEntitySettings(entities, policies,
            entitySettingsBySettingNameMap, SPECS, Collections.emptyMap());

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

        @Test
        public void testResolveConflict() {
            Map<String, SettingSpec> specs = ImmutableMap.of(specName, settingSpecTiebreaker);
            Setting resolvedSetting =
                SettingResolver.applyTiebreaker(conflictSetting1, conflictSetting2, specs);
            assertThat(testLabel, resolvedSetting, is(expectedSetting));
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
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1), 444444L)));

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
            createEntitySettings(entityOid1, Arrays.asList(setting2, setting1), 444444L),
            createEntitySettings(entityOid2, Arrays.asList(setting2, setting1), 444444L)), requestObserver);
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
                createEntitySettings(entityOid1, Arrays.asList(setting2, setting1), 444444L)));

        verify(testSettingPolicyService, never()).updateSettingPolicy(any());
    }

    private static SettingPolicy createSettingPolicy(
                    long policyId, String name, SettingPolicy.Type type,
                    List<Setting> settings, List<Long> groupIds) {
        return  SettingPolicy.newBuilder()
            .setId(policyId)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setName(name)
                .addAllSettings(settings)
                .setEntityType(TEST_ENTITY_TYPE)
                .setScope(Scope.newBuilder()
                    .addAllGroups(groupIds)
                    .build())
                .build())
            .setSettingPolicyType(type)
            .build();
    }

    private static SettingPolicy createUserSettingPolicy(long policyId, String name, List<Setting> settings) {
        return  SettingPolicy.newBuilder()
            .setId(policyId)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setName(name)
                .addAllSettings(settings)
                .setEntityType(TEST_ENTITY_TYPE)
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
                    long entityOid, List<Setting> userSettings, long policyId) {
        return EntitySettings.newBuilder()
            .setEntityOid(entityOid)
            .addAllUserSettings(createUserSettings(userSettings, policyId))
            .build();
    }

    private static EntitySettings createEntitySettings(
                    long entityOid, List<Setting> userSettings, long policyId, long defaultPolicyId) {
        return EntitySettings.newBuilder()
            .setEntityOid(entityOid)
            .addAllUserSettings(createUserSettings(userSettings, policyId))
            .setDefaultSettingPolicyId(defaultPolicyId)
            .build();
    }

    private static EntitySettings createDefaultEntitySettings(long entityOid, long policyId) {
        return EntitySettings.newBuilder()
                        .setEntityOid(entityOid)
                        .setDefaultSettingPolicyId(policyId)
                        .build();
    }

    private static List<SettingToPolicyId> createUserSettings(List<Setting> userSettings, long policyId) {
        return userSettings.stream()
                .map(setting -> SettingToPolicyId.newBuilder()
                        .setSetting(setting)
                        .addSettingPolicyId(policyId)
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

    private static List<Setting> getSettings(List<SettingAndPolicyIdRecord> settingAndPolicyIdRecords) {
        return settingAndPolicyIdRecords.stream()
                .map(settingAndPolicyIdRecord -> settingAndPolicyIdRecord.getSetting())
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
}
