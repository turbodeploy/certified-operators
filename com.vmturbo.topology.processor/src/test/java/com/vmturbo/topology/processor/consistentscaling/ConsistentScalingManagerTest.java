package com.vmturbo.topology.processor.consistentscaling;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityWithName;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver.SettingAndPolicyIdRecord;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Define five groups.  Two groups have common VMs.  Two are discovered with consistent scaling
 * enabled.  Groups A, B, and C have consistent scaling enabled via policy.  VM-6, which is in a
 * group with probe-defined consistent scaling, has consistent scaling disabled via policy.
 *
 * <p>Group-A: VM-1, VM-2, VM-3
 * Group-B: VM-2, VM-4
 * Group-C: VM-5
 * DiscoveredGroup-A: VM-4, VM-6
 * DiscoveredGroup-B: VM-7
 *
 * <p>Group-A merges with Group-B to form one scaling group because VM-2 is in both groups.
 * Group-C forms a separate scaling group
 */
public class ConsistentScalingManagerTest {
    private ConsistentScalingConfig config;
    private TopologyGraph<TopologyEntity> topologyGraph;
    Map<Long, Grouping> testGroups;
    Map<Long, SettingPolicy> consistentScalingPolicies = new HashMap<>();

    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
        spy(new SettingPolicyServiceMole());
    private final SettingServiceMole testSettingService = spy(new SettingServiceMole());
    private GroupServiceBlockingStub groupServiceClient;

    private void makeGrouping(long id, String name, Long... vmIds) {
        StaticMembersByType members = StaticMembersByType.newBuilder()
            .setType(MemberType.newBuilder().setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
            .addAllMembers(Arrays.asList(vmIds))
            .build();
        testGroups.put(id, Grouping.newBuilder()
                .setId(id)
                .setDefinition(GroupDefinition.newBuilder()
                    .setDisplayName(name)
                    .setStaticGroupMembers(StaticMembers.newBuilder().addMembersByType(members)))
                .build());
    }

    private void populateCSMWithTestData(ConsistentScalingManager csm) {
        // Add discovered scaling groups
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-A", true));
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-B", true));

        // Add consistent scaling policies
        consistentScalingPolicies.entrySet().forEach(entry -> {
            Grouping group = testGroups.get(entry.getKey());
            csm.addEntities(group, getTopologyEntityStreamFromGroup(group),
                Arrays.asList(entry.getValue()));
        });
    }

    private Map<Long, Map<String, SettingAndPolicyIdRecord>>
    buildScalingGroups(ConsistentScalingManager csm) {
        Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName =
            new HashMap<>();
        csm.buildScalingGroups(topologyGraph, testGroups.values().iterator(),
            userSettingsByEntityAndName);
        return userSettingsByEntityAndName;
    }

    private Stream<TopologyEntity> getTopologyEntityStreamFromGroup(final Grouping group) {
        return group.getDefinition().getStaticGroupMembers()
            .getMembersByType(0).getMembersList().stream()
            .map(entityId -> topologyGraph.getEntity(entityId).get());
    }

    private void makeSettingPolicy(long policyId, Long groupId,
                                            boolean enableConsistentScaling) {
        String policyName = "Policy-" + policyId;
        SettingPolicy policy = SettingPolicy.newBuilder()
            .setId(policyId)
            .setSettingPolicyType(Type.USER)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setDisplayName(policyName).setName(policyName)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnabled(true)
                .addSettings(Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.EnableConsistentResizing.getSettingName())
                    .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                        .setValue(enableConsistentScaling)))
                .setScope(Scope.newBuilder().addGroups(groupId)))
            .build();
        consistentScalingPolicies.put(groupId, policy);
    }

    private ConsistentScalingManager createCSM(boolean enabled) {
        config = mock(ConsistentScalingConfig.class);
        when(config.isEnabled()).thenReturn(enabled);
        return new ConsistentScalingManager(config);
    }

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
        testSettingPolicyService, testSettingService);

    @Before
    public void setUp() throws Exception {
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        Map<Long, Builder> topologyMap = new HashMap<>();
        topologyMap.put(100L, topologyEntity(100L, EntityType.PHYSICAL_MACHINE));
        for (long vmNum = 1; vmNum <= 12; vmNum++) {
            TopologyEntity.Builder builder =
                topologyEntityWithName(vmNum, EntityType.VIRTUAL_MACHINE,
                    "VM-" + Long.toString(vmNum), 100L);
            // Make VMs 10 and above not controllable.
            if (vmNum >= 10) {
                builder.getEntityBuilder().setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setControllable(false));
            }
            topologyMap.put(vmNum, builder);
        }
        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        testGroups = new HashMap<>();
        makeGrouping(101L, "Group-A", 1L, 2L, 3L);
        makeGrouping(102L, "Group-B", 4L, 2L);  // The order ensures that group merge logic executes
        makeGrouping(103L, "Group-C", 5L);
        makeGrouping(104L, "DiscoveredGroup-A", 4L, 6L);
        makeGrouping(105L, "DiscoveredGroup-B", 7L);
        makeGrouping(106L, "One-VM-in-DiscoveredGroup-A", 6L);
        // VMs 10, 11 and 12 are powered off
        makeGrouping(109L, "One-VM-powered-off", 9L, 10L);
        makeGrouping(111L, "All-VMs-powered-off", 11L, 12L);

        // Create the consistent scaling setting policies
        makeSettingPolicy(1001L, 101L, true);
        makeSettingPolicy(1002L, 102L, true);
        makeSettingPolicy(1003L, 103L, true);
        makeSettingPolicy(1006L, 106L, false);
        makeSettingPolicy(1009L, 109L, true);
        makeSettingPolicy(1011L, 111L, true);
    }

    private InterpretedGroup makeInterpretedGroup(String groupName, boolean consistentResizing) {
        CommonDTO.GroupDTO dto = CommonDTO.GroupDTO.newBuilder()
            .setGroupName(groupName)
            .setDisplayName(groupName)
            .setIsConsistentResizing(consistentResizing)
            .build();
        InterpretedGroup group = new InterpretedGroup(dto, Optional.of(GroupDefinition.newBuilder()));
        return group;
    }

    @Test
    public void testAddScalingGroupSettings() {
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);
        Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName =
                buildScalingGroups(csm);
        csm.addScalingGroupSettings(userSettingsByEntityAndName);
        // The are 7 VMs in the topology, and VM-6 has consistent scaling disabled via policy.
        // Ensure that there are 6 entries in the user settings map and that 6 does not exist.
        Assert.assertEquals(userSettingsByEntityAndName.keySet(),
                new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 7L, 9L)));
        // Ensure that the "scalingGroupMembership" setting is present for all of these
        Assert.assertTrue(userSettingsByEntityAndName.values().stream()
            .allMatch(m -> m.keySet()
                .contains(EntitySettingSpecs.ScalingGroupMembership.getSettingName())));
    }

    @Test
    public void testAddEntities() {
        // addEntities() is already tested in XXX.  Here tests when consistent scaling is globally
        // disabled.
        ConsistentScalingManager csm = createCSM(false);
        populateCSMWithTestData(csm);
        buildScalingGroups(csm);
        // There should be no groups or policies defined
        Assert.assertFalse(csm.getScalingGroupId(1L).isPresent());
    }

    /**
     * Ensure that non-controllable entities are not included in scaling groups.  Also, ensure that
     * a group with no controllable entities is not created.
     */
    @Test
    public void testDisabledEntities() {
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);

        buildScalingGroups(csm);
        // Group 1009 and 1011 have two VMs each.  Group 1009 (VM-9 and VM-10) has one
        // non-controllable VM.
        Assert.assertTrue(csm.getScalingGroupId(9L).isPresent());
        Assert.assertFalse(csm.getScalingGroupId(10L).isPresent());
        Assert.assertEquals("One-VM-powered-off", csm.getScalingGroupId(9L).get());
        // Group 1011 (VM-11, VM-12) has both VMs non-controllable, so the scaling group should
        // not be created.  The internal group list is not exposed in the CSM and I don't feel like
        // doing it just for test, so if the scaling group ID query for both members returns empty,
        // then we can be assured that there was no group created for them.
        Assert.assertFalse(csm.getScalingGroupId(11L).isPresent());
        Assert.assertFalse(csm.getScalingGroupId(12L).isPresent());
    }

    @Test
    public void testGetScalingGroupId() {
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);
        buildScalingGroups(csm);
        // Invalid entity OID
        Assert.assertEquals(Optional.empty(), csm.getScalingGroupId(null));
        // Member of merged group
        Assert.assertEquals(Optional.of("Group-B, Group-A"), csm.getScalingGroupId(1L));
        // Member of non-merged group
        Assert.assertEquals(Optional.of("Group-C"), csm.getScalingGroupId(5L));
    }

    @Test
    public void testClear() {
        ConsistentScalingManager csm = createCSM(true);
        // Build groups and verify group membership of an entity
        populateCSMWithTestData(csm);
        Map<Long, Map<String, SettingAndPolicyIdRecord>> settingsMaps = buildScalingGroups(csm);
        Assert.assertTrue(csm.getScalingGroupId(1L).isPresent());
        // VM-9 is in a scaling group, so it should be in the settings map
        Assert.assertNotNull(settingsMaps.get(9L));
        csm.clear();
        Assert.assertFalse(csm.getScalingGroupId(1L).isPresent());

        // Repopulate
        populateCSMWithTestData(csm);
        // Clear discovered groups
        csm.clearDiscoveredGroups();
        buildScalingGroups(csm);
        // VM-7 is in DiscoveredGroup-B, so now it should no longer be in there
        Assert.assertFalse(csm.getScalingGroupId(7L).isPresent());
    }

    @Test
    public void testAddDiscoveredGroupEnabled() {
        ConsistentScalingManager csm = createCSM(true);
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-A", true));
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-B", true));
        buildScalingGroups(csm);
        Assert.assertEquals(Optional.of("DiscoveredGroup-B"), csm.getScalingGroupId(7L));
        Assert.assertEquals(Optional.empty(), csm.getScalingGroupId(1L));
    }

    @Test
    public void testAddDiscoveredGroupDisabled() {
        // Globally disable consistent scaling
        ConsistentScalingManager csm = createCSM(false);
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-A", true));
        csm.addDiscoveredGroup(makeInterpretedGroup("DiscoveredGroup-B", true));
        buildScalingGroups(csm);
        Assert.assertEquals(Optional.empty(), csm.getScalingGroupId(7L));
    }

    @Test
    public void testBuildScalingGroups() {
        // buildScalingGroups() is tested is other areas.  This test targets processing groups
        // returned from a group query.
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);
        final GroupResolver groupResolver = mock(GroupResolver.class);
        when(groupResolver.getGroupServiceClient()).thenReturn(groupServiceClient);
        when(testGroupService.getGroups(any()))
                .thenReturn(new ArrayList<>(testGroups.values()));
        csm.buildScalingGroups(topologyGraph, groupResolver, new HashMap<>());
        Assert.assertEquals(Optional.of("Group-B, Group-A"), csm.getScalingGroupId(1L));
        // Member of non-merged group
        Assert.assertEquals(Optional.of("Group-C"), csm.getScalingGroupId(5L));
    }

    /**
     * Verify that the OID to settings map shares map between members of the same scaling group.
     */
    @Test
    public void testPreMergeScalingGroups() {
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);
        final GroupResolver groupResolver = mock(GroupResolver.class);
        when(groupResolver.getGroupServiceClient()).thenReturn(groupServiceClient);
        when(testGroupService.getGroups(any()))
            .thenReturn(new ArrayList<>(testGroups.values()));
        Map<Long, Map<String, SettingAndPolicyIdRecord>> settingsMaps = new HashMap<>();
                csm.buildScalingGroups(topologyGraph, groupResolver, settingsMaps);
        // There are 7 powered on entities in the test topology, all of them in scaling groups, so
        // the settings map must also contain 7 entries.
        Assert.assertEquals(7, settingsMaps.size());
        /*
         * Group-A: VM-1, VM-2, VM-3
         * Group-B: VM-2, VM-4
         * Group-C: VM-5
         * DiscoveredGroup-A: VM-4, VM-6
         * DiscoveredGroup-B: VM-7
         *
         * Internal Groups:
         * - SG-1: VM-1, 2, 3, 4
         * - SG-2: VM-5
         * - SG-3: VM-7
         * - SG-4: VM-9
         */
        final Map<String, SettingAndPolicyIdRecord> m1 = settingsMaps.get(1L);  // Get settings map for VM-1
        Assert.assertNotNull(m1);
        Assert.assertEquals(4, settingsMaps.values().stream().filter(settings -> settings == m1).count());

        final Map<String, SettingAndPolicyIdRecord> m2 = settingsMaps.get(5L);  // Get settings map for VM-5
        Assert.assertNotNull(m2);
        Assert.assertEquals(1, settingsMaps.values().stream().filter(settings -> settings == m2).count());

        final Map<String, SettingAndPolicyIdRecord> m3 = settingsMaps.get(7L);  // Get settings map for VM-7
        Assert.assertNotNull(m3);
        Assert.assertEquals(1, settingsMaps.values().stream().filter(settings -> settings == m3).count());

        final Map<String, SettingAndPolicyIdRecord> m4 = settingsMaps.get(9L);  // Get settings map for VM-9
        Assert.assertNotNull(m4);
        Assert.assertEquals(1, settingsMaps.values().stream().filter(settings -> settings == m4).count());
    }
}