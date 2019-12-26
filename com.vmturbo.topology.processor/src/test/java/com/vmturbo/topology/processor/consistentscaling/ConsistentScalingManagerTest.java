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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
import com.vmturbo.commons.Pair;
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

    private void buildScalingGroups(ConsistentScalingManager csm) {
        csm.buildScalingGroups(topologyGraph, testGroups.values().iterator());
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
        for (long vmNum = 1; vmNum < 8; vmNum++) {
            topologyMap.put(vmNum, topologyEntityWithName(vmNum, EntityType.VIRTUAL_MACHINE,
                "VM-" + Long.toString(vmNum), 100L));
        }
        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        testGroups = new HashMap<>();
        makeGrouping(101L, "Group-A", 1L, 2L, 3L);
        makeGrouping(102L, "Group-B", 4L, 2L);  // The order ensures that group merge logic executes
        makeGrouping(103L, "Group-C", 5L);
        makeGrouping(104L, "DiscoveredGroup-A", 4L, 6L);
        makeGrouping(105L, "DiscoveredGroup-B", 7L);
        makeGrouping(106L, "One-VM-in-DiscoveredGroup-A", 6L);

        // Create the consistent scaling setting policies
        makeSettingPolicy(1001L, 101L, true);
        makeSettingPolicy(1002L, 102L, true);
        makeSettingPolicy(1003L, 103L, true);
        makeSettingPolicy(1006L, 106L, false);
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
        buildScalingGroups(csm);
        Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName = new HashMap<>();
        csm.addScalingGroupSettings(userSettingsByEntityAndName);
        // The are 7 VMs in the topology, and VM-6 has consistent scaling disabled via policy.
        // Ensure that there are 6 entries in the user settings map and that 6 does not exist.
        Assert.assertEquals(userSettingsByEntityAndName.keySet(),
                new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 7L)));
        // Ensure that the "scalingGroupMembership" setting is present for all of these
        Assert.assertTrue(userSettingsByEntityAndName.values().stream()
            .allMatch(m -> m.keySet()
                .contains(EntitySettingSpecs.ScalingGroupMembership.getSettingName())));
    }

    @Test
    public void testGetPoliciesStream() {
        ConsistentScalingManager csm = createCSM(true);
        populateCSMWithTestData(csm);
        buildScalingGroups(csm);
        List<Pair<Set<Long>, List<SettingPolicy>>> policies =
            csm.getPoliciesStream().collect(Collectors.toList());
        // There are three consistent scaling policies: 1001L, 1002L, and 1003L.  Ensure that they
        // are all present in the policies stream.
        Set<Long> uniqueGroups = new HashSet<>();
        csm.getPoliciesStream().forEach(p -> {
            uniqueGroups.addAll(p.second.stream().map(SettingPolicy::getId)
                .collect(Collectors.toList()));
        });
        Assert.assertEquals(new HashSet<>(Arrays.asList(1001L, 1002L, 1003L)), uniqueGroups);
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
        Assert.assertTrue(csm.getPoliciesStream().count() == 0);
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
        buildScalingGroups(csm);
        Assert.assertTrue(csm.getScalingGroupId(1L).isPresent());
        csm.clear();
        Assert.assertTrue(csm.getPoliciesStream().count() == 0);
        Assert.assertFalse(csm.getScalingGroupId(1L).isPresent());
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
        csm.buildScalingGroups(topologyGraph, groupResolver);        // Member of merged group
        Assert.assertEquals(Optional.of("Group-B, Group-A"), csm.getScalingGroupId(1L));
        // Member of non-merged group
        Assert.assertEquals(Optional.of("Group-C"), csm.getScalingGroupId(5L));
    }
}