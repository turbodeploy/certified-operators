package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.builders.PhysicalMachineBuilder;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.InterpretedGroup;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class DiscoveredSettingPolicyScannerTest {
    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private final DiscoveredGroupUploader groupUploader = mock(DiscoveredGroupUploader.class);

    private final StitchingContext stitchingContext = mock(StitchingContext.class);

    private final DiscoveredSettingPolicyScanner scanner = new DiscoveredSettingPolicyScanner(probeStore, targetStore);

    @Captor
    private ArgumentCaptor<List<InterpretedGroup>> groupsCaptor;

    @Captor
    private ArgumentCaptor<List<DiscoveredSettingPolicyInfo>> settingPolicyCaptor;

    private final List<TopologyStitchingEntity> hosts = new ArrayList<>();

    private static final long VC_TARGET_ID = 782634L;
    private static final long VMM_TARGET_ID = 9891371236L;

    private static final long HOST_1_ID = 1L;
    private static final long HOST_2_ID = 2L;
    private static final long HOST_3_ID = 3L;

    private static final String COMPUTE_CLUSTER_NAME = "compute-cluster";
    private static final String GROUP_COMPONENT_COMPUTE_CLUSTER_NAME = COMPUTE_CLUSTER_NAME +
        "-" + EntityType.PHYSICAL_MACHINE;


    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        MockitoAnnotations.initMocks(this);

        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType())).thenReturn(Optional.empty());
        when(probeStore.getProbeIdForType(SDKProbeType.VMM.getProbeType())).thenReturn(Optional.empty());
    }

    @Test
    public void testNoThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID));

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader, never()).addDiscoveredGroupsAndPolicies(anyLong(),
            anyListOf(InterpretedGroup.class),
            anyListOf(DiscoveredSettingPolicyInfo.class));
    }

    @Test
    public void testDefaultThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID)
            .withMemThreshold(DiscoveredSettingPolicyScanner.DEFAULT_UTILIZATION_THRESHOLD));

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader, never()).addDiscoveredGroupsAndPolicies(anyLong(),
            anyListOf(InterpretedGroup.class),
            anyListOf(DiscoveredSettingPolicyInfo.class));
    }

    @Test
    public void testVcMemThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID).withMemThreshold(50.0));
        final DiscoveredSettingPolicyInfo settingPolicy = memSettingPolicy(VC_TARGET_ID, 50.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Collections.singletonList(settingPolicy)));
    }

    @Test
    public void testVcCpuThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID).withCpuThreshold(67.0));
        final DiscoveredSettingPolicyInfo settingPolicy = cpuSettingPolicy(VC_TARGET_ID, 67.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Collections.singletonList(settingPolicy)));
    }

    @Test
    public void testVcMemAndCpuThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0));
        final DiscoveredSettingPolicyInfo settingPolicy = memAndCpuSettingPolicy(VC_TARGET_ID, 90.0, 67.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Collections.singletonList(settingPolicy)));
    }

    @Test
    public void testVcMultipleSameThresholds() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID, HOST_2_ID);
        givenHosts(VC_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0),
            host(HOST_2_ID).withMemThreshold(90.0).withCpuThreshold(67.0));
        final DiscoveredSettingPolicyInfo settingPolicy = memAndCpuSettingPolicy(VC_TARGET_ID, 90.0, 67.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        // Even though we have added two hosts with mem and cpu thresholds, we should only create one setting
        // policy because the hosts have the same value.
        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Collections.singletonList(settingPolicy)));
    }

    @Test
    public void testVcMultipleDifferentThresholds() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID, HOST_2_ID);
        givenHosts(VC_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0),
            host(HOST_2_ID).withMemThreshold(60.0).withCpuThreshold(50.0));
        final DiscoveredSettingPolicyInfo firstSettingPolicy = memAndCpuSettingPolicy(VC_TARGET_ID, 90.0, 67.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);
        final DiscoveredSettingPolicyInfo secondSettingPolicy = memAndCpuSettingPolicy(VC_TARGET_ID, 60.0, 50.0,
            GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        // This should create two conflicting settings policies on the same cluster. We'll pick the more
        // conservative values per conflict resolution when actually applying settings.
        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Arrays.asList(firstSettingPolicy, secondSettingPolicy)));
    }

    @Test
    public void testSingleVmmHost() {
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0));

        final DiscoveredSettingPolicyInfo.Builder settingPolicy =
            memAndCpuSettingPolicy(VMM_TARGET_ID, 90.0, 67.0).toBuilder();
        final InterpretedGroup group = setupHostGroup(settingPolicy, hosts);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VMM_TARGET_ID),
            eq(Collections.singletonList(group)),
            eq(Collections.singletonList(settingPolicy.build())));
    }

    @Test
    public void testMultipleVmmHostsSameThresholds() {
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0),
            host(HOST_2_ID).withMemThreshold(90.0).withCpuThreshold(67.0));

        final DiscoveredSettingPolicyInfo.Builder settingPolicy =
            memAndCpuSettingPolicy(VMM_TARGET_ID, 90.0, 67.0).toBuilder();
        final InterpretedGroup group = setupHostGroup(settingPolicy, hosts);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VMM_TARGET_ID),
            eq(Collections.singletonList(group)),
            eq(Collections.singletonList(settingPolicy.build())));
    }

    @Test
    public void testMultipleVmmHostsDifferentThresholds() {
        // Should create two discovered groups and two thresholds
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0),
            host(HOST_2_ID).withMemThreshold(90.0).withCpuThreshold(67.0),
            host(HOST_3_ID).withMemThreshold(50.0).withCpuThreshold(67.0));

        // The first two hosts should be put in a single group because they share the same mem and CPU thresholds.
        final DiscoveredSettingPolicyInfo.Builder settingPolicy1 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, 90.0, 67.0).toBuilder();
        final InterpretedGroup group1 = setupHostGroup(settingPolicy1, hosts.subList(0, 2));
        // The last host should be put in its own group because it has different thresholds.
        final DiscoveredSettingPolicyInfo.Builder settingPolicy2 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, 50.0, 67.0).toBuilder();
        final InterpretedGroup group2 = setupHostGroup(settingPolicy2, hosts.subList(2, 3));

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VMM_TARGET_ID),
            groupsCaptor.capture(),
            settingPolicyCaptor.capture());

        assertThat(groupsCaptor.getValue(), containsInAnyOrder(group1, group2));
        assertThat(settingPolicyCaptor.getValue(), containsInAnyOrder(settingPolicy1.build(), settingPolicy2.build()));
    }

    @Test
    public void testVcAndVmmTogether() {
        // Should create two discovered groups and two thresholds
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, COMPUTE_CLUSTER_NAME, HOST_2_ID);
        givenHosts(VMM_TARGET_ID, host(HOST_1_ID).withMemThreshold(90.0).withCpuThreshold(67.0));
        givenHosts(VC_TARGET_ID, host(HOST_2_ID).withMemThreshold(90.0).withCpuThreshold(67.0));

        // The hosts should be put in separate setting policies because they belong to different targets.
        final DiscoveredSettingPolicyInfo.Builder settingPolicy1 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, 90.0, 67.0).toBuilder();
        final InterpretedGroup group1 = setupHostGroup(settingPolicy1, hosts.subList(0, 1));
        final DiscoveredSettingPolicyInfo settingPolicy2 =
            memAndCpuSettingPolicy(VC_TARGET_ID, 90.0, 67.0,
                GROUP_COMPONENT_COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        // Since discovered setting policies are associated on a per-target basis, there should be one call
        // per-target.
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VMM_TARGET_ID),
            eq(Collections.singletonList(group1)),
            eq(Collections.singletonList(settingPolicy1.build())));
        verify(groupUploader).addDiscoveredGroupsAndPolicies(eq(VC_TARGET_ID),
            eq(Collections.emptyList()),
            eq(Collections.singletonList(settingPolicy2)));
    }

    private long givenTarget(@Nonnull final SDKProbeType probeType, final long targetId) {
        final long probeId = IdentityGenerator.next();
        final Target target = mock(Target.class);
        when(target.getId()).thenReturn(targetId);

        when(probeStore.getProbeIdForType(probeType.getProbeType())).thenReturn(Optional.of(probeId));
        when(targetStore.getProbeTargets(probeId)).thenReturn(Collections.singletonList(target));
        when(targetStore.getTargetAddress(targetId)).thenReturn(Optional.of(probeType.getProbeType()));

        return targetId;
    }

    private void givenComputeCluster(final long targetId,
                                     @Nonnull final String clusterName, final Long... clusterMembers) {
        final DiscoveredGroupInfo computeCluster = DiscoveredGroupInfo.newBuilder()
            .setInterpretedCluster(ClusterInfo.newBuilder()
                .setName(clusterName)
                .setClusterType(Type.COMPUTE)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addAllStaticMemberOids(Arrays.asList(clusterMembers))))
            .build();

        when(groupUploader.getDiscoveredGroupInfoByTarget()).thenReturn(
            ImmutableMap.of(targetId, Collections.singletonList(computeCluster)));
    }

    private void givenHosts(final long targetId, HostBuilder... hostBuilders) {
        final List<TopologyStitchingEntity> hostsToAdd = Stream.of(hostBuilders)
            .map(hostBuilder -> hostBuilder.build(targetId))
            .collect(Collectors.toList());
        hosts.addAll(hostsToAdd);

        when(stitchingContext.internalEntities(EntityType.PHYSICAL_MACHINE, targetId))
            .thenReturn(hostsToAdd.stream());
    }

    private DiscoveredSettingPolicyInfo memSettingPolicy(final long targetId,
                                                         final double expectedValue,
                                                         @Nonnull String... groupNames) {
        return settingPolicy(groupNames)
            .setName(composeSettingPolicyName(targetId, Optional.of(expectedValue), Optional.empty()))
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.MemoryUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue((float) expectedValue)))
            .build();
    }

    private DiscoveredSettingPolicyInfo cpuSettingPolicy(final long targetId,
                                                         final double expectedValue,
                                                         @Nonnull String... groupNames) {
        return settingPolicy(groupNames)
            .setName(composeSettingPolicyName(targetId, Optional.empty(), Optional.of(expectedValue)))
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.CpuUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue((float) expectedValue)))
            .build();
    }

    private DiscoveredSettingPolicyInfo memAndCpuSettingPolicy(final long targetId,
                                                               final double expectedMemValue,
                                                               final double expectedCpuValue,
                                                               @Nonnull String... groupNames) {
        final String settingPolicyName = composeSettingPolicyName(targetId,
            Optional.of(expectedMemValue), Optional.of(expectedCpuValue));

        return settingPolicy(groupNames)
            .setName(settingPolicyName)
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.MemoryUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue((float) expectedMemValue)))
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.CpuUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue((float) expectedCpuValue)))
            .build();
    }

    private String composeSettingPolicyName(final long targetId,
                                            final Optional<Double> expectedMemValue,
                                            final Optional<Double> expectedCpuValue) {
        String name = expectedMemValue.map(value -> "memUtilization-" + value).orElse("");
        name += (name.isEmpty() ? "" : "-") +
            expectedCpuValue.map(value -> "cpuUtilization-" + value).orElse("");
        name += "/" + targetStore.getTargetAddress(targetId).get();

        return name;
    }

    private static DiscoveredSettingPolicyInfo.Builder settingPolicy(@Nonnull String... groupNames) {
        return DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addAllDiscoveredGroupNames(Arrays.asList(groupNames));
    }

    private InterpretedGroup setupHostGroup(@Nonnull final DiscoveredSettingPolicyInfo.Builder settingPolicy,
                                            @Nonnull final List<TopologyStitchingEntity> hosts) {
        final String groupName = settingPolicy.getName() + "-group";
        settingPolicy.addDiscoveredGroupNames(groupName);

        return interpretedGroupFor(hosts, groupName);
    }

    @Nonnull
    private InterpretedGroup interpretedGroupFor(@Nonnull final List<TopologyStitchingEntity> hosts,
                                                 @Nonnull final String groupName) {
        final CommonDTO.GroupDTO groupDTO = CommonDTO.GroupDTO.newBuilder()
            .setDisplayName(groupName)
            .setGroupName(groupName)
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setMemberList(MembersList.newBuilder()
                    .addAllMember(hosts.stream()
                        .map(TopologyStitchingEntity::getOid)
                        .map(Object::toString)
                        .collect(Collectors.toList()))
            ).build();

        final GroupInfo groupInfo = GroupInfo.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setName(groupName)
            .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                .addAllStaticMemberOids(hosts.stream()
                    .map(TopologyStitchingEntity::getOid)
                    .collect(Collectors.toList())))
            .build();

        return new InterpretedGroup(groupDTO, Optional.of(groupInfo), Optional.empty());
    }

    /**
     * A helper class that permits easy setup of a host {@link TopologyStitchingEntity} with easy setters for
     * for {@link #withMemThreshold(double)} and {@link #withCpuThreshold(double)}.
     */
    private static class HostBuilder {
        private final long oid;
        private final PhysicalMachineBuilder pmBuilder;

        private HostBuilder(final long oid) {
            this.oid = oid;
            this.pmBuilder = physicalMachine("pm");
        }

        public HostBuilder withMemThreshold(final double memUtilizationThreshold) {
            pmBuilder.selling(memKB().sold().utilizationThresholdPct(memUtilizationThreshold));

            return this;
        }

        public HostBuilder withCpuThreshold(final double cpuUtilizationThreshold) {
            pmBuilder.selling(cpuMHz().sold().utilizationThresholdPct(cpuUtilizationThreshold));

            return this;
        }

        public TopologyStitchingEntity build(final long targetId) {
            final TopologyStitchingEntity entity = new TopologyStitchingEntity(
                pmBuilder.build().toBuilder(), oid, targetId, 0);
            entity.getEntityBuilder().getCommoditiesSoldBuilderList().stream().forEach(commoditySold ->
                entity.addCommoditySold(commoditySold, Optional.empty()));

            return entity;
        }
    }

    /**
     * Create a new {@link HostBuilder} for a host with the given oid.
     *
     * @param oid The oid for the host.
     * @return a new {@link HostBuilder} for a host with the given oid.
     */
    @Nonnull
    private static HostBuilder host(final long oid) {
        return new HostBuilder(oid);
    }
}