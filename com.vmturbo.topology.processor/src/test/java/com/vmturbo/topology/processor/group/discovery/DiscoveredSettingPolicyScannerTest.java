package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.memKB;
import static com.vmturbo.platform.common.builders.CommodityBuilders.space;
import static com.vmturbo.platform.common.builders.EntityBuilders.datacenter;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.builders.PhysicalMachineBuilder;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader.TargetDiscoveredData;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.GroupTestUtils;

/**
 * Unit tests for {@link DiscoveredSettingPolicyScanner}.
 */
public class DiscoveredSettingPolicyScannerTest {

    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private final DiscoveredGroupUploader groupUploader = mock(DiscoveredGroupUploader.class);

    private static final StitchingContext stitchingContext = mock(StitchingContext.class);

    private final DiscoveredSettingPolicyScanner scanner =
                    new DiscoveredSettingPolicyScanner(probeStore, targetStore);

    @Captor
    private ArgumentCaptor<Collection<InterpretedGroup>> groupsCaptor;

    @Captor
    private ArgumentCaptor<Collection<DiscoveredSettingPolicyInfo>> settingPolicyCaptor;

    private final List<TopologyStitchingEntity> hosts = new ArrayList<>();

    private static final long VC_TARGET_ID = 782634L;
    private static final long VMM_TARGET_ID = 9891371236L;

    private static final long HOST_1_ID = 1001L;
    private static final long HOST_2_ID = 1002L;
    private static final long HOST_3_ID = 1003L;

    private static final double MEM_THRESHOLD = 91.0;
    private static final double CPU_THRESHOLD = 39.0;
    private static final double MEM_THRESHOLD_2 = 88.0;
    private static final double CPU_THRESHOLD_2 = 70.0;

    private static final String COMPUTE_CLUSTER_NAME = "SG-123456";
    private static final String DC_DISPLAY_NAME = "DC17";
    private static final String CLUSTER_DISPLAY_NAME = "Test Cluster";

    private static final String DC_ID = "dc-id";
    private static final TopologyStitchingEntity DC_ENTITY = new TopologyStitchingEntity(
        datacenter(DC_ID).displayName(DC_DISPLAY_NAME).build().toBuilder(),
        2001L,
        VC_TARGET_ID,
        0);

    static {
        when(stitchingContext.getEntitiesByEntityTypeAndTarget())
            .thenReturn(ImmutableMap.of(EntityType.PHYSICAL_MACHINE, ImmutableMap.of(
                VC_TARGET_ID, Lists.newArrayList(host(HOST_1_ID).build(VC_TARGET_ID)))));
    }

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        MockitoAnnotations.initMocks(this);

        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType()))
            .thenReturn(Optional.empty());
        when(probeStore.getProbeIdForType(SDKProbeType.VMM.getProbeType()))
            .thenReturn(Optional.empty());
    }

    @Test
    public void testNoThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID));

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader, never()).setScannedGroupsAndPolicies(anyLong(),
            anyListOf(InterpretedGroup.class),
            anyListOf(DiscoveredSettingPolicyInfo.class));
    }

    @Test
    public void testDefaultThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID)
            .withMemThreshold(DiscoveredSettingPolicyScanner.DEFAULT_UTILIZATION_THRESHOLD));

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader, never()).setScannedGroupsAndPolicies(anyLong(),
            anyListOf(InterpretedGroup.class),
            anyListOf(DiscoveredSettingPolicyInfo.class));
    }

    @Test
    public void testVcMemThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD_2));
        final DiscoveredSettingPolicyInfo settingPolicy =
                        memSettingPolicy(VC_TARGET_ID, MEM_THRESHOLD_2, COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy)));
    }

    @Test
    public void testVcCpuThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID).withCpuThreshold(CPU_THRESHOLD));
        final DiscoveredSettingPolicyInfo settingPolicy =
                        cpuSettingPolicy(VC_TARGET_ID, CPU_THRESHOLD, COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy)));
    }

    @Test
    public void testVcMemAndCpuThreshold() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VC_TARGET_ID, host(HOST_1_ID)
            .withMemThreshold(MEM_THRESHOLD)
            .withCpuThreshold(CPU_THRESHOLD));
        final DiscoveredSettingPolicyInfo settingPolicy = memAndCpuSettingPolicy(
            VC_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD, COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy)));
    }

    @Test
    public void testVcMultipleSameThresholds() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID, HOST_2_ID);
        givenHosts(VC_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD),
            host(HOST_2_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD));
        final DiscoveredSettingPolicyInfo settingPolicy = memAndCpuSettingPolicy(
            VC_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD, COMPUTE_CLUSTER_NAME);

        // Even though we have added two hosts with mem and cpu thresholds, we should only create one setting
        // policy because the hosts have the same value.
        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy)));
    }

    @Test
    public void testVcMultipleDifferentThresholds() {
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID, HOST_2_ID);
        givenHosts(VC_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD),
            host(HOST_2_ID).withMemThreshold(MEM_THRESHOLD_2).withCpuThreshold(CPU_THRESHOLD_2));
        final DiscoveredSettingPolicyInfo firstSettingPolicy = memAndCpuSettingPolicy(
            VC_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD, COMPUTE_CLUSTER_NAME);
        final DiscoveredSettingPolicyInfo secondSettingPolicy = memAndCpuSettingPolicy(
            VC_TARGET_ID, MEM_THRESHOLD_2, CPU_THRESHOLD_2, COMPUTE_CLUSTER_NAME).toBuilder()
                // Because it's a duplicate policy, we expect a suffix to be added to the name.
                .setName(firstSettingPolicy.getName() + "-1")
                .build();

        // This should create two conflicting settings policies on the same cluster. We'll pick
        //  the more conservative values per conflict resolution when actually applying settings.
        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(containsInAnyOrder(firstSettingPolicy, secondSettingPolicy)));
    }

    @Test
    public void testSingleVmmHost() {
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD));

        final DiscoveredSettingPolicyInfo.Builder settingPolicy =
            memAndCpuSettingPolicy(VMM_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD).toBuilder();
        final InterpretedGroup group = setupHostGroup(settingPolicy, hosts);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VMM_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(containsInAnyOrder(group)),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(containsInAnyOrder(settingPolicy.build())));
    }

    @Test
    public void testMultipleVmmHostsSameThresholds() {
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD),
            host(HOST_2_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD));

        final DiscoveredSettingPolicyInfo.Builder settingPolicy =
            memAndCpuSettingPolicy(VMM_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD).toBuilder();
        final InterpretedGroup group = setupHostGroup(settingPolicy, hosts);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VMM_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(contains(group)),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy.build())));
    }

    @Test
    public void testMultipleVmmHostsDifferentThresholds() {
        // Should create two discovered groups and two thresholds
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenComputeCluster(VMM_TARGET_ID, HOST_1_ID);
        givenHosts(VMM_TARGET_ID,
            host(HOST_1_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD),
            host(HOST_2_ID).withMemThreshold(MEM_THRESHOLD).withCpuThreshold(CPU_THRESHOLD),
            host(HOST_3_ID).withMemThreshold(MEM_THRESHOLD_2).withCpuThreshold(CPU_THRESHOLD));

        // The first two hosts should be put in a single group because they share the
        // same mem and CPU thresholds.
        final DiscoveredSettingPolicyInfo.Builder settingPolicy1 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD).toBuilder();
        final InterpretedGroup group1 = setupHostGroup(settingPolicy1, hosts.subList(0, 2));
        // The last host should be put in its own group because it has different thresholds.
        List<TopologyStitchingEntity> twoHosts = hosts.subList(2, 3);
        long host2Oid = twoHosts.stream().map(TopologyStitchingEntity::getOid).min(Long::compare).get();
        final DiscoveredSettingPolicyInfo.Builder settingPolicy2 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, host2Oid, MEM_THRESHOLD_2, CPU_THRESHOLD)
                .toBuilder();
        final InterpretedGroup group2 = setupHostGroup(settingPolicy2, twoHosts);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VMM_TARGET_ID),
            groupsCaptor.capture(),
            settingPolicyCaptor.capture());

        assertThat(new HashSet<>(groupsCaptor.getValue()), containsInAnyOrder(group1, group2));
        assertThat(settingPolicyCaptor.getValue(),
            containsInAnyOrder(settingPolicy1.build(), settingPolicy2.build()));
    }

    @Test
    public void testVcAndVmmTogether() {
        // Should create two discovered groups and two thresholds
        givenTarget(SDKProbeType.VMM, VMM_TARGET_ID);
        givenTarget(SDKProbeType.VCENTER, VC_TARGET_ID);
        givenComputeCluster(VC_TARGET_ID, HOST_1_ID);
        givenHosts(VMM_TARGET_ID, host(HOST_2_ID)
            .withMemThreshold(MEM_THRESHOLD)
            .withCpuThreshold(CPU_THRESHOLD));
        givenHosts(VC_TARGET_ID, host(HOST_1_ID)
            .withMemThreshold(MEM_THRESHOLD)
            .withCpuThreshold(CPU_THRESHOLD));

        // The hosts should be put in separate setting policies because they belong to different targets.
        final DiscoveredSettingPolicyInfo.Builder settingPolicy1 =
            memAndCpuSettingPolicy(VMM_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD).toBuilder();
        final InterpretedGroup group1 = setupHostGroup(settingPolicy1, hosts.subList(0, 1));
        final DiscoveredSettingPolicyInfo settingPolicy2 =
            memAndCpuSettingPolicy(VC_TARGET_ID, MEM_THRESHOLD, CPU_THRESHOLD,
                COMPUTE_CLUSTER_NAME);

        scanner.scanForDiscoveredSettingPolicies(stitchingContext, groupUploader);
        // Since discovered setting policies are associated on a per-target basis, there
        // should be one call per-target.
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VMM_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(contains(group1)),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy1.build())));
        verify(groupUploader).setScannedGroupsAndPolicies(eq(VC_TARGET_ID),
            (Collection<InterpretedGroup>)Matchers.argThat(empty()),
            (Collection<DiscoveredSettingPolicyInfo>)Matchers.argThat(contains(settingPolicy2)));
    }

    private long givenTarget(@Nonnull final SDKProbeType probeType, final long targetId) {
        final long probeId = IdentityGenerator.next();
        final Target target = mock(Target.class);
        when(target.getId()).thenReturn(targetId);

        when(probeStore.getProbeIdForType(probeType.getProbeType()))
            .thenReturn(Optional.of(probeId));
        when(targetStore.getProbeTargets(probeId))
            .thenReturn(Collections.singletonList(target));
        when(targetStore.getTargetDisplayName(targetId))
            .thenReturn(Optional.of(probeType.getProbeType()));

        return targetId;
    }

    private void givenComputeCluster(final long targetId, final Long... clusterMembers) {
        final DiscoveredGroupInfo computeCluster = DiscoveredGroupInfo.newBuilder()
            .setUploadedGroup(GroupTestUtils.createUploadedCluster(COMPUTE_CLUSTER_NAME,
                    CLUSTER_DISPLAY_NAME, GroupType.COMPUTE_HOST_CLUSTER,
                    Arrays.asList(clusterMembers)))
            .build();
        InterpretedGroup mockCluster = mock(InterpretedGroup.class);
        when(mockCluster.createDiscoveredGroupInfo()).thenReturn(computeCluster);
        TargetDiscoveredData targetData = mock(TargetDiscoveredData.class);
        when(targetData.getGroups()).thenReturn(Stream.of(mockCluster));
        when(groupUploader.getDataByTarget()).thenReturn(
            ImmutableMap.of(targetId, targetData));
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
                    final double expectedMemValue, @Nonnull String... groupNames) {
        return memAndCpuSettingPolicy(targetId, expectedMemValue, -1, groupNames);
    }

    private DiscoveredSettingPolicyInfo cpuSettingPolicy(final long targetId,
                    final double expectedCpuValue, @Nonnull String... groupNames) {
        return memAndCpuSettingPolicy(targetId, -1, expectedCpuValue, groupNames);
    }

    private DiscoveredSettingPolicyInfo memAndCpuSettingPolicy(final long targetId,
                    final double expectedMemValue, final double expectedCpuValue,
                    @Nonnull String... groupNames) {
        return memAndCpuSettingPolicy(targetId, HOST_1_ID, expectedMemValue, expectedCpuValue, groupNames);
    }

    private DiscoveredSettingPolicyInfo memAndCpuSettingPolicy(final long targetId,
                    final long hostOid,
                    final double expectedMemValue, final double expectedCpuValue,
                    @Nonnull String... groupNames) {
        String baseName = groupNames.length == 0
                    ? composeSettingPolicyName(
                        targetId, Optional.of(expectedMemValue), Optional.of(expectedCpuValue))
                    : groupNames[0];
        DiscoveredSettingPolicyInfo.Builder policyBuilder = settingPolicy(groupNames)
            .setName("HA-" + baseName)
            .setDisplayName(policyDisplayName(targetId, hostOid));
        if (expectedMemValue >= 0) {
            policyBuilder.addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.MemoryUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue((float)expectedMemValue)));
        }
        if (expectedCpuValue > 0) {
            policyBuilder.addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.CpuUtilization.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue((float)expectedCpuValue)));
        }
        return policyBuilder.build();
    }

    private String policyDisplayName(long targetId, long hostId) {
        String clusterDisplayNames = targetId == VMM_TARGET_ID
            ? String.valueOf(hostId) : (DC_DISPLAY_NAME + "/" + CLUSTER_DISPLAY_NAME);
        return String.format(DiscoveredSettingPolicyScanner.IMPORTED_HA_SETTINGS_DISPLAY_NAME,
            clusterDisplayNames, targetStore.getTargetDisplayName(targetId).get());
    }

    private static DiscoveredSettingPolicyInfo.Builder settingPolicy(@Nonnull String... groupNames) {
        return DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addAllDiscoveredGroupNames(Arrays.stream(groupNames)
                    .map(name -> GroupProtoUtil.createIdentifyingKey(
                            GroupType.COMPUTE_HOST_CLUSTER, name))
                    .collect(Collectors.toList()));
    }

    private InterpretedGroup setupHostGroup(
                    @Nonnull final DiscoveredSettingPolicyInfo.Builder settingPolicy,
                    @Nonnull final List<TopologyStitchingEntity> hosts) {
        // This method is used only for VMM
        Optional<Double> mem = getThreshold(settingPolicy, "mem");
        Optional<Double> cpu = getThreshold(settingPolicy, "cpu");

        final String groupName = "GROUP-" + composeSettingPolicyName(VMM_TARGET_ID, mem, cpu);
        final String groupDisplayName = String.format(
            "PMs with Mem threshold %s and CPU threshold %s on VMM", mem.get(), cpu.get());
        settingPolicy.addDiscoveredGroupNames(GroupProtoUtil.createIdentifyingKey(
                GroupType.REGULAR, groupName));
        settingPolicy.setDisplayName("HA Settings for " + groupDisplayName);

        return interpretedGroupFor(hosts, groupName, groupDisplayName);
    }

    Optional<Double> getThreshold(DiscoveredSettingPolicyInfo.Builder settingPolicy, String type) {
        return settingPolicy.getSettingsList().stream()
                        .filter(setting -> setting.getSettingSpecName().contains(type))
                        .findAny()
                        .map(setting -> (double)setting.getNumericSettingValue().getValue());
    }

    private String composeSettingPolicyName(final long targetId,
                    final Optional<Double> expectedMemValue,
                    final Optional<Double> expectedCpuValue) {
        String name = expectedMemValue.map(value -> "mem-" + value).orElse("");
        name += (name.isEmpty() ? "" : "-") +
                        expectedCpuValue.map(value -> "cpu-" + value).orElse("");
        name += "/" + targetStore.getTargetDisplayName(targetId).get();

        return name;
    }

    @Nonnull
    private InterpretedGroup interpretedGroupFor(
                    @Nonnull final List<TopologyStitchingEntity> hosts,
                    @Nonnull final String groupName,
                    @Nonnull final String groupDisplayName) {
        final CommonDTO.GroupDTO groupDTO = CommonDTO.GroupDTO.newBuilder()
            .setGroupType(GroupType.REGULAR)
            .setDisplayName(groupDisplayName)
            .setGroupName(groupName)
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setMemberList(MembersList.newBuilder()
                    .addAllMember(hosts.stream()
                        .map(TopologyStitchingEntity::getOid)
                        .map(Object::toString)
                        .collect(Collectors.toList()))
            ).build();

        final GroupDefinition.Builder groupDef = GroupTestUtils.createStaticGroupDef(
                groupDisplayName, EntityType.PHYSICAL_MACHINE_VALUE,
                hosts.stream()
                        .map(TopologyStitchingEntity::getOid)
                        .collect(Collectors.toList()));

        return new InterpretedGroup(groupDTO, Optional.of(groupDef));
    }

    /**
     * A helper class that permits easy setup of a host {@link TopologyStitchingEntity} with easy
     * setters for for {@link #withMemThreshold(double)} and {@link #withCpuThreshold(double)}.
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
            entity.getEntityBuilder().getCommoditiesSoldBuilderList().stream().forEach(
                commoditySold -> entity.addCommoditySold(commoditySold, Optional.empty()));
            // VC hosts buy something from DC, to test that DC display name is added to
            // the policy display name
            if (targetId == VC_TARGET_ID) {
                entity.addProviderCommodityBought(DC_ENTITY,
                    new CommoditiesBought(Collections.singletonList(space().build().toBuilder())));
            }
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