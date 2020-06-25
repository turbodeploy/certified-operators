package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.stitching.DiscoveryOriginBuilder.discoveredBy;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyGraphOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class PostStitchingOperationScopeFactoryTest {

    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final TargetStore targetStore = mock(TargetStore.class);
    private CpuCapacityStore cpuCapacityStore = mock(CpuCapacityStore.class);
    private PostStitchingOperationScopeFactory scopeFactory;

    private final TopologyEntity.Builder vm1 = topologyEntityBuilder(1L, EntityType.VIRTUAL_MACHINE,
        discoveredBy(1L).lastUpdatedAt(11L), Collections.emptyList());
    private final TopologyEntity.Builder vm2 = topologyEntityBuilder(2L, EntityType.VIRTUAL_MACHINE,
        discoveredBy(2L).lastUpdatedAt(22L), Collections.emptyList());
    private final TopologyEntity.Builder vm3 = topologyEntityBuilder(3L, EntityType.VIRTUAL_MACHINE,
        discoveredBy(3L).lastUpdatedAt(33L), Collections.emptyList());
    private final TopologyEntity.Builder pm =
        topologyEntityBuilder(4L, EntityType.PHYSICAL_MACHINE,
                              discoveredBy(1L, null).withMerge(new StitchingMergeInformation(4L,
                                                                                       4L,
                                                                                       StitchingErrors.none()))
                                              .lastUpdatedAt(11L),
                              Collections.emptyList());
    private final TopologyEntity.Builder vm4 = topologyEntityBuilder(5L, EntityType.VIRTUAL_MACHINE,
            discoveredBy(4L).lastUpdatedAt(44L), Collections.emptyList());

    private final Target target1 = mock(Target.class);
    private final Target target2 = mock(Target.class);
    private final Target target3 = mock(Target.class);
    private final Target target4 = mock(Target.class);
    private final Target target5 = mock(Target.class);

    private TopologyGraph<TopologyEntity> topologyGraph = topologyGraphOf(vm1, vm2, vm3, vm4, pm);

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setup() {
        scopeFactory = new PostStitchingOperationScopeFactory(topologyGraph,
            probeStore, targetStore, cpuCapacityStore);

        when(probeStore.getProbeIdForType("111")).thenReturn(Optional.of(111L));
        when(probeStore.getProbeIdForType("222")).thenReturn(Optional.of(222L));
        when(probeStore.getProbeIdForType("444")).thenReturn(Optional.of(444L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR)).thenReturn(
            Arrays.asList(111L, 222L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.STORAGE)).thenReturn(Collections.emptyList());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERCONVERGED)).thenReturn(
            Collections.singletonList(444L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.GUEST_OS_PROCESSES)).thenReturn(
                Collections.singletonList(555L));

        when(target1.getId()).thenReturn(1L);
        when(target2.getId()).thenReturn(2L);
        when(target3.getId()).thenReturn(3L);
        when(target4.getId()).thenReturn(4L);
        when(target5.getId()).thenReturn(5L);

        when(targetStore.getProbeTargets(eq(111L))).thenReturn(Arrays.asList(target1, target3));
        when(targetStore.getProbeTargets(eq(222L))).thenReturn(Collections.singletonList(target2));
        when(targetStore.getProbeTargets(eq(444L))).thenReturn(Collections.singletonList(target4));
        when(targetStore.getProbeTargets(eq(555L))).thenReturn(Collections.singletonList(target5));
    }

    @Test
    public void testGlobalScope() throws Exception {
        assertThat(scopeFactory.globalScope().entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 4L, 5L));
    }

    @Test
    public void testProbeScope() throws Exception {
        assertThat(scopeFactory.probeScope("111").entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 3L, 4L));
        assertThat(scopeFactory.probeScope("222").entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(2L));
    }

    @Test
    public void testUnregisteredProbeScope() throws Exception {
        when(probeStore.getProbeIdForType("333")).thenReturn(Optional.empty());

        assertThat(scopeFactory.probeScope("333").entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testEntityTypeScope() throws Exception {
        assertThat(scopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 5L));
        assertThat(scopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), contains(4L));
        assertThat(scopeFactory.entityTypeScope(EntityType.NETWORK).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testProbeEntityTypeScope() throws Exception {
        assertThat(scopeFactory.probeEntityTypeScope("111", EntityType.VIRTUAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 3L));
        assertThat(scopeFactory.probeEntityTypeScope("111", EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), contains(4L));
        assertThat(scopeFactory.probeEntityTypeScope("222", EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultiProbeEntityTypeScope() throws Exception {

        // this will contain targets 1,3
        final ImmutableSet<String> singleProbeSet = ImmutableSet.of("111");

        // this will contain targets 1,2,3
        final ImmutableSet<String> multipleProbeSet = ImmutableSet.of("111", "222");

        assertThat(scopeFactory.multiProbeEntityTypeScope(singleProbeSet, EntityType.VIRTUAL_MACHINE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), containsInAnyOrder(1L, 3L));

        assertThat(scopeFactory.multiProbeEntityTypeScope(multipleProbeSet, EntityType.VIRTUAL_MACHINE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));

        assertThat(scopeFactory.multiProbeEntityTypeScope(multipleProbeSet, EntityType.PHYSICAL_MACHINE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), contains(4L));
    }

    @Test
    public void testProbeCategoryEntityTypeScope() throws Exception {
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.VIRTUAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.STORAGE, EntityType.VIRTUAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), is(empty()));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), contains(4L));
    }

    @Test
    public void testHasAndMissProbeCategoryEntityTypeScope() throws Exception {
        StitchingMergeInformation vm5Stitching = new StitchingMergeInformation(5L, 5L, StitchingErrors.none());
        final TopologyEntity.Builder vm5 = topologyEntityBuilder(6L, EntityType.VIRTUAL_MACHINE,
                discoveredBy(1L).withMerge(vm5Stitching).lastUpdatedAt(11L),
                Collections.emptyList());
        final TopologyEntity.Builder vm6 = topologyEntityBuilder(7L, EntityType.VIRTUAL_MACHINE,
                discoveredBy(5L).lastUpdatedAt(66L),
                Collections.emptyList());
        final TopologyGraph<TopologyEntity> topologyGraph =
                topologyGraphOf(vm1, vm2, vm3, vm5, vm6, vm4);
        scopeFactory = new PostStitchingOperationScopeFactory(topologyGraph,
                probeStore, targetStore, cpuCapacityStore);
        Set<Long> vmsHaveHyperviosrMissGuestOs = scopeFactory.hasAndLacksProbeCategoryEntityTypeStitchingScope(
                Collections.singleton(ProbeCategory.HYPERVISOR), Collections.singleton(ProbeCategory.GUEST_OS_PROCESSES), EntityType.VIRTUAL_MACHINE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
        Assert.assertEquals(vmsHaveHyperviosrMissGuestOs, Sets.newHashSet(1L, 2L, 3L));
    }

    @Test
    public void testEntityDiscoveredByMultipleTargets() throws Exception {
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), contains(4L));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERCONVERGED, EntityType.PHYSICAL_MACHINE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), contains(4L));
    }

    /**
     * testProbeCategoryScope.
     */
    @Test
    public void testProbeCategoryScope() {
        assertThat(scopeFactory.probeCategoryScope(
            ProbeCategory.HYPERVISOR).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
        assertThat(scopeFactory.probeCategoryScope(
            ProbeCategory.STORAGE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), is(empty()));
    }

    /**
     * testEntityDiscoveredByMultipleTargetsProbeCategory.
     */
    @Test
    public void testEntityDiscoveredByMultipleTargetsProbeCategory() {
        assertThat(scopeFactory.probeCategoryScope(
            ProbeCategory.HYPERVISOR).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
        assertThat(scopeFactory.probeCategoryScope(
            ProbeCategory.HYPERCONVERGED).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(4L, 5L));
    }

    @Test
    public void testContainsAllEntityTypesScope() {
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE)).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 5L));
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE, EntityType.PHYSICAL_MACHINE)).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 4L, 5L));
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE, EntityType.STORAGE_VOLUME)).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultiProbeCategoryEntityTypeScope() throws Exception {
        assertThat(scopeFactory.multiProbeCategoryEntityTypeScope(
                ImmutableSet.of(ProbeCategory.HYPERVISOR, ProbeCategory.HYPERCONVERGED),
                EntityType.VIRTUAL_MACHINE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 5L));
        assertThat(scopeFactory.multiProbeCategoryEntityTypeScope(
                ImmutableSet.of(ProbeCategory.HYPERVISOR, ProbeCategory.STORAGE),
                EntityType.STORAGE).entities()
                .map(TopologyEntity::getOid)
                .collect(Collectors.toList()), is(empty()));
    }

    /**
     * Test that the missingDerivedTargetEntityTypeScope works by creating a topology with a
     * storage shared by 2 targets and cover the various scenarios: 1 both VC targets have
     * storage browsing derived targets, 2 Exactly one VC target has a derived storage browsing
     * target, and 3 neither VC target has a derived browsing target.
     */
    @Test
    public void testMissingDerivedTargetEntityTypeScope() {
        // For this test, we treat targets 1 and 2 as VC targets with target 3 being the
        // storage browsing target related to target 1.  Target 2 has storage browsing disabled.
        // Then we create Storages with different
        // combinations of targets and make sure we get the correct matches for this scope
        // stor1 has one VC target and its corresponding storage browsing target
        long oid1 = 1L;
        StitchingMergeInformation smi1 = new StitchingMergeInformation(oid1, 3L, StitchingErrors.none());
        final TopologyEntity.Builder stor1 = topologyEntityBuilder(oid1, EntityType.STORAGE,
            discoveredBy(1L).withMerge(smi1).lastUpdatedAt(11L),
            Collections.emptyList());
        // Shared Storage discovered by 2 VCs but only one storage browsing target
        long oid2 = 2L;
        StitchingMergeInformation smi2 = new StitchingMergeInformation(oid2, 2L, StitchingErrors.none());
        StitchingMergeInformation smi3 = new StitchingMergeInformation(oid2, 3L, StitchingErrors.none());
        final TopologyEntity.Builder stor2 = topologyEntityBuilder(oid2, EntityType.STORAGE,
            discoveredBy(1L).withMerge(Arrays.asList(smi2, smi3))
                .lastUpdatedAt(33L), Collections.emptyList());
        // Storage discovered by VC target 2 only
        final TopologyEntity.Builder stor3 = topologyEntityBuilder(3L, EntityType.STORAGE,
            discoveredBy(2L).lastUpdatedAt(44L), Collections.emptyList());
        // VM discovered by VC target 2 only - make sure our scope only includes Storages
        final TopologyEntity.Builder vm1 = topologyEntityBuilder(4L, EntityType.VIRTUAL_MACHINE,
            discoveredBy(2L).lastUpdatedAt(44L), Collections.emptyList());
        final ProbeInfo vcProbeInfo = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(SDKProbeType.VCENTER.getProbeType())
            .build();
        final ProbeInfo vcStorageBrowseProbeInfo = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.STORAGE_BROWSING.getCategory())
            .setUiProbeCategory(ProbeCategory.STORAGE_BROWSING.getCategory())
            .setProbeType(SDKProbeType.VC_STORAGE_BROWSE.getProbeType())
            .build();
        when(target1.getProbeInfo()).thenReturn(vcProbeInfo);
        when(target2.getProbeInfo()).thenReturn(vcProbeInfo);
        when(target3.getProbeInfo()).thenReturn(vcStorageBrowseProbeInfo);
        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType()))
            .thenReturn(Optional.of(111L));
        when(probeStore.getProbeIdForType(SDKProbeType.VC_STORAGE_BROWSE.getProbeType()))
            .thenReturn(Optional.of(222L));
        when(targetStore.getProbeTargets(eq(111L)))
            .thenReturn(Arrays.asList(target1, target2));
        when(targetStore.getProbeTargets(eq(222L)))
            .thenReturn(Arrays.asList(target3));
        when(targetStore.getDerivedTargetIds(1L))
            .thenReturn(Sets.newHashSet(3L));
        when(targetStore.getDerivedTargetIds(2L))
            .thenReturn(Sets.newHashSet());
        when(targetStore.getProbeTypeForTarget(1L))
            .thenReturn(Optional.of(SDKProbeType.VCENTER));
        when(targetStore.getProbeTypeForTarget(2L))
            .thenReturn(Optional.of(SDKProbeType.VCENTER));
        when(targetStore.getProbeTypeForTarget(3L))
            .thenReturn(Optional.of(SDKProbeType.VC_STORAGE_BROWSE));
        when(targetStore.getTarget(3L)).thenReturn(Optional.of(target3));
        final TopologyGraph<TopologyEntity> topologyGraph =
            topologyGraphOf(stor1, stor2, stor3, vm1);
        scopeFactory = new PostStitchingOperationScopeFactory(topologyGraph,
            probeStore, targetStore, cpuCapacityStore);
        List<Long> entitiesInScope = scopeFactory
            .missingDerivedTargetEntityTypeScope(SDKProbeType.VCENTER.getProbeType(),
            SDKProbeType.VC_STORAGE_BROWSE.getProbeType(), EntityType.STORAGE).entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList());
        Assert.assertEquals(2, entitiesInScope.size());
        assertThat(entitiesInScope, containsInAnyOrder(stor2.getOid(), stor3.getOid()));
    }
}
