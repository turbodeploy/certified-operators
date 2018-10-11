package com.vmturbo.topology.processor.stitching.prestitching;

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
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.PostStitchingOperationScopeFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyGraph;

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
    private final TopologyEntity.Builder pm = topologyEntityBuilder(4L, EntityType.PHYSICAL_MACHINE,
        discoveredBy(1L).withMergeFromTargetIds(4L).lastUpdatedAt(11L), Collections.emptyList());
    private final TopologyEntity.Builder vm4 = topologyEntityBuilder(5L, EntityType.VIRTUAL_MACHINE,
            discoveredBy(4L).lastUpdatedAt(44L), Collections.emptyList());

    private final Target target1 = mock(Target.class);
    private final Target target2 = mock(Target.class);
    private final Target target3 = mock(Target.class);
    private final Target target4 = mock(Target.class);

    private TopologyGraph topologyGraph = topologyGraphOf(vm1, vm2, vm3, vm4, pm);

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

        when(target1.getId()).thenReturn(1L);
        when(target2.getId()).thenReturn(2L);
        when(target3.getId()).thenReturn(3L);
        when(target4.getId()).thenReturn(4L);

        when(targetStore.getProbeTargets(eq(111L))).thenReturn(Arrays.asList(target1, target3));
        when(targetStore.getProbeTargets(eq(222L))).thenReturn(Collections.singletonList(target2));
        when(targetStore.getProbeTargets(eq(444L))).thenReturn(Collections.singletonList(target4));
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
}