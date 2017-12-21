package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.stitching.DiscoveryInformation.discoveredBy;
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

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.PostStitchingOperationScopeFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class PostStitchingOperationScopeFactoryTest {

    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final TargetStore targetStore = mock(TargetStore.class);
    private PostStitchingOperationScopeFactory scopeFactory;

    private final TopologyEntity.Builder vm1 = topologyEntityBuilder(1L, EntityType.VIRTUAL_MACHINE,
        Collections.emptyList())
        .discoveryInformation(discoveredBy(1L).lastUpdatedAt(11L));
    private final TopologyEntity.Builder vm2 = topologyEntityBuilder(2L, EntityType.VIRTUAL_MACHINE,
        Collections.emptyList())
        .discoveryInformation(discoveredBy(2L).lastUpdatedAt(22L));
    private final TopologyEntity.Builder vm3 = topologyEntityBuilder(3L, EntityType.VIRTUAL_MACHINE,
        Collections.emptyList())
        .discoveryInformation(discoveredBy(3L).lastUpdatedAt(33L));
    private final TopologyEntity.Builder pm = topologyEntityBuilder(4L, EntityType.PHYSICAL_MACHINE,
        Collections.emptyList())
        .discoveryInformation(discoveredBy(1L).lastUpdatedAt(11L));

    private final Target target1 = mock(Target.class);
    private final Target target2 = mock(Target.class);
    private final Target target3 = mock(Target.class);

    private TopologyGraph topologyGraph = topologyGraphOf(vm1, vm2, vm3, pm);

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setup() {
        scopeFactory = new PostStitchingOperationScopeFactory(topologyGraph,
            probeStore, targetStore);

        when(probeStore.getProbeIdForType("111")).thenReturn(Optional.of(111L));
        when(probeStore.getProbeIdForType("222")).thenReturn(Optional.of(222L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR)).thenReturn(
            Arrays.asList(111L, 222L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.STORAGE)).thenReturn(Collections.emptyList());

        when(target1.getId()).thenReturn(1L);
        when(target2.getId()).thenReturn(2L);
        when(target3.getId()).thenReturn(3L);

        when(targetStore.getProbeTargets(eq(111L))).thenReturn(Arrays.asList(target1, target3));
        when(targetStore.getProbeTargets(eq(222L))).thenReturn(Collections.singletonList(target2));
    }

    @Test
    public void testGlobalScope() throws Exception {
        assertThat(scopeFactory.globalScope().entities()
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
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
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L, 3L));
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
}