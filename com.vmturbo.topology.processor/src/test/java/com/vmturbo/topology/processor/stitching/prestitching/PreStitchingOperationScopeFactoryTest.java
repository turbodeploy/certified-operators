package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.topologyMapOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.PreStitchingOperationScopeFactory;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingContext.Builder;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class PreStitchingOperationScopeFactoryTest {

    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final TargetStore targetStore = mock(TargetStore.class);
    private PreStitchingOperationScopeFactory calculationScopeFactory;

    private final StitchingEntityData vm1 = stitchingData("1", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(1L);
    private final StitchingEntityData vm2 = stitchingData("2", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(2L);
    private final StitchingEntityData vm3 = stitchingData("3", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(3L);
    private final StitchingEntityData pm = stitchingData("4", EntityType.PHYSICAL_MACHINE,
        Collections.emptyList()).forTarget(1L);
    private final Map<String, StitchingEntityData> targetGraph = topologyMapOf(vm1, vm2, pm);

    private final Target target1 = mock(Target.class);
    private final Target target2 = mock(Target.class);
    private final Target target3 = mock(Target.class);

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setup() {
        final Builder contextBuilder = StitchingContext.newBuilder(3);
        contextBuilder.addEntity(vm1, targetGraph);
        contextBuilder.addEntity(vm2, targetGraph);
        contextBuilder.addEntity(vm3, targetGraph);
        contextBuilder.addEntity(pm, targetGraph);

        calculationScopeFactory = new PreStitchingOperationScopeFactory(contextBuilder.build(),
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
        assertThat(calculationScopeFactory.globalScope().entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "4"));
    }

    @Test
    public void testProbeScope() throws Exception {
        assertThat(calculationScopeFactory.probeScope("111").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "3", "4"));
        assertThat(calculationScopeFactory.probeScope("222").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("2"));
    }

    @Test
    public void testUnregisteredProbeScope() throws Exception {
        when(probeStore.getProbeIdForType("333")).thenReturn(Optional.empty());

        assertThat(calculationScopeFactory.probeScope("333").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testEntityTypeScope() throws Exception {
        assertThat(calculationScopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3"));
        assertThat(calculationScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
        assertThat(calculationScopeFactory.entityTypeScope(EntityType.NETWORK).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testProbeEntityTypeScope() throws Exception {
        assertThat(calculationScopeFactory.probeEntityTypeScope("111", EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "3"));
        assertThat(calculationScopeFactory.probeEntityTypeScope("111", EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
        assertThat(calculationScopeFactory.probeEntityTypeScope("222", EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testProbeCategoryEntityTypeScope() throws Exception {
        assertThat(calculationScopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3"));
        assertThat(calculationScopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.STORAGE, EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
        assertThat(calculationScopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
    }
}