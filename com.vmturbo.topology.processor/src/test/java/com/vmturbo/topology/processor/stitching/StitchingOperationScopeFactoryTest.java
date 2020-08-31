package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.topologyMapOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext.Builder;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class StitchingOperationScopeFactoryTest {

    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final TargetStore targetStore = mock(TargetStore.class);
    private StitchingOperationScopeFactory scopeFactory;

    private final StitchingEntityData vm1 = stitchingData("1", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(1L);
    private final StitchingEntityData vm2 = stitchingData("2", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(2L);
    private final StitchingEntityData vm3 = stitchingData("3", EntityType.VIRTUAL_MACHINE,
        Collections.emptyList()).forTarget(3L);
    private final StitchingEntityData pm = stitchingData("4", EntityType.PHYSICAL_MACHINE,
        Collections.emptyList()).forTarget(1L);
    private final StitchingEntityData vm4 = stitchingData("5", EntityType.VIRTUAL_MACHINE,
            Collections.emptyList()).forTarget(4L);
    private final Map<String, StitchingEntityData> targetGraph = topologyMapOf(vm1, vm2, pm);

    private final Target target1 = mock(Target.class);
    private final Target target2 = mock(Target.class);
    private final Target target3 = mock(Target.class);
    private final Target target4 = mock(Target.class);

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setup() {
        final TargetStore targetStore = mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final Builder contextBuilder = StitchingContext.newBuilder(5, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class));
        contextBuilder.addEntity(vm1, targetGraph);
        contextBuilder.addEntity(vm2, targetGraph);
        contextBuilder.addEntity(vm3, targetGraph);
        contextBuilder.addEntity(pm, targetGraph);
        contextBuilder.addEntity(vm4, targetGraph);

        scopeFactory = new StitchingOperationScopeFactory(contextBuilder.build(),
            probeStore, targetStore);

        when(probeStore.getProbeIdForType("111")).thenReturn(Optional.of(111L));
        when(probeStore.getProbeIdForType("222")).thenReturn(Optional.of(222L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR)).thenReturn(
            Arrays.asList(111L, 222L));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.STORAGE)).thenReturn(Collections.emptyList());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERCONVERGED)).thenReturn(
                Arrays.asList(444L));

        when(target1.getId()).thenReturn(1L);
        when(target2.getId()).thenReturn(2L);
        when(target3.getId()).thenReturn(3L);
        when(target4.getId()).thenReturn(4L);

        when(targetStore.getProbeTargets(eq(111L))).thenReturn(Arrays.asList(target1, target3));
        when(targetStore.getProbeTargets(eq(222L))).thenReturn(Collections.singletonList(target2));
        when(targetStore.getProbeTargets(eq(444L))).thenReturn(Collections.singletonList(target4));
        when(targetStore.getParentTargetIds(4L))
                .thenReturn(Collections.singleton(1L));
    }

    @Test
    public void testGlobalScope() throws Exception {
        assertThat(scopeFactory.globalScope().entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "4", "5"));
    }

    @Test
    public void testProbeScope() throws Exception {
        assertThat(scopeFactory.probeScope("111").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "3", "4"));
        assertThat(scopeFactory.probeScope("222").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("2"));
    }

    @Test
    public void testUnregisteredProbeScope() throws Exception {
        when(probeStore.getProbeIdForType("333")).thenReturn(Optional.empty());

        assertThat(scopeFactory.probeScope("333").entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testEntityTypeScope() throws Exception {
        assertThat(scopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "5"));
        assertThat(scopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
        assertThat(scopeFactory.entityTypeScope(EntityType.NETWORK).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testProbeEntityTypeScope() throws Exception {
        assertThat(scopeFactory.probeEntityTypeScope("111", EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("1", "3"));
        assertThat(scopeFactory.probeEntityTypeScope("111", EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
        assertThat(scopeFactory.probeEntityTypeScope("222", EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultiProbeEntityTypeScope() throws Exception {

        // this will contain targets 1,3
        final ImmutableSet<String> singleProbeSet = ImmutableSet.of("111");

        // this will contain targets 1,2,3
        final ImmutableSet<String> multipleProbeSet = ImmutableSet.of("111", "222");

        assertThat(scopeFactory.multiProbeEntityTypeScope(singleProbeSet, EntityType.VIRTUAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "3"));

        assertThat(scopeFactory.multiProbeEntityTypeScope(multipleProbeSet, EntityType.VIRTUAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3"));

        assertThat(scopeFactory.multiProbeEntityTypeScope(multipleProbeSet, EntityType.PHYSICAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), contains("4"));
    }

    @Test
    public void testProbeCategoryEntityTypeScope() throws Exception {
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
                ProbeCategory.HYPERVISOR, EntityType.VIRTUAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3"));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
                ProbeCategory.HYPERCONVERGED, EntityType.VIRTUAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("5"));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.STORAGE, EntityType.VIRTUAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
        assertThat(scopeFactory.probeCategoryEntityTypeScope(
            ProbeCategory.HYPERVISOR, EntityType.PHYSICAL_MACHINE).entities()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("4"));
    }

    @Test
    public void testContainsAllEntityTypesScope() {
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE)).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "5"));
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE, EntityType.PHYSICAL_MACHINE)).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "4", "5"));
        assertThat(scopeFactory.containsAllEntityTypesScope(ImmutableList.of(
                EntityType.VIRTUAL_MACHINE, EntityType.STORAGE_VOLUME)).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultiProbeCategoryEntityTypeScope() throws Exception {
        assertThat(scopeFactory.multiProbeCategoryEntityTypeScope(
                ImmutableSet.of(ProbeCategory.HYPERVISOR, ProbeCategory.HYPERCONVERGED),
                EntityType.VIRTUAL_MACHINE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), containsInAnyOrder("1", "2", "3", "5"));
        assertThat(scopeFactory.multiProbeCategoryEntityTypeScope(
                ImmutableSet.of(ProbeCategory.HYPERVISOR, ProbeCategory.STORAGE),
                EntityType.STORAGE).entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()), is(empty()));
    }

    /**
     * Test that we only get VMs from the identified target's parent.
     *
     * @throws Exception if something goes wrong getting the scope's entities.
     */
    @Test
    public void testParentTargetEntityTypeScope() {
        List<String> vmsFromParent = scopeFactory.parentTargetEntityType(
                EntityType.VIRTUAL_MACHINE, 4L)
                .entities()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList());
        assertEquals(1, vmsFromParent.size());
        assertEquals("1", vmsFromParent.iterator().next());
    }
}