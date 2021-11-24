package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.AbstractExternalSignatureCachingStitchingOperation;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class StitchingManagerTest {

    private static final long PROBE_ID = 1234L;
    private static final long FIRST_TARGET_ID = 5678L;
    private static final long SECOND_TARGET_ID = 9012L;

    private final TargetStore targetStore = mock(TargetStore.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final CpuCapacityStore cpuCapacityStore = mock(CpuCapacityStore.class);
    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
    private final PreStitchingOperationLibrary preStitchingOperationLibrary = mock(PreStitchingOperationLibrary.class);
    private final PostStitchingOperationLibrary postStitchingOperationLibrary = mock(PostStitchingOperationLibrary.class);
    private final Target target = mock(Target.class);


    // Entities on the first target are internal
    private final EntityDTO.Builder vmFoo = virtualMachine("foo")
        .guestName("foo")
        .build().toBuilder();
    private final EntityDTO.Builder vmBar = virtualMachine("bar")
        .guestName("bar")
        .build().toBuilder();

    // Entities on the second target are external
    private final EntityDTO.Builder otherFoo = virtualMachine("other-foo")
        .guestName("foo")
        .build().toBuilder();
    private final EntityDTO.Builder otherBar = virtualMachine("other-bar")
        .guestName("bar")
        .build().toBuilder();
    private final EntityDTO.Builder unstitchedProxy2Remove =
        virtualMachine("unstitched-proxy-delete")
        .guestName("nomatch")
        .build().toBuilder()
        .setKeepStandalone(false)
        .setOrigin(EntityOrigin.PROXY);
    private final EntityDTO.Builder unstitchedProxy2Keep = virtualMachine("unstitched-proxy-keep")
        .guestName("nomatch")
        .build().toBuilder()
        .setKeepStandalone(true)
        .setOrigin(EntityOrigin.PROXY);

    private final EntityDTO.Builder pm = physicalMachine("pm")
        .numCpuCores(3)
        .build().toBuilder();

    private final Map<String, StitchingEntityData> entityData =
        ImmutableMap.<String, StitchingEntityData>builder()
            .put(vmFoo.getId(), nextEntity(vmFoo, FIRST_TARGET_ID))
            .put(vmBar.getId(), nextEntity(vmBar, FIRST_TARGET_ID))
            .put(otherFoo.getId(), nextEntity(otherFoo, SECOND_TARGET_ID))
            .put(otherBar.getId(), nextEntity(otherBar, SECOND_TARGET_ID))
            .put(unstitchedProxy2Remove.getId(), nextEntity(unstitchedProxy2Remove,
                    SECOND_TARGET_ID))
            .put(unstitchedProxy2Keep.getId(), nextEntity(unstitchedProxy2Keep, SECOND_TARGET_ID))
            .put(pm.getId(), nextEntity(pm, FIRST_TARGET_ID))
            .build();

    @Captor
    private ArgumentCaptor<TopologyStitchingEntity> stitchingEntityCaptor;

    /**
     * Rule to manage enablements via a mutable store.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(target.getId()).thenReturn(FIRST_TARGET_ID);
        when(target.getDisplayName()).thenReturn("target");
        when(targetStore.getProbeTargets(eq(PROBE_ID)))
            .thenReturn(Collections.singletonList(target));
        when(targetStore.getAll()).thenReturn(Collections.singletonList(target));
        when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
        when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(SDKProbeType.VCENTER.getProbeType())
            .build()));
        when(target.getProbeId()).thenReturn(20L);
        when(target.getProbeInfo()).thenReturn(ProbeInfo.getDefaultInstance());
    }

    @Test
    public void testStitchAloneOperation()  {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsAlone("foo", "bar");
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class));
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(target));
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(PROBE_ID, stitchingOperation)));
        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                    postStitchingOperationLibrary, probeStore, targetStore, cpuCapacityStore);

        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
        verify(stitchingContext, times(2))
            .removeEntity(stitchingEntityCaptor.capture());

        final List<String> removedEntities = stitchingEntityCaptor.getAllValues().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder(vmFoo.getId(),
            unstitchedProxy2Remove.getId()));
        assertEquals("updated-vm", vmBar.getDisplayName());
    }

    @Test
    public void testStitchWithExternalEntities() {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsByGuestName();
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(PROBE_ID, stitchingOperation)));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class));
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(target));
        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore, cpuCapacityStore);

        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
        verify(stitchingContext, times(3)).removeEntity(stitchingEntityCaptor.capture());

        final List<String> removedEntities = stitchingEntityCaptor.getAllValues().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder(otherFoo.getId(), otherBar.getId(),
            unstitchedProxy2Remove.getId()));
        assertFalse(removedEntities.contains(unstitchedProxy2Keep.getId()));
        assertEquals("updated-foo", vmFoo.getDisplayName());
        assertEquals("updated-bar", vmBar.getDisplayName());
    }

    /**
     * Test that when we have a stitching operation with two internal targets both targets stitch
     * properly.
     */
    @Test
    public void testStitchTwoInternalTargetsWithExternalEntities() {
        final long thirdTargetId = 9013L;
        final long stitchingProbeId = 2424L;
        final Target target2 = mock(Target.class);
        final Target target3 = mock(Target.class);
        final Map<String, StitchingEntityData> entityDataLocal =
                ImmutableMap.<String, StitchingEntityData>builder()
                        .put(vmFoo.getId(), nextEntity(vmFoo, FIRST_TARGET_ID))
                        .put(vmBar.getId(), nextEntity(vmBar, FIRST_TARGET_ID))
                        .put(otherFoo.getId(), nextEntity(otherFoo, SECOND_TARGET_ID))
                        .put(otherBar.getId(), nextEntity(otherBar, thirdTargetId))
                .build();
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsByGuestNameWithScope();
        when(stitchingOperationStore.getAllOperations())
                .thenReturn(Collections.singletonList(new ProbeStitchingOperation(stitchingProbeId,
                        stitchingOperation)));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(4,
                targetStore).setIdentityProvider(mock(IdentityProviderImpl.class));
        entityDataLocal.values()
                .forEach(entity -> contextBuilder.addEntity(entity, entityDataLocal));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        when(targetStore.getTarget(FIRST_TARGET_ID)).thenReturn(Optional.of(target));
        when(targetStore.getTarget(SECOND_TARGET_ID)).thenReturn(Optional.of(target2));
        when(targetStore.getTarget(thirdTargetId)).thenReturn(Optional.of(target3));
        when(targetStore.getProbeTargets(stitchingProbeId)).thenReturn(Lists.newArrayList(target2,
                target3));
        when(target2.getId()).thenReturn(SECOND_TARGET_ID);
        when(target3.getId()).thenReturn(thirdTargetId);
        when(probeStore.getProbe(stitchingProbeId)).thenReturn(Optional.of(ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE.getCategory())
                .setUiProbeCategory(ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE.getCategory())
                .setProbeType(SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType())
                .build()));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(PROBE_ID));

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore,
                targetStore, cpuCapacityStore);

        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
        verify(stitchingContext, times(2)).removeEntity(stitchingEntityCaptor
                .capture());

        final List<String> removedEntities = stitchingEntityCaptor.getAllValues().stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder(vmFoo.getId(), vmBar.getId()));
        assertEquals("updated-other-foo", otherFoo.getDisplayName());
        assertEquals("updated-other-bar", otherBar.getDisplayName());
    }

    @Test
    public void testPreStitching() {
        when(preStitchingOperationLibrary.getPreStitchingOperations()).thenReturn(
            Collections.singletonList(new EntityScopePreStitchingOperation()));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = contextBuilder.build();

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore, cpuCapacityStore);
        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());

        entityData.values().forEach(entity -> {
            if (entity.getEntityDtoBuilder().getEntityType() == EntityType.VIRTUAL_MACHINE) {
                assertThat(entity.getEntityDtoBuilder().getDisplayName(), startsWith("updated-"));
            } else {
                assertThat(entity.getEntityDtoBuilder().getDisplayName(), not(startsWith("updated-")));
            }
        });
    }

    @Test
    public void testPostStitching() {
        when(postStitchingOperationLibrary.getPostStitchingOperations()).thenReturn(
            Collections.singletonList(new EntityScopePostStitchingOperation()));
        final Map<Long, TopologyEntity.Builder> entities = ImmutableMap.of(
            1L, topologyEntityBuilder(1L, EntityType.VIRTUAL_MACHINE, Collections.emptyList()),
            2L, topologyEntityBuilder(2L, EntityType.PHYSICAL_MACHINE, Collections.emptyList()),
            3L, topologyEntityBuilder(3L, EntityType.STORAGE, Collections.emptyList()),
            4L, topologyEntityBuilder(4L, EntityType.VIRTUAL_MACHINE, Collections.emptyList())
        );
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(entities);
        final StitchingJournal<TopologyEntity> stitchingJournal = new StitchingJournal<>();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, Collections.emptyMap(),
            Collections.emptyMap());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
                cpuCapacityStore);
        stitchingManager.postStitch(graphWithSettings, stitchingJournal, Collections.emptySet());

        graph.entities().forEach(entity -> {
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                assertThat(entity.getDisplayName(), startsWith("post-stitch-updated-"));
            } else {
                assertThat(entity.getDisplayName(), not(startsWith("post-stitch-updated-")));
            }
        });
    }

    @Test
    public void testTargetsRecordedInJournal() {
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(0, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);
        when(stitchingOperationStore.getAllOperations()).thenReturn(Collections.emptyList());
        final StitchingManager stitchingManager =
            spy(new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                postStitchingOperationLibrary, probeStore, targetStore, cpuCapacityStore));
        @SuppressWarnings("unchecked")
        final StitchingJournal<StitchingEntity> journal = mock(StitchingJournal.class);
        stitchingManager.stitch(stitchingContext, journal);

        verify(journal).recordTargets(any(Supplier.class));
    }
    @RunWith(Parameterized.class)
    public static class DelayedDataHandlingFunctionalityTest {

        private static final String PROVIDER_ID = "123";
        private final TargetStore targetStore = mock(TargetStore.class);
        private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
        private final Target target = mock(Target.class);

        public DelayedDataHandlingFunctionalityTest(
                ParameterStructure data) {
            this.data = data;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Stream.of(
                    //case 1:
                    //      base: onto - nonStale,
                    //      base2: fromEntities - nonStale
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.DISCOVERED, false,
                            EntityOrigin.DISCOVERED),
                    //case 2:
                    //      base: onto - nonStale,
                    //      base2: fromEntities - stale
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.DISCOVERED, true,
                            EntityOrigin.DISCOVERED),
                    //case 3:
                    //      base: onto - stale,
                    //      base2: fromEntities - nonstale
                    //expected controllable true
                    new ParameterStructure(true, true, EntityOrigin.DISCOVERED, false,
                            EntityOrigin.DISCOVERED),
                    //case 4:
                    //      base: onto - stale,
                    //      base2: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.DISCOVERED, true,
                            EntityOrigin.DISCOVERED),
                    //case 5:
                    //      proxy: onto - nonStale,
                    //      base: fromEntities - nonStale
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.PROXY, false,
                            EntityOrigin.DISCOVERED),
                    //case 6:
                    //      proxy: onto - nonStale,
                    //      base: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, false, EntityOrigin.PROXY, true,
                            EntityOrigin.DISCOVERED),
                    //case 7:
                    //      proxy: onto - stale,
                    //      base: fromEntities - nonstale
                    //expected controllable true
                    new ParameterStructure(true, true, EntityOrigin.PROXY, false,
                            EntityOrigin.DISCOVERED),
                    //case 8:
                    //      proxy: onto - stale,
                    //      base: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.PROXY, true,
                            EntityOrigin.DISCOVERED),
                    //case 9:
                    //      base: onto - nonStale,
                    //      proxy: fromEntities - nonStale
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.DISCOVERED, false,
                            EntityOrigin.PROXY),
                    //case 10:
                    //      base: onto - nonStale,
                    //      proxy: fromEntities - stale
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.DISCOVERED, true,
                            EntityOrigin.PROXY),
                    //case 11:
                    //      base: onto - stale,
                    //      proxy: fromEntities - nonstale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.DISCOVERED, false,
                            EntityOrigin.PROXY),
                    //case 12:
                    //      base: onto - stale,
                    //      proxy: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.DISCOVERED, true,
                            EntityOrigin.PROXY),
                    //case 13:
                    //      proxy: onto - nonStale,
                    //      proxy: fromEntities - nonStale
                    //expected controllable false
                    new ParameterStructure(false, false, EntityOrigin.PROXY, false,
                            EntityOrigin.PROXY),
                    //case 14:
                    //      proxy: onto - nonStale,
                    //      proxy: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.PROXY, true,
                            EntityOrigin.PROXY),
                    //case 15:
                    //      proxy: onto - stale,
                    //      proxy: fromEntities - nonstale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.PROXY, false,
                            EntityOrigin.PROXY),
                    //case 16:
                    //      proxy: onto - stale,
                    //      proxy: fromEntities - stale
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.PROXY, true,
                            EntityOrigin.PROXY),
                    //case 17:
                    //      base: onto - nonstale,
                    //expected controllable true
                    new ParameterStructure(true, false, EntityOrigin.DISCOVERED, null, null),
                    //case 18:
                    //      base: onto - stale,
                    //expected controllable false
                    new ParameterStructure(false, true, EntityOrigin.DISCOVERED, null, null)).map(
                    e -> new Object[]{e}).collect(
                    Collectors.toList());
        }

        private final ParameterStructure data;

        @Before
        public void setUp() throws Exception {
            MockitoAnnotations.initMocks(this);
        }

        /**
         * Rule to manage enablements via a mutable store.
         */
        @Rule
        public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

        @Captor
        private ArgumentCaptor<Collection<StitchingPoint>> stitchingPointsCapture;

        /**
         * Test merging of entities with the {@link FeatureFlags#DELAYED_DATA_HANDLING} flag
         * enabled.
         */
        @Test
        public void testDelayedDataHandlingFunctionality() {
            featureFlagTestRule.enable(FeatureFlags.DELAYED_DATA_HANDLING);
            Mockito.when(target.getId()).thenReturn(FIRST_TARGET_ID);
            final StitchingOperation<?, ?> stitchingOperation = Mockito.spy(
                    new StorageStitchingOperation());
            Mockito.when(stitchingOperationStore.getAllOperations()).thenReturn(
                    Collections.singletonList(
                            new ProbeStitchingOperation(PROBE_ID, stitchingOperation)));
            final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5,
                    targetStore).setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

            final StitchingEntityData internalEntity = getTopologyStitchingEntity("1",
                    data.staleInternalEntity, data.originInternalEntity, FIRST_TARGET_ID);
            final StitchingEntityData externalEntity = getTopologyStitchingEntity("2",
                    data.staleExternalEntity, data.originExternalEntity, SECOND_TARGET_ID);
            final StitchingEntityData provider = StitchingEntityData.newBuilder(
                            EntityDTO.newBuilder().setId(PROVIDER_ID).setEntityType(EntityType.DISK_ARRAY))
                    .build();
            final Map<String, StitchingEntityData> entityData = new HashMap<>();
            if (internalEntity != null) {
                entityData.put(internalEntity.getLocalId(), internalEntity);
            }
            entityData.put(externalEntity.getLocalId(), externalEntity);
            entityData.put(provider.getLocalId(), provider);
            entityData.values().forEach(entity -> contextBuilder.addEntity(entity, entityData));

            final StitchingContext stitchingContext = Mockito.spy(contextBuilder.build());
            Mockito.when(Mockito.mock(EntityStore.class).constructStitchingContext()).thenReturn(
                    stitchingContext);
            Mockito.when(targetStore.getTarget(Mockito.anyLong())).thenReturn(Optional.of(target));
            final ProbeStore probeStore =  Mockito.mock(ProbeStore.class);
            when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
            when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(ProbeInfo.newBuilder()
                    .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
                    .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
                    .setProbeType(SDKProbeType.VCENTER.getProbeType())
                    .build()));
            final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                    Mockito.mock(PreStitchingOperationLibrary.class), Mockito.mock(PostStitchingOperationLibrary.class),
                    probeStore,
                    targetStore, Mockito.mock(CpuCapacityStore.class));
            Mockito.when(targetStore.getProbeTargets(Mockito.eq(PROBE_ID))).thenReturn(
                    Collections.singletonList(target));
            stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
            Assert.assertEquals(data.expectedControllable, externalEntity.getEntityDtoBuilder()
                    .getConsumerPolicyBuilder()
                    .getControllable());
            Mockito.verify(stitchingOperation, Mockito.times(1)).stitch(stitchingPointsCapture.capture(),
                    Mockito.any());
            if (internalEntity != null) {
                Assert.assertFalse(stitchingPointsCapture.getValue().isEmpty());
            } else {
                Assert.assertTrue(stitchingPointsCapture.getValue().isEmpty());
            }
        }

        private StitchingEntityData getTopologyStitchingEntity(String id, Boolean stale,
                EntityOrigin origin, long targetId) {
            if (stale == null || origin == null) {
                return null;
            }
            final EntityDTO.Builder firstBuilder = EntityDTO.newBuilder().setId(id).setEntityType(
                    EntityType.STORAGE).addCommoditiesBought(CommodityBought.newBuilder()
                    .addBought(CommodityDTO.newBuilder()
                            .setCommodityType(CommodityType.ACCESS)
                            .build())
                    .setProviderId(PROVIDER_ID)
                    .setProviderType(EntityType.DISK_ARRAY)
                    .build()).setOrigin(origin).setStorageData(
                    StorageData.newBuilder().addExternalName("external name ").build());
            return StitchingEntityData.newBuilder(firstBuilder).oid(Long.parseLong(id)).setStale(
                    stale).targetId(targetId).build();
        }
    }

    private static class ParameterStructure {
        private final Boolean staleExternalEntity;
        private final Boolean staleInternalEntity;
        private final EntityOrigin originExternalEntity;
        private final EntityOrigin originInternalEntity;
        private final boolean expectedControllable;

        public ParameterStructure(boolean expectedControllable, Boolean staleExternalEntity,
                EntityOrigin originExternalEntity, Boolean staleInternalEntity, EntityOrigin originInternalEntity) {
            this.staleExternalEntity = staleExternalEntity;
            this.originExternalEntity = originExternalEntity;
            this.staleInternalEntity = staleInternalEntity;
            this.originInternalEntity = originInternalEntity;
            this.expectedControllable = expectedControllable;
        }
    }

    private long curOid = 1L;
    private StitchingEntityData nextEntity(@Nonnull final EntityDTO.Builder entityDto, final long targetId) {
        return StitchingEntityData.newBuilder(entityDto)
            .targetId(targetId)
            .oid(curOid++)
            .lastUpdatedTime(0)
            .build();
    }

    public static class StitchVmsAlone implements  StitchingOperation<String, Void> {

        private final String vmToRemove;

        private final String vmToUpdate;

        public StitchVmsAlone(@Nonnull final String vmToRemove,
                              @Nonnull final String vmToUpdate) {
            this.vmToRemove = Objects.requireNonNull(vmToRemove);
            this.vmToUpdate = Objects.requireNonNull(vmToUpdate);
        }

        @Override
        public void initializeOperationBeforeStitching(
                @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {

        }

        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(
                @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Map<Void, Collection<StitchingEntity>> getExternalSignatures(
                @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Collections.emptyMap();
        }

        @Nonnull
        @Override
        public EntityType getInternalEntityType() {
            return EntityType.VIRTUAL_MACHINE;
        }

        @Nonnull
        @Override
        public Optional<EntityType> getExternalEntityType() {
            return Optional.empty();
        }

        @Override
        public Collection<String> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
            return Collections.singleton(internalEntity.getLocalId());
        }

        @Nonnull
        @Override
        public TopologicalChangelog<StitchingEntity> stitch(@Nonnull Collection<StitchingPoint> stitchingPoints,
                                         @Nonnull StitchingChangesBuilder<StitchingEntity> result) {
            for (StitchingPoint stitchingPoint : stitchingPoints) {
                final StitchingEntity internalEntity = stitchingPoint.getInternalEntity();

                if (internalEntity.getLocalId().equals(vmToRemove)) {
                    result.queueEntityRemoval(internalEntity);
                } else if (internalEntity.getLocalId().equals(vmToUpdate)) {
                    internalEntity.getEntityBuilder().setDisplayName("updated-vm");
                }
            }
            return result.build();
        }
    }

    public static class StitchVmsByGuestName extends
            AbstractExternalSignatureCachingStitchingOperation<String, String> {
        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(
                @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public EntityType getInternalEntityType() {
            return EntityType.VIRTUAL_MACHINE;
        }

        @Nonnull
        @Override
        public Optional<EntityType> getExternalEntityType() {
            return Optional.of(EntityType.VIRTUAL_MACHINE);
        }

        @Override
        public Collection<String> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
            return Collections.singleton(internalEntity.getEntityBuilder().getVirtualMachineData()
                            .getGuestName());
        }

        @Override
        protected Collection<String> getExternalSignature(@Nonnull StitchingEntity externalEntity) {
            return Collections.singleton(externalEntity.getEntityBuilder().getVirtualMachineData()
                            .getGuestName());
        }

        @Nonnull
        @Override
        public TopologicalChangelog<StitchingEntity> stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                         @Nonnull StitchingChangesBuilder<StitchingEntity> result) {
            stitchingPoints.forEach(stitchingPoint -> {
                stitchingPoint.getExternalMatches().forEach(result::queueEntityRemoval);
                stitchingPoint.getInternalEntity().getEntityBuilder()
                    .setDisplayName("updated-" + stitchingPoint.getInternalEntity().getLocalId());
            });

            return result.build();
        }
    }

    public static class StitchVmsByGuestNameWithScope extends StitchVmsByGuestName {
        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(
                @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Optional.of(stitchingScopeFactory.probeCategoryEntityTypeScope(
                    ProbeCategory.HYPERVISOR, EntityType.VIRTUAL_MACHINE));
        }
    }

    private static class EntityScopePreStitchingOperation implements PreStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE);
        }

        @Nonnull
        @Override
        public TopologicalChangelog<StitchingEntity> performOperation(@Nonnull Stream<StitchingEntity> entities,
                                                   @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
            entities.forEach(entity ->
                resultBuilder.queueUpdateEntityAlone(entity,
                    e -> e.getEntityBuilder().setDisplayName("updated-" + e.getLocalId())));

            return resultBuilder.build();
        }
    }

    private static class EntityScopePostStitchingOperation implements PostStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE);
        }

        @Nonnull
        @Override
        public TopologicalChangelog<TopologyEntity> performOperation(@Nonnull final Stream<TopologyEntity> entities,
                                                   @Nonnull final EntitySettingsCollection settingsCollection,
                                                   @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            entities.forEach(entity ->
                resultBuilder.queueUpdateEntityAlone(entity,
                    e -> e.getTopologyEntityDtoBuilder().setDisplayName("post-stitch-updated-" + e.getOid())));

            return resultBuilder.build();
        }
    }
}
