package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class StitchingManagerTest {

    private final TargetStore targetStore = mock(TargetStore.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
    private final PreStitchingOperationLibrary preStitchingOperationLibrary = mock(PreStitchingOperationLibrary.class);
    private final PostStitchingOperationLibrary postStitchingOperationLibrary = mock(PostStitchingOperationLibrary.class);
    private final Target target = mock(Target.class);

    private final long probeId = 1234L;
    private final long firstTargetId = 5678L;
    private final long secondTargetId = 9012L;

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

    private final EntityDTO.Builder pm = physicalMachine("pm")
        .numCpuCores(3)
        .build().toBuilder();

    private final Map<String, StitchingEntityData> entityData = ImmutableMap.of(
        vmFoo.getId(), nextEntity(vmFoo, firstTargetId),
        vmBar.getId(), nextEntity(vmBar, firstTargetId),
        otherFoo.getId(), nextEntity(otherFoo, secondTargetId),
        otherBar.getId(), nextEntity(otherBar, secondTargetId),
        pm.getId(), nextEntity(pm, firstTargetId)
    );

    @Captor
    private ArgumentCaptor<TopologyStitchingEntity> stitchingEntityCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(target.getId()).thenReturn(firstTargetId);
        when(targetStore.getProbeTargets(eq(probeId)))
            .thenReturn(Collections.singletonList(target));
        when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
    }

    @Test
    public void testStitchAloneOperation()  {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsAlone("foo", "bar");
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, null);
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext(targetStore, Collections.emptyMap())).thenReturn(stitchingContext);

        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                    postStitchingOperationLibrary, probeStore, targetStore);

        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
        verify(stitchingContext).removeEntity(stitchingEntityCaptor.capture());

        assertEquals("foo", stitchingEntityCaptor.getValue().getLocalId());
        assertEquals("updated-vm", vmBar.getDisplayName());
    }

    @Test
    public void testStitchWithExternalEntities() {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsByGuestName();
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, null);
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext(targetStore, Collections.emptyMap())).thenReturn(stitchingContext);

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);

        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
        verify(stitchingContext, times(2)).removeEntity(stitchingEntityCaptor.capture());

        final List<String> removedEntities = stitchingEntityCaptor.getAllValues().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder("other-foo", "other-bar"));
        assertEquals("updated-foo", vmFoo.getDisplayName());
        assertEquals("updated-bar", vmBar.getDisplayName());
    }

    @Test
    public void testPreStitching() {
        when(preStitchingOperationLibrary.getPreStitchingOperations()).thenReturn(
            Collections.singletonList(new EntityScopePreStitchingOperation()));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5, null);
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = contextBuilder.build();

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
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
        final TopologyGraph graph = TopologyGraph.newGraph(entities);
        final StitchingJournal<TopologyEntity> stitchingJournal = new StitchingJournal<>();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, Collections.emptyMap(),
            Collections.emptyMap());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
        stitchingManager.postStitch(graphWithSettings, stitchingJournal);

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
        final StitchingContext stitchingContext = mock(StitchingContext.class);
        when(stitchingOperationStore.getAllOperations()).thenReturn(Collections.emptyList());
        final StitchingManager stitchingManager =
            spy(new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                postStitchingOperationLibrary, probeStore, targetStore));
        @SuppressWarnings("unchecked")
        final StitchingJournal<StitchingEntity> journal = mock(StitchingJournal.class);
        stitchingManager.stitch(stitchingContext, journal);

        verify(journal).recordTargets(any(Supplier.class));
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

        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(@Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
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
            return Optional.empty();
        }

        @Override
        public Optional<String> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
            return Optional.of(internalEntity.getLocalId());
        }

        @Override
        public Optional<Void> getExternalSignature(@Nonnull StitchingEntity externalEntity) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public TopologicalChangelog stitch(@Nonnull Collection<StitchingPoint> stitchingPoints,
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

    public static class StitchVmsByGuestName implements StitchingOperation<String, String> {
        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(@Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
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
        public Optional<String> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
            return Optional.of(internalEntity.getEntityBuilder().getVirtualMachineData().getGuestName());
        }

        @Override
        public Optional<String> getExternalSignature(@Nonnull StitchingEntity externalEntity) {
            return Optional.of(externalEntity.getEntityBuilder().getVirtualMachineData().getGuestName());
        }

        @Nonnull
        @Override
        public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                         @Nonnull StitchingChangesBuilder<StitchingEntity> result) {
            stitchingPoints.forEach(stitchingPoint -> {
                stitchingPoint.getExternalMatches().forEach(result::queueEntityRemoval);
                stitchingPoint.getInternalEntity().getEntityBuilder()
                    .setDisplayName("updated-" + stitchingPoint.getInternalEntity().getLocalId());
            });

            return result.build();
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
        public TopologicalChangelog performOperation(@Nonnull Stream<StitchingEntity> entities,
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
        public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                                                   @Nonnull final EntitySettingsCollection settingsCollection,
                                                   @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            entities.forEach(entity ->
                resultBuilder.queueUpdateEntityAlone(entity,
                    e -> e.getTopologyEntityDtoBuilder().setDisplayName("post-stitch-updated-" + e.getOid())));

            return resultBuilder.build();
        }
    }
}