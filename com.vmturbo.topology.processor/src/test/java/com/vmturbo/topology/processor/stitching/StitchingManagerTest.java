package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class StitchingManagerTest {

    private final TargetStore targetStore = mock(TargetStore.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
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

    private final Map<String, StitchingEntityData> entityData = ImmutableMap.of(
        vmFoo.getId(), nextEntity(vmFoo, firstTargetId),
        vmBar.getId(), nextEntity(vmBar, firstTargetId),
        otherFoo.getId(), nextEntity(otherFoo, secondTargetId),
        otherBar.getId(), nextEntity(otherBar, secondTargetId)
    );

    @Captor
    private ArgumentCaptor<TopologyStitchingEntity> stitchingEntityCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(target.getId()).thenReturn(firstTargetId);
        when(targetStore.getProbeTargets(eq(probeId)))
            .thenReturn(Collections.singletonList(target));
    }

    @Test
    public void testStitchGeneratesContext() throws Exception {
        final StitchingContext stitchingContext = mock(StitchingContext.class);
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        when(stitchingOperationStore.getAllOperations()).thenReturn(Collections.emptyList());
        final StitchingManager stitchingManager =
                spy(new StitchingManager(stitchingOperationStore, targetStore));
        final StitchingContext returnedContext = stitchingManager.stitch(entityStore);

        verify(stitchingManager).stitch(eq(stitchingContext));
        assertEquals(stitchingContext, returnedContext);
    }

    @Test
    public void testStitchAloneOperation()  {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsAlone("foo", "bar");
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5);
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, targetStore);

        stitchingManager.stitch(stitchingContext);
        verify(stitchingContext).removeEntity(stitchingEntityCaptor.capture());

        assertEquals("foo", stitchingEntityCaptor.getValue().getLocalId());
        assertEquals("updated-vm", vmBar.getDisplayName());
    }

    @Test
    public void testStitchWithExternalEntities() {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsByGuestName();
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5);
        entityData.values()
            .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        final StitchingContext stitchingContext = spy(contextBuilder.build());
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);

        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, targetStore);

        stitchingManager.stitch(stitchingContext);
        verify(stitchingContext, times(2)).removeEntity(stitchingEntityCaptor.capture());

        final List<String> removedEntities = stitchingEntityCaptor.getAllValues().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder("other-foo", "other-bar"));
        assertEquals("updated-foo", vmFoo.getDisplayName());
        assertEquals("updated-bar", vmBar.getDisplayName());
    }

    private long curOid = 1L;
    private StitchingEntityData nextEntity(@Nonnull final EntityDTO.Builder entityDto, final long targetId) {
        return new StitchingEntityData(entityDto, targetId, curOid++, 0);
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
        public StitchingOperationResult stitch(@Nonnull Collection<StitchingPoint> stitchingPoints,
                                               @Nonnull StitchingOperationResult.Builder result) {
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
        public StitchingOperationResult stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                               @Nonnull StitchingOperationResult.Builder result) {
            stitchingPoints.forEach(stitchingPoint -> {
                stitchingPoint.getExternalMatches().forEach(result::queueEntityRemoval);
                stitchingPoint.getInternalEntity().getEntityBuilder()
                    .setDisplayName("updated-" + stitchingPoint.getInternalEntity().getLocalId());
            });

            return result.build();
        }
    }
}