package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.StitchingGraph;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingOperationResult.CommoditiesBoughtChange;
import com.vmturbo.stitching.StitchingOperationResult.RemoveEntityChange;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class StitchingManagerTest {

    private final TargetStore targetStore = mock(TargetStore.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final StitchingOperationStore stitchingOperationStore = mock(StitchingOperationStore.class);
    final StitchingContext stitchingContext = mock(StitchingContext.class);
    private final Target target = mock(Target.class);

    private final long probeId = 1234L;
    private final long targetId = 5678L;

    private final EntityDTO.Builder vmFoo = virtualMachine("foo")
        .guestName("foo")
        .build().toBuilder();
    private final EntityDTO.Builder vmBar = virtualMachine("bar")
        .guestName("bar")
        .build().toBuilder();

    private final EntityDTO.Builder otherFoo = virtualMachine("other-foo")
        .guestName("foo")
        .build().toBuilder();
    private final EntityDTO.Builder otherBar = virtualMachine("other-bar")
        .guestName("bar")
        .build().toBuilder();

    @Captor
    private ArgumentCaptor<StitchingOperationResult> resultCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(target.getId()).thenReturn(targetId);
        when(targetStore.getProbeTargets(eq(probeId)))
            .thenReturn(Collections.singletonList(target));
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);
    }

    @Test
    public void testStitchGeneratesContext() throws Exception {
        when(stitchingOperationStore.getAllOperations()).thenReturn(Collections.emptyList());
        final StitchingManager stitchingManager = spy(new StitchingManager(stitchingOperationStore));
        final StitchingContext returnedContext = stitchingManager.stitch(entityStore, targetStore);

        verify(stitchingManager).stitch(eq(stitchingContext), eq(targetStore));
        assertEquals(stitchingContext, returnedContext);
    }

    @Test
    public void testStitchAloneOperation()  {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsAlone("foo", "bar");
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        when(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, targetId))
            .thenReturn(Stream.of(vmBar, vmFoo, otherFoo, otherBar));
        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore);

        final StitchingContext resultContext = stitchingManager.stitch(stitchingContext, targetStore);
        verify(resultContext).applyStitchingResult(resultCaptor.capture());

        final RemoveEntityChange removal = (RemoveEntityChange)resultCaptor.getValue()
            .getChanges().stream()
            .filter(change -> change instanceof RemoveEntityChange)
            .findFirst()
            .get();
        final CommoditiesBoughtChange update = (CommoditiesBoughtChange)resultCaptor.getValue()
            .getChanges().stream()
            .filter(change -> change instanceof CommoditiesBoughtChange)
            .findFirst()
            .get();

        assertEquals("foo", removal.entityBuilder.getId());
        assertEquals("bar", update.entityBuilder.getId());
    }

    @Test
    public void testStitchWithExternalEntities() {
        final StitchingOperation<?, ?> stitchingOperation = new StitchVmsByGuestName();
        when(stitchingOperationStore.getAllOperations())
            .thenReturn(Collections.singletonList(new ProbeStitchingOperation(probeId, stitchingOperation)));
        when(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, targetId))
            .thenReturn(Stream.of(vmBar, vmFoo));
        when(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, targetId))
            .thenReturn(Stream.of(otherBar, otherFoo));

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore);

        final StitchingContext resultContext = stitchingManager.stitch(stitchingContext, targetStore);
        verify(resultContext).applyStitchingResult(resultCaptor.capture());

        final List<String> removedEntities = resultCaptor.getValue()
            .getChanges().stream()
            .filter(change -> change instanceof RemoveEntityChange)
            .map(change -> (RemoveEntityChange)change)
            .map(removal -> removal.entityBuilder.getId())
            .collect(Collectors.toList());
        final List<String> updatedEntities = resultCaptor.getValue()
            .getChanges().stream()
            .filter(change -> change instanceof CommoditiesBoughtChange)
            .map(change -> (CommoditiesBoughtChange)change)
            .map(removal -> removal.entityBuilder.getId())
            .collect(Collectors.toList());

        assertThat(removedEntities, containsInAnyOrder("other-foo", "other-bar"));
        assertThat(updatedEntities, containsInAnyOrder("foo", "bar"));
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
        public Optional<String> getInternalSignature(@Nonnull EntityDTOOrBuilder internalEntity) {
            return Optional.of(internalEntity.getId());
        }

        @Override
        public Optional<Void> getExternalSignature(@Nonnull EntityDTOOrBuilder externalEntity) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public StitchingOperationResult stitch(@Nonnull Collection<StitchingPoint> stitchingPoints,
                                               @Nonnull StitchingGraph stitchingGraph) {
            final StitchingOperationResult.Builder result = StitchingOperationResult.newBuilder();
            for (StitchingPoint stitchingPoint : stitchingPoints) {
                final EntityDTO.Builder internalEntity = stitchingPoint.getInternalEntity();

                if (internalEntity.getId().equals(vmToRemove)) {
                    result.removeEntity(internalEntity);
                } else if (internalEntity.getId().equals(vmToUpdate)) {
                    result.updateCommoditiesBought(internalEntity, entity -> {});
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
        public Optional<String> getInternalSignature(@Nonnull EntityDTOOrBuilder internalEntity) {
            return Optional.of(internalEntity.getVirtualMachineData().getGuestName());
        }

        @Override
        public Optional<String> getExternalSignature(@Nonnull EntityDTOOrBuilder externalEntity) {
            return Optional.of(externalEntity.getVirtualMachineData().getGuestName());
        }

        @Nonnull
        @Override
        public StitchingOperationResult stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                               @Nonnull StitchingGraph stitchingGraph) {
            final StitchingOperationResult.Builder result = StitchingOperationResult.newBuilder();
            stitchingPoints.forEach(stitchingPoint -> {
                stitchingPoint.getExternalMatches().forEach(result::removeEntity);
                result.updateCommoditiesBought(stitchingPoint.getInternalEntity(), entity -> {});
            });

            return result.build();
        }
    }
}