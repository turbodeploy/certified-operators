package com.vmturbo.history.ingesters.common.writers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.GuestLoadFilters;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Tests for {@link ProjectedTopologyWriterBase} class.
 */
public class ProjectedTopologyWriterBaseTest {
    private static final long VM_ID = 1L;
    private static final long APP_ID = 2L;
    private static final long GUEST_APP_ID = 3L;
    private static final long NO_ENTITY_ID = 4L;

    private final TopologyDTO.ProjectedTopologyEntity vm = TopologyDTO.ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(VM_ID)
                    .build())
            .build();
    private final TopologyDTO.ProjectedTopologyEntity app = TopologyDTO.ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT_VALUE)
                    .setOid(APP_ID)
                    .build())
            .build();
    private final TopologyDTO.ProjectedTopologyEntity guestLoadApp = TopologyDTO.ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT_VALUE)
                    .setOid(GUEST_APP_ID)
                    .putEntityPropertyMap("common_dto.EntityDTO.ApplicationData.type", "GuestLoad")
                    .build())
            .build();
    private final TopologyDTO.ProjectedTopologyEntity noEntity = TopologyDTO.ProjectedTopologyEntity.newBuilder()
            .build();

    /**
     * Tests that {@link ProjectedTopologyWriterBase#processEntities(Collection, String)} called
     * without {@link com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity} which
     * don't have {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testProcessEntitiesWithoutTopologyEntity() throws InterruptedException {
        final TestProjectedTopologyWriterBase writer = new TestProjectedTopologyWriterBase(e -> true);
        writer.processChunk(Arrays.asList(vm, app, guestLoadApp, noEntity), "Topology Summary");

        final Set<Long> expectedOids = Sets.newHashSet(VM_ID, APP_ID, GUEST_APP_ID);
        Assert.assertEquals(expectedOids.size(), writer.getProcessedEntities().size());

        final Set<Long> actualOids = writer.getProcessedEntities().stream()
                .map(TopologyDTO.ProjectedTopologyEntity::getEntity)
                .map(TopologyDTO.TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
        Assert.assertEquals(expectedOids, actualOids);
    }

    /**
     * Tests that {@link ProjectedTopologyWriterBase#processEntities(Collection, String)} called
     * with filtered entities (doesn't process GuestLoad applications).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testProcessEntitiesWithFilteringByGuestLoadApps() throws InterruptedException {
        final TestProjectedTopologyWriterBase writer = new TestProjectedTopologyWriterBase(GuestLoadFilters::isNotGuestLoad);
        writer.processChunk(Arrays.asList(vm, app, guestLoadApp), "Topology Summary");

        final Set<Long> expectedOids = Sets.newHashSet(VM_ID, APP_ID);
        Assert.assertEquals(expectedOids.size(), writer.getProcessedEntities().size());

        final Set<Long> actualOids = writer.getProcessedEntities().stream()
                .map(TopologyDTO.ProjectedTopologyEntity::getEntity)
                .map(TopologyDTO.TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
        Assert.assertEquals(expectedOids, actualOids);
    }

    /**
     * Implementation of an abstract class {@link ProjectedTopologyWriterBase} to test common code.
     */
    private static class TestProjectedTopologyWriterBase extends ProjectedTopologyWriterBase {
        private Collection<TopologyDTO.ProjectedTopologyEntity> processedEntities;

        /**
         * Constructor with entities filter.
         *
         * @param entitiesFilter entities filter
         */
        TestProjectedTopologyWriterBase(@NotNull Predicate<TopologyDTO.TopologyEntityDTO> entitiesFilter) {
            super(entitiesFilter);
        }

        @Override
        protected ChunkDisposition processEntities(@NotNull Collection<TopologyDTO.ProjectedTopologyEntity> chunk, @NotNull String infoSummary) throws InterruptedException {
            processedEntities = chunk;
            return ChunkDisposition.SUCCESS;
        }

        /**
         * Gets entities which were passed to process into {@link #processEntities(Collection, String)} method.
         *
         * @return Returns processed entities.
         */
        protected Collection<TopologyDTO.ProjectedTopologyEntity> getProcessedEntities() {
            return processedEntities;
        }
    }
}
