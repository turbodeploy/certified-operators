package com.vmturbo.components.test.utilities.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyUtilsTest {

    @Test
    public void testGenerateProbeTopology() throws Exception {
        final int requestedSize = 500;
        Map<EntityType, Long> entityTypeCounts = TopologyUtils.generateProbeTopology(requestedSize).stream()
            .collect(Collectors.groupingBy(
                EntityDTO::getEntityType,
                Collectors.counting()
            ));

        assertEquals(
            entityTypeCounts.get(EntityType.APPLICATION),
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE)
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE))
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE))
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE),
            greaterThan(entityTypeCounts.get(EntityType.STORAGE))
        );
        assertThat(
            entityTypeCounts.get(EntityType.STORAGE),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE))
        );
        assertEquals(1L, (long)entityTypeCounts.get(EntityType.DATACENTER));

        // Verify the number of generated entities was very close to the number of requested entities.
        int totalEntities = entityTypeCounts.values().stream()
            .mapToInt(Long::intValue)
            .sum();
        Assert.assertThat(Math.abs(totalEntities - requestedSize), lessThan(5));
    }

    @Test
    @Ignore
    public void testGenerateTopology() throws Exception {
        final int requestedSize = 600;
        Map<Integer, Long> entityTypeCounts = TopologyUtils.generateTopology(requestedSize).stream()
            .collect(Collectors.groupingBy(
                TopologyEntityDTO::getEntityType,
                Collectors.counting()
            ));

        assertEquals(
            entityTypeCounts.get(EntityType.APPLICATION.getNumber()),
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE.getNumber())
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE.getNumber()),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE.getNumber()))
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE.getNumber()),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE.getNumber()))
        );
        assertThat(
            entityTypeCounts.get(EntityType.VIRTUAL_MACHINE.getNumber()),
            greaterThan(entityTypeCounts.get(EntityType.STORAGE.getNumber()))
        );
        assertThat(
            entityTypeCounts.get(EntityType.STORAGE.getNumber()),
            greaterThan(entityTypeCounts.get(EntityType.PHYSICAL_MACHINE.getNumber()))
        );
        assertEquals(1L, (long)entityTypeCounts.get(EntityType.DATACENTER.getNumber()));

        // Verify the number of generated entities was very close to the number of requested entities.
        int totalEntities = entityTypeCounts.values().stream()
            .mapToInt(Long::intValue)
            .sum();
        Assert.assertThat(Math.abs(totalEntities - requestedSize), lessThan(5));
    }
}