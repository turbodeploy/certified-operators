package com.vmturbo.extractor.models;

import java.util.Collection;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests {@link EntityRelationshipFilter}.
 */
public class EntityRelationshipFilterTest {

    /**
     * Tests whitelist.
     */
    @Test
    public void testCorrectFiltering() {

        final Collection<Integer> acceptedRelatedTypes =
                ImmutableList.of(EntityType.PHYSICAL_MACHINE_VALUE, EntityType.DATACENTER_VALUE);
        final Collection<Integer> rejectedRelatedTypes =
                ImmutableList.of(EntityType.CONTAINER_VALUE);

        Assert.assertArrayEquals(acceptedRelatedTypes.toArray(), acceptedRelatedTypes
                .stream()
                .filter(EntityRelationshipFilter.forType(EntityType.VIRTUAL_MACHINE_VALUE))
                .toArray());

        Assert.assertEquals(0, rejectedRelatedTypes
                .stream()
                .filter(EntityRelationshipFilter.forType(EntityType.VIRTUAL_MACHINE_VALUE))
                .count());
    }

    /**
     * Tests that entities that are not contained in the whitelist's keys are allowed to relate to
     * any entity.
     */
    @Test
    public void testMissingEntityWillFilterOutAllRelatedEntities() {
        final Collection<Integer> relatedTypes =
                ImmutableList.of(EntityType.PHYSICAL_MACHINE_VALUE, EntityType.DATACENTER_VALUE);

        Assert.assertEquals(0, relatedTypes
                .stream()
                .filter(EntityRelationshipFilter.forType(EntityType.UNKNOWN_VALUE))
                .count());
    }
}