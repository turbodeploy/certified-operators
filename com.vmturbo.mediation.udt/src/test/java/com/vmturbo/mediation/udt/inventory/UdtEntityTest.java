package com.vmturbo.mediation.udt.inventory;

import static java.util.Collections.emptySet;

import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link UdtEntity}.
 */
public class UdtEntityTest {

    /**
     * Verify UdtEntity fields values.
     */
    @Test
    public void testInstanceFields() {
        final EntityType type = EntityType.SERVICE;
        final String id = "908070";
        final String name = "svc-0";
        final Set<UdtChildEntity> childs = Sets.newHashSet(new UdtChildEntity(10L, EntityType.VIRTUAL_MACHINE));
        final UdtEntity udtEntity = new UdtEntity(type, id, name, childs);

        Assert.assertEquals(type, udtEntity.getEntityType());
        Assert.assertEquals(id, udtEntity.getId());
        Assert.assertEquals(name, udtEntity.getName());
        Assert.assertTrue(childs.containsAll(udtEntity.getChildren()));

    }

    /**
     * Tests that UdtEntity with OID value provides identification for an EntityDTO == str(OID).
     * Otherwise, it provides UdtEntity.id (goes from topology data definition ID).
     */
    @Test
    public void testOidValue() {
        final String tddId = "605040";
        final UdtEntity udtEntity = new UdtEntity(EntityType.SERVICE, tddId, "vm-0", emptySet());
        Assert.assertEquals(tddId, udtEntity.getId());
    }
}
