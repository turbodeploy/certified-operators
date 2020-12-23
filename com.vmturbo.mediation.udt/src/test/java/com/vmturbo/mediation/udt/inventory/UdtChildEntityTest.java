package com.vmturbo.mediation.udt.inventory;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link UdtChildEntity}.
 */
public class UdtChildEntityTest {

    /**
     * Verify a correctness on entity fields.
     */
    @Test
    public void testInstanceFields() {
        EntityType type = EntityType.VIRTUAL_MACHINE;
        long oid = 908070L;
        String udtId = "aa2020bb";
        UdtChildEntity udtChildEntity = new UdtChildEntity(oid, type);

        Assert.assertEquals(oid, udtChildEntity.getOid());
        Assert.assertEquals(type, udtChildEntity.getEntityType());

        udtChildEntity.setUdtId(udtId);
        Assert.assertEquals(udtId, udtChildEntity.getDtoId());

    }
}
