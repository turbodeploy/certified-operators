package com.vmturbo.extractor.patchers;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for AbstractPatcher.
 */
public class AbstractPatcherTest {
    /**
     * Test how enum value is converted to db value.
     */
    @Test
    public void testGetEnumDbValue() {
        Assert.assertEquals(1L, AbstractPatcher.getEnumDbValue(1L, PatchCase.SEARCH, Long.class,
                        null));
        Assert.assertEquals(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE.ordinal(),
                        AbstractPatcher.getEnumDbValue(
                                        CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE,
                                        PatchCase.SEARCH, CommonDTO.EntityDTO.EntityType.class,
                                        null));
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE,
                        AbstractPatcher.getEnumDbValue(1L, PatchCase.REPORTING, Long.class,
                                        val -> EntityType.VIRTUAL_MACHINE));
        Assert.assertNull(AbstractPatcher.getEnumDbValue(1L, PatchCase.REPORTING, Long.class,
                        null));
    }
}
