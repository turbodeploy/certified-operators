package com.vmturbo.components.common;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Unit test for {@link com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits}.
 */
public class CommodityTypeUnitsTest {
    /**
     * Test that every non-deprecated commodity type is listed in commodity units, to
     * ensure that this enum will be kept up to date when new commodity types are added.
     */
    @Test
    public void testAllCommodityTypesPresent() {
        for (CommodityType commodityType : CommodityType.values()) {
            if (commodityType.getDescriptorForType().getOptions().getDeprecated()) {
                continue;
            }

            boolean present = true;
            try {
                CommodityTypeUnits.valueOf(commodityType.name());
            } catch (IllegalArgumentException ex) {
                present = false;
            }
            Assert.assertTrue(commodityType.name()
                + " must be present in ClassicEnumMapper.CommodityTypeUnits", present);
        }
    }
}
