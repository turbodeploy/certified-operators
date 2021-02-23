package com.vmturbo.common.protobuf.topology;

import static com.vmturbo.api.conversion.entity.CommodityTypeMapping.COMMODITY_TYPE_TO_API_STRING;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.platform.common.dto.CommonDTO;

/**
 *
 */
public class UICommodityTypeTest {

    /**
     * Validate that the generated display names are what we expect for some of the unusual commodity
     * type strings.
     */
    @RunWith(Parameterized.class)
    public static class DisplayNameTests {
        @Parameters(name = "{index}: input {0}, expectedOutput {1}")
        public static Collection<Object[]> cases() {
            return Arrays.asList(new Object[][]{
                    {UICommodityType.STORAGE_AMOUNT, "Storage Amount"},
                    {UICommodityType.CPU_ALLOCATION, "CPU Allocation"},
                    {UICommodityType.DISK_ARRAY_ACCESS, "Disk Array Access"},
                    {UICommodityType.VCPU, "VCPU"},
                    {UICommodityType.VDC, "VDC"},
            });
        }

        @Parameter(0)
        public UICommodityType commodity;

        @Parameter(1)
        public String expectedDisplayName;

        @Test
        public void validateDisplayName() {
            Assert.assertEquals(expectedDisplayName, commodity.displayName());
        }
    }

    /**
     * This test checks if commodity type in {@link UICommodityType} is the same as commodity types in
     * {@link CommodityTypeMapping}.
     */
    @Test
    public void testConversionProjectCommodityTypes() {
        Set<CommonDTO.CommodityDTO.CommodityType> commodityTypes = new HashSet<>();
        for (UICommodityType uiCommodityType : UICommodityType.values()) {
            commodityTypes.add(uiCommodityType.sdkType());
        }

        commodityTypes.removeAll(COMMODITY_TYPE_TO_API_STRING.keySet());

        if (!commodityTypes.isEmpty()) {
            fail("The commodity types " + commodityTypes + " should be added to "
              + "com.vmturbo.api.conversion.entity.CommodityTypeMapping.COMMODITY_TYPE_TO_API_STRING");
        }
    }
}
