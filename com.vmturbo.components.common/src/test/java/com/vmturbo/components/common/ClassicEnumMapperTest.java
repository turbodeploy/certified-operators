package com.vmturbo.components.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;

/**
 * Test {@link ClassicEnumMapper}.
 */
public class ClassicEnumMapperTest {

    /**
     * Test {@link CommodityTypeUnits#unitFromEntityAndCommodityType}.
     */
    @Test
    public void testUnitFromEntityAndCommodityType() {
        String appVCPUUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.APPLICATION_COMPONENT, "vCPU", false);
        assertNull(appVCPUUnit);

        String appVCPUUnitIgnoreCase = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.APPLICATION_COMPONENT, "vCPU", true);
        assertEquals("MHz", appVCPUUnitIgnoreCase);

        String vmVCPUUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.VIRTUAL_MACHINE, "vcpu", true);
        assertEquals("MHz", vmVCPUUnit);

        String vmVCPURequestUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.VIRTUAL_MACHINE, "vcpurequest", true);
        assertEquals("mCores", vmVCPURequestUnit);

        String podVCPUUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.CONTAINERPOD, "vCPU", true);
        assertEquals("mCores", podVCPUUnit);

        String podVCPULimitQuotaUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.CONTAINERPOD, "vCPULimitQuota", true);
        assertEquals("mCores", podVCPULimitQuotaUnit);

        String cntClusterVCPUUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            StringConstants.CONTAINER_PLATFORM_CLUSTER, "vCPU", true);
        assertEquals("mCores", cntClusterVCPUUnit);

        String unKnownCommUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            null, "unknown", true);
        assertEquals("", unKnownCommUnit);

        String nullUnit = CommodityTypeUnits.unitFromEntityAndCommodityType(
            null, "fake", true);
        assertNull(nullUnit);
    }
}