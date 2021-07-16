package com.vmturbo.api.conversion.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link CommodityTypeMapping}.
 */
public class CommodityTypeMappingTest {

    /**
     * Test {@link CommodityTypeMapping#getUnitForEntityCommodityType}.
     */
    @Test
    public void testGetUnitForEntityCommodityType() {
        String appVCPUUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.APPLICATION_COMPONENT_VALUE, CommodityType.VCPU_VALUE);
        assertEquals("MHz", appVCPUUnit);

        String vmVCPUUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.VCPU_VALUE);
        assertEquals("MHz", vmVCPUUnit);

        String vmVCPURequestUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.VCPU_REQUEST_VALUE);
        assertEquals("mCores", vmVCPURequestUnit);

        String podVCPUUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.CONTAINER_POD_VALUE, CommodityType.VCPU_VALUE);
        assertEquals("mCores", podVCPUUnit);

        String podVCPULimitQuotaUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.CONTAINER_POD_VALUE, CommodityType.VCPU_LIMIT_QUOTA_VALUE);
        assertEquals("mCores", podVCPULimitQuotaUnit);

        String cntClusterVCPUUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE, CommodityType.VCPU_VALUE);
        assertEquals("mCores", cntClusterVCPUUnit);

        String unKnownCommUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.UNKNOWN_VALUE, CommodityType.UNKNOWN_VALUE);
        assertEquals("", unKnownCommUnit);

        String nullUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.UNKNOWN_VALUE, -1);
        assertNull(nullUnit);
    }
}