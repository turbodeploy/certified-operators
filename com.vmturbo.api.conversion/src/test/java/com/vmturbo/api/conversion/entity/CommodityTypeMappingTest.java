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

        String vmNetThroughputOutUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.NET_THROUGHPUT_OUT_VALUE);
        assertEquals("KByte/sec", vmNetThroughputOutUnit);

        String vmNetThroughputInUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.NET_THROUGHPUT_IN_VALUE);
        assertEquals("KByte/sec", vmNetThroughputInUnit);

        String vmIOPSSSDReadUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.STORAGE_ACCESS_SSD_READ_VALUE);
        assertEquals("IOPS", vmIOPSSSDReadUnit);

        String vmIOPSSSDWriteUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.STORAGE_ACCESS_SSD_WRITE_VALUE);
        assertEquals("IOPS", vmIOPSSSDWriteUnit);

        String vmIOPSStandardReadUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.STORAGE_ACCESS_STANDARD_READ_VALUE);
        assertEquals("IOPS", vmIOPSStandardReadUnit);

        String vmIOPSStandardWriteUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
                EntityType.VIRTUAL_MACHINE_VALUE, CommodityType.STORAGE_ACCESS_STANDARD_WRITE_VALUE);
        assertEquals("IOPS", vmIOPSStandardWriteUnit);


        String unKnownCommUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.UNKNOWN_VALUE, CommodityType.UNKNOWN_VALUE);
        assertEquals("", unKnownCommUnit);

        String nullUnit = CommodityTypeMapping.getUnitForEntityCommodityType(
            EntityType.UNKNOWN_VALUE, -1);
        assertNull(nullUnit);
    }

    /**
     * A unit test to test getApiCommodityTypeForVMComputeCommodities for most commonly seen VM
     * compute resources.
     */
    @Test
    public void testGetApiCommodityTypeForVMComputeCommodities() {
        assertEquals("VCPU", CommodityTypeMapping.getApiCommodityType(CommodityType.VCPU));
        assertEquals("CPU", CommodityTypeMapping.getApiCommodityType(CommodityType.CPU));
        assertEquals("CPUProvisioned", CommodityTypeMapping.getApiCommodityType(CommodityType.CPU_PROVISIONED));
        assertEquals("VMem", CommodityTypeMapping.getApiCommodityType(CommodityType.VMEM));
        assertEquals("Mem", CommodityTypeMapping.getApiCommodityType(CommodityType.MEM));
        assertEquals("MemProvisioned", CommodityTypeMapping.getApiCommodityType(CommodityType.MEM_PROVISIONED));
        assertEquals("IOThroughput", CommodityTypeMapping.getApiCommodityType(CommodityType.IO_THROUGHPUT));
        assertEquals("NetThroughput", CommodityTypeMapping.getApiCommodityType(CommodityType.NET_THROUGHPUT));
        assertEquals("NetThroughputInbound", CommodityTypeMapping.getApiCommodityType(CommodityType.NET_THROUGHPUT_IN));
        assertEquals("NetThroughputOutbound", CommodityTypeMapping.getApiCommodityType(CommodityType.NET_THROUGHPUT_OUT));
        assertEquals("StorageAccess", CommodityTypeMapping.getApiCommodityType(CommodityType.STORAGE_ACCESS));
        assertEquals("IopsSSDRead", CommodityTypeMapping.getApiCommodityType(CommodityType.STORAGE_ACCESS_SSD_READ));
        assertEquals("IopsSSDWrite", CommodityTypeMapping.getApiCommodityType(CommodityType.STORAGE_ACCESS_SSD_WRITE));
        assertEquals("IopsStandardRead", CommodityTypeMapping.getApiCommodityType(CommodityType.STORAGE_ACCESS_STANDARD_READ));
        assertEquals("IopsStandardRead", CommodityTypeMapping.getApiCommodityType(CommodityType.STORAGE_ACCESS_STANDARD_READ));
    }
}