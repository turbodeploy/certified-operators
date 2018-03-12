package com.vmturbo.history.schema;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Map from all upcase commodity types to mixed case used in DB
 **/
public  enum CommodityTypes {
    BALLOONING("Ballooning", "KB"),
    COOLING("Cooling", "C"),
    CPU("CPU", "MHz"),
    CPU_ALLOCATION("CPUAllocation", "MHz"),
    CPU_PROVISIONED("CPUProvisioned", "MHz"),
    EXTENT("Extent", ""),
    FLOW("Flow", "Bytes"),
    FLOWALLOCATION("FlowAllocation", "Bytes"),
    IO_THROUGHPUT("IOThroughput", "KByte/sec"),
    MEM("Mem", "KB"),
    MEM_ALLOCATION("MemAllocation", "KB"),
    MEM_PROVISIONED("MemProvisioned", "KB"),
    NET_THROUGHPUT("NetThroughput", "KByte/sec"),
    NUM_CPUS("numCPUs", ""),
    NUM_SOCKETS("numSockets", ""),
    NUM_CORES("numCores", ""),
    NUM_VCPUS("numVCPUs", ""),
    POWER("Power", "W"),
    PRODUCES("Produces", ""),
    SPACE("Space", ""),
    STORAGE_AMOUNT("StorageAmount", "MB"),
    STORAGE_PROVISIONED("StorageProvisioned", "MB"),
    STORAGE_ACCESS("StorageAccess", "IOPS"),
    STORAGE_LATENCY("StorageLatency", "msec"),
    Q1VCPU("Q1VCPU", "msec"),
    Q2VCPU("Q2VCPU", "msec"),
    Q4VCPU("Q4VCPU", "msec"),
    Q8VCPU("Q8VCPU", "msec"),
    STORAGEALLOCATION("StorageAllocation", "MB"),
    SLACOMMODITY("SLACommodity", ""),
    SWAPPING("Swapping", "Byte/sec"),
    TRANSACTION("Transaction", "TPS"),
    VCPU("VCPU", "MHz"),
    VCPUALLOCATION("VCPUAllocation", "MHz"),
    VMEM("VMem", "KB"),
    VMEMALLOCATION("VMemAllocation", "MB"),
    VSTORAGE("VStorage", "MB"),
    // Access Commodities
    CLUSTER("ClusterCommodity", ""),
    DATASTORE("DatastoreCommodity", ""),
    NETWORK("NetworkCommodity", ""),
    SEGMENTATION("SegmentationCommodity", ""),
    DATACENTER("DataCenterCommodity", ""),
    DSPM_ACCESS("DSPMAccessCommodity", ""),
    APPLICATION("ApplicationCommodity", ""),
    DRS_SEGMENTATION("DrsSegmentationCommodity", ""),
    STORAGE_CLUSTER("StorageClusterCommodity", ""),
    VAPP_ACCESS("VAppAccessCommodity", ""),
    VDC("VDCCommodity", ""),
    VMPM_ACCESS("VMPMAccessCommodity", ""),
    // End of Access Commodities
    UNKNOWN("Unknown", "");


    private final String mixedCase;
    private final String units;

    private CommodityTypes(String mixedCase, String units) {
        this.mixedCase = mixedCase;
        this.units = units;
    }

    public String getMixedCase() {
        return mixedCase;
    }

    public String getUnits() {
        return units;
    }

    public static CommodityTypes fromString(String mixedCaseName) {
        return COMMODITY_TYPE_MAP.get(mixedCaseName);
    }

    static final Map<String, CommodityTypes> COMMODITY_TYPE_MAP;

    static {
        ImmutableMap.Builder<String, CommodityTypes> commodityTypeMapBuilder =
                new ImmutableMap.Builder();
        for (CommodityTypes t : CommodityTypes.values()) {
            commodityTypeMapBuilder.put(t.getMixedCase(), t);
        }
        COMMODITY_TYPE_MAP = commodityTypeMapBuilder.build();
    }
}
