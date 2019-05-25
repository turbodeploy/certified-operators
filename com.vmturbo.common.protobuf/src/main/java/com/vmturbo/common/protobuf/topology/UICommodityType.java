package com.vmturbo.common.protobuf.topology;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public enum UICommodityType {
    ACTION_PERMIT("ActionPermit", CommodityType.ACTION_PERMIT),
    APPLICATION("ApplicationCommodity", CommodityType.APPLICATION),
    BALLOONING("Ballooning", CommodityType.BALLOONING),
    BUFFER_COMMODITY("BufferCommodity", CommodityType.BUFFER_COMMODITY),
    CLUSTER("ClusterCommodity", CommodityType.CLUSTER),
    COLLECTION_TIME("CollectionTime", CommodityType.COLLECTION_TIME),
    COOLING("Cooling", CommodityType.COOLING),
    CONNECTION("Connection", CommodityType.CONNECTION),
    CPU("CPU", CommodityType.CPU),
    CPU_ALLOCATION("CPUAllocation", CommodityType.CPU_ALLOCATION),
    CPU_PROVISIONED("CPUProvisioned", CommodityType.CPU_PROVISIONED),
    CPU_REQUEST_ALLOCATION("CPURequestAllocation", CommodityType.CPU_REQUEST_ALLOCATION),
    CROSS_CLOUD_MOVE_SVC("CrossCloudMoveSvc", CommodityType.CROSS_CLOUD_MOVE_SVC),
    CROSS_CLUSTER_MOVE_SVC("CrossClusterMoveSvc", CommodityType.CROSS_CLUSTER_MOVE_SVC),
    DATACENTER("DataCenterCommodity", CommodityType.DATACENTER),
    DATASTORE("DatastoreCommodity", CommodityType.DATASTORE),
    DB_CACHE_HIT_RATE("DBCacheHitRate", CommodityType.DB_CACHE_HIT_RATE),
    DB_MEM("DBMem", CommodityType.DB_MEM),
    DISK_ARRAY_ACCESS("DISK_ARRAY_ACCESS", CommodityType.DISK_ARRAY_ACCESS),
    DRS_SEGMENTATION("DrsSegmentationCommodity", CommodityType.DRS_SEGMENTATION),
    DSPM_ACCESS("DSPMAccessCommodity", CommodityType.DSPM_ACCESS),
    EXTENT("Extent", CommodityType.EXTENT),
    FLOW("Flow", CommodityType.FLOW),
    FLOW_ALLOCATION("FlowAllocation", CommodityType.FLOW_ALLOCATION),
    HEAP("Heap", CommodityType.HEAP),
    HOST_LUN_ACCESS("HOST_LUN_ACCESS", CommodityType.HOST_LUN_ACCESS),
    IO_THROUGHPUT("IOThroughput", CommodityType.IO_THROUGHPUT),
    LICENSE_ACCESS("LICENSE_ACCESS", CommodityType.LICENSE_ACCESS),
    MEM("Mem", CommodityType.MEM),
    MEM_ALLOCATION("MemAllocation", CommodityType.MEM_ALLOCATION),
    MEM_REQUEST_ALLOCATION("MemRequestAllocation", CommodityType.MEM_REQUEST_ALLOCATION),
    MEM_PROVISIONED("MemProvisioned", CommodityType.MEM_PROVISIONED),
    NET_THROUGHPUT("NetThroughput", CommodityType.NET_THROUGHPUT),
    NETWORK("NetworkCommodity", CommodityType.NETWORK),
    PORT_CHANEL("PORT_CHANEL", CommodityType.PORT_CHANEL),
    POWER("Power", CommodityType.POWER),
    Q16_VCPU("Q16VCPU", CommodityType.Q16_VCPU),
    Q1_VCPU("Q1VCPU", CommodityType.Q1_VCPU),
    Q2_VCPU("Q2VCPU", CommodityType.Q2_VCPU),
    Q32_VCPU("Q32VCPU", CommodityType.Q32_VCPU),
    Q4_VCPU("Q4VCPU", CommodityType.Q4_VCPU),
    Q64_VCPU("Q64VCPU", CommodityType.Q64_VCPU),
    Q8_VCPU("Q8VCPU", CommodityType.Q8_VCPU),
    RESPONSE_TIME("ResponseTime", CommodityType.RESPONSE_TIME),
    SAME_CLUSTER_MOVE_SVC("SameClusterMoveSvc", CommodityType.SAME_CLUSTER_MOVE_SVC),
    SEGMENTATION("SegmentationCommodity", CommodityType.SEGMENTATION),
    SLA_COMMODITY("SLACommodity", CommodityType.SLA_COMMODITY),
    SOFTWARE_LICENSE_COMMODITY("SoftwareLicenseCommodity", CommodityType.SOFTWARE_LICENSE_COMMODITY),
    SPACE("Space", CommodityType.SPACE),
    STORAGE_ACCESS("StorageAccess", CommodityType.STORAGE_ACCESS),
    STORAGE_ALLOCATION("StorageAllocation", CommodityType.STORAGE_ALLOCATION),
    STORAGE_AMOUNT("StorageAmount", CommodityType.STORAGE_AMOUNT),
    STORAGE_CLUSTER("StorageClusterCommodity", CommodityType.STORAGE_CLUSTER),
    STORAGE_LATENCY("StorageLatency", CommodityType.STORAGE_LATENCY),
    STORAGE_PROVISIONED("StorageProvisioned", CommodityType.STORAGE_PROVISIONED),
    SWAPPING("Swapping", CommodityType.SWAPPING),
    THREADS("Threads", CommodityType.THREADS),
    TRANSACTION("Transaction", CommodityType.TRANSACTION),
    TRANSACTION_LOG("TransactionLog", CommodityType.TRANSACTION_LOG),
    VCPU("VCPU", CommodityType.VCPU),
    VCPU_REQUEST("VCPURequest", CommodityType.VCPU_REQUEST),
    VDC("VDCCommodity", CommodityType.VDC),
    VMEM("VMem", CommodityType.VMEM),
    VMEM_REQUEST("VMemRequest", CommodityType.VMEM_REQUEST),
    VSTORAGE("VStorage", CommodityType.VSTORAGE),
    UNKNOWN("Unknown", CommodityType.UNKNOWN);

    private final String apiStr;
    private final CommodityType sdkType;

    UICommodityType(@Nonnull final String apiStr, @Nonnull final CommodityType sdkType) {
        this.apiStr = apiStr;
        this.sdkType = sdkType;
    }

    @Nonnull
    public String apiStr() {
        return apiStr;
    }

    @Nonnull
    public CommodityType sdkType() {
        return sdkType;
    }

    public int typeNumber() {
        return sdkType.getNumber();
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    private static final BiMap<Integer, UICommodityType> COMM_TYPE_MAPPINGS;
    private static final BiMap<String, UICommodityType> COMM_STR_MAPPINGS;

    static {
        ImmutableBiMap.Builder<Integer, UICommodityType> commTypeMappingBldr = new ImmutableBiMap.Builder<>();
        ImmutableBiMap.Builder<String, UICommodityType> commStrMappingBldr = new ImmutableBiMap.Builder<>();
        for (UICommodityType type : UICommodityType.values()) {
            commTypeMappingBldr.put(type.typeNumber(), type);
            commStrMappingBldr.put(type.apiStr(), type);
        }
        COMM_TYPE_MAPPINGS = commTypeMappingBldr.build();
        COMM_STR_MAPPINGS = commStrMappingBldr.build();
    }

    /**
     * @param type The commodity's type in TopologyEntityDTO.
     * @return     The corresponding {@link UICommodityType}.
     */
    @Nonnull
    public static UICommodityType fromType(final int type) {
        return COMM_TYPE_MAPPINGS.getOrDefault(type, UICommodityType.UNKNOWN);
    }

    @Nonnull
    public static UICommodityType fromType(@Nonnull final TopologyDTO.CommodityType type) {
        return fromType(type.getType());
    }

    @Nonnull
    public static UICommodityType fromEntity(@Nonnull final TopologyEntityDTOOrBuilder entity) {
        return fromType(entity.getEntityType());
    }

    /**
     * Converts type from a string to the enum type.
     * @param type string representation of service entity type
     * @return UI entity type enum
     */
    @Nonnull
    public static UICommodityType fromString(@Nonnull final String type) {
        return COMM_STR_MAPPINGS.getOrDefault(type, UICommodityType.UNKNOWN);
    }
}
