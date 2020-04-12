package com.vmturbo.common.protobuf.topology;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Utility class to help map SDK commodity types to their UI/API string equivalents, as well as
 * to provide an intra-XL representation for the types.
 */
public enum UICommodityType {
    /** The commodity ACTION_PERMIT. */
    ACTION_PERMIT("ActionPermit", CommodityType.ACTION_PERMIT, "Action Permit"),
    /** The commodity ACTIVE_SESSIONS. */
    ACTIVE_SESSIONS("ActiveSessions", CommodityType.ACTIVE_SESSIONS, "Active Sessions"),
    /** The commodity APPLICATION. */
    APPLICATION("ApplicationCommodity", CommodityType.APPLICATION, "Application Commodity"),
    /** The commodity BALLOONING. */
    BALLOONING("Ballooning", CommodityType.BALLOONING, "Ballooning"),
    /** The commodity BUFFER_COMMODITY. */
    BUFFER_COMMODITY("BufferCommodity", CommodityType.BUFFER_COMMODITY, "Buffer Commodity"),
    /** The commodity CLUSTER. */
    CLUSTER("ClusterCommodity", CommodityType.CLUSTER, "Cluster Commodity"),
    /** The commodity COLLECTION_TIME. */
    COLLECTION_TIME("CollectionTime", CommodityType.COLLECTION_TIME, "Collection Time"),
    /** The commodity COOLING. */
    COOLING("Cooling", CommodityType.COOLING, "Cooling"),
    /** The commodity CONNECTION. */
    CONNECTION("Connection", CommodityType.CONNECTION, "Connection"),
    /** The commodity COUPON. */
    COUPON("Coupon", CommodityType.COUPON, "Coupon"),
    /** The commodity CPU. */
    CPU("CPU", CommodityType.CPU, "CPU"),
    /** The commodity CPU_ALLOCATION. */
    CPU_ALLOCATION("CPUAllocation", CommodityType.CPU_ALLOCATION, "CPU Allocation"),
    /** The commodity CPU_PROVISIONED. */
    CPU_PROVISIONED("CPUProvisioned", CommodityType.CPU_PROVISIONED, "CPU Provisioned"),
    /** The commodity CPU_REQUEST_ALLOCATION. */
    CPU_REQUEST_ALLOCATION("CPURequestAllocation", CommodityType.CPU_REQUEST_ALLOCATION, "CPU Request Allocation"),
    /** The commodity CROSS_CLOUD_MOVE_SVC. */
    CROSS_CLOUD_MOVE_SVC("CrossCloudMoveSvc", CommodityType.CROSS_CLOUD_MOVE_SVC, "Cross Cloud Move SVC"),
    /** The commodity CROSS_CLUSTER_MOVE_SVC. */
    CROSS_CLUSTER_MOVE_SVC("CrossClusterMoveSvc", CommodityType.CROSS_CLUSTER_MOVE_SVC, "Cross Cluster Move SVC"),
    /** The commodity DATACENTER. */
    DATACENTER("DataCenterCommodity", CommodityType.DATACENTER, "Data Center Commodity"),
    /** The commodity DATASTORE. */
    DATASTORE("DatastoreCommodity", CommodityType.DATASTORE, "Data Store Commodity"),
    /** The commodity DB_CACHE_HIT_RATE. */
    DB_CACHE_HIT_RATE("DBCacheHitRate", CommodityType.DB_CACHE_HIT_RATE, "DB Cache Hit Rate"),
    /** The commodity DB_MEM. */
    DB_MEM("DBMem", CommodityType.DB_MEM, "DB Mem"),
    /** The commodity DISK_ARRAY_ACCESS. */
    DISK_ARRAY_ACCESS("DISK_ARRAY_ACCESS", CommodityType.DISK_ARRAY_ACCESS, "Disk Array Access"),
    /** The commodity DRS_SEGMENTATION. */
    DRS_SEGMENTATION("DrsSegmentationCommodity", CommodityType.DRS_SEGMENTATION, "DRS Segmentation Commodity"),
    /** The commodity DSPM_ACCESS. */
    DSPM_ACCESS("DSPMAccessCommodity", CommodityType.DSPM_ACCESS, "DSPM Access Commodity"),
    /** The commodity EXTENT. */
    EXTENT("Extent", CommodityType.EXTENT, "Extent"),
    /** The commodity FLOW. */
    FLOW("Flow", CommodityType.FLOW, "Flow"),
    /** The commodity FLOW_ALLOCATION. */
    FLOW_ALLOCATION("FlowAllocation", CommodityType.FLOW_ALLOCATION, "Flow Allocation"),
    /** The commodity HEAP. */
    HEAP("Heap", CommodityType.HEAP, "Heap"),
    /** The commodity HOST_LUN_ACCESS. */
    HOST_LUN_ACCESS("HOST_LUN_ACCESS", CommodityType.HOST_LUN_ACCESS, "Host LUN Access"),
    /** The commodity IMAGE_CPU. */
    IMAGE_CPU("ImageCPU", CommodityType.IMAGE_CPU, "Image CPU"),
    /** The commodity IMAGE_MEM. */
    IMAGE_MEM("ImageMem", CommodityType.IMAGE_MEM, "Image Mem"),
    /** The commodity IMAGE_STORAGE. */
    IMAGE_STORAGE("ImageStorage", CommodityType.IMAGE_STORAGE, "Image Storage"),
    /** The commodity INSTANCE_DISK_SIZE. */
    INSTANCE_DISK_SIZE("InstanceDiskSize", CommodityType.INSTANCE_DISK_SIZE, "Instance Disk Size"),
    /** The commodity INSTANCE_DISK_TYPE. */
    INSTANCE_DISK_TYPE("InstanceDiskType", CommodityType.INSTANCE_DISK_TYPE, "Instance Disk Type"),
    /** The commodity IO_THROUGHPUT. */
    IO_THROUGHPUT("IOThroughput", CommodityType.IO_THROUGHPUT, "IO Throughput"),
    /** The commodity LICENSE_ACCESS. */
    LICENSE_ACCESS("LICENSE_ACCESS", CommodityType.LICENSE_ACCESS, "License Access"),
    /** The commodity MEM. */
    MEM("Mem", CommodityType.MEM, "Mem"),
    /** The commodity MEM_ALLOCATION. */
    MEM_ALLOCATION("MemAllocation", CommodityType.MEM_ALLOCATION, "Mem Allocation"),
    /** The commodity MEM_REQUEST_ALLOCATION. */
    MEM_REQUEST_ALLOCATION("MemRequestAllocation", CommodityType.MEM_REQUEST_ALLOCATION, "Mem Request Allocation"),
    /** The commodity MEM_PROVISIONED. */
    MEM_PROVISIONED("MemProvisioned", CommodityType.MEM_PROVISIONED, "Mem Provisioned"),
    /** The commodity NET_THROUGHPUT. */
    NET_THROUGHPUT("NetThroughput", CommodityType.NET_THROUGHPUT, "Net Throughput"),
    /** The commodity NETWORK. */
    NETWORK("NetworkCommodity", CommodityType.NETWORK, "Network Commodity"),
    /** The commodity NUM_DISK. */
    NUM_DISK("NumDisk", CommodityType.NUM_DISK, "Num Disk"),
    /** The commodity NUMBER_COMSUMERS. */
    NUMBER_CONSUMERS("NumberConsumers", CommodityType.NUMBER_CONSUMERS, "Number Consumers"),
    /** The commodity POOL_CPU. */
    POOL_CPU("PoolCPU", CommodityType.POOL_CPU, "Pool CPU"),
    /** The commodity POOL_MEM. */
    POOL_MEM("PoolMem", CommodityType.POOL_MEM, "Pool Mem"),
    /** The commodity POOL_STORAGE. */
    POOL_STORAGE("PoolStorage", CommodityType.POOL_STORAGE, "Pool Storage"),
    /** The commodity PORT_CHANEL. */
    PORT_CHANEL("PORT_CHANNEL", CommodityType.PORT_CHANEL, "Port Channel"),
    /** The commodity POWER. */
    POWER("Power", CommodityType.POWER, "Power"),
    /** The commodity Q16_VCPU. */
    Q16_VCPU("Q16VCPU", CommodityType.Q16_VCPU, "Q16 VCPU"),
    /** The commodity Q1_VCPU. */
    Q1_VCPU("Q1VCPU", CommodityType.Q1_VCPU, "Q1 VCPU"),
    /** The commodity Q2_VCPU. */
    Q2_VCPU("Q2VCPU", CommodityType.Q2_VCPU, "Q2 VCPU"),
    /** The commodity Q32_VCPU. */
    Q32_VCPU("Q32VCPU", CommodityType.Q32_VCPU, "Q32 VCPU"),
    /** The commodity Q4_VCPU. */
    Q4_VCPU("Q4VCPU", CommodityType.Q4_VCPU, "Q4 VCPU"),
    /** The commodity Q64_VCPU. */
    Q64_VCPU("Q64VCPU", CommodityType.Q64_VCPU, "Q64 VCPU"),
    /** The commodity Q8_VCPU. */
    Q8_VCPU("Q8VCPU", CommodityType.Q8_VCPU, "Q8 VCPU"),
    /** The commodity RESPONSE_TIME. */
    RESPONSE_TIME("ResponseTime", CommodityType.RESPONSE_TIME, "Response Time"),
    /** The commodity SAME_CLUSTER_MOVE_SVC. */
    SAME_CLUSTER_MOVE_SVC("SameClusterMoveSvc", CommodityType.SAME_CLUSTER_MOVE_SVC, "Same Cluster Move SVC"),
    /** The commodity SEGMENTATION. */
    SEGMENTATION("SegmentationCommodity", CommodityType.SEGMENTATION, "Segmentation Commodity"),
    /** The commodity SLA_COMMODITY. */
    SLA_COMMODITY("SLACommodity", CommodityType.SLA_COMMODITY, "SLA Commodity"),
    /** The commodity SOFTWARE_LICENSE_COMMODITY. */
    SOFTWARE_LICENSE_COMMODITY("SoftwareLicenseCommodity", CommodityType.SOFTWARE_LICENSE_COMMODITY, "Software License Commodity"),
    /** The commodity SPACE. */
    SPACE("Space", CommodityType.SPACE, "Space"),
    /** The commodity STORAGE_ACCESS. */
    STORAGE_ACCESS("StorageAccess", CommodityType.STORAGE_ACCESS, "Storage Access"),
    /** The commodity STORAGE_ALLOCATION. */
    STORAGE_ALLOCATION("StorageAllocation", CommodityType.STORAGE_ALLOCATION, "Storage Allocation"),
    /** The commodity STORAGE_AMOUNT. */
    STORAGE_AMOUNT("StorageAmount", CommodityType.STORAGE_AMOUNT, "Storage Amount"),
    /** The commodity STORAGE_CLUSTER. */
    STORAGE_CLUSTER("StorageClusterCommodity", CommodityType.STORAGE_CLUSTER, "Storage Cluster Commodity"),
    /** The commodity STORAGE_LATENCY. */
    STORAGE_LATENCY("StorageLatency", CommodityType.STORAGE_LATENCY, "Storage Latency"),
    /** The commodity STORAGE_PROVISIONED. */
    STORAGE_PROVISIONED("StorageProvisioned", CommodityType.STORAGE_PROVISIONED, "Storage Provisioned"),
    /** The commodity SWAPPING. */
    SWAPPING("Swapping", CommodityType.SWAPPING, "Swapping"),
    /** The commodity THREADS. */
    THREADS("Threads", CommodityType.THREADS, "Threads"),
    /** The commodity TRANSACTION. */
    TRANSACTION("Transaction", CommodityType.TRANSACTION, "Transaction"),
    /** The commodity TRANSACTION_LOG. */
    TRANSACTION_LOG("TransactionLog", CommodityType.TRANSACTION_LOG, "Transaction Log"),
    /** The commodity VCPU. */
    VCPU("VCPU", CommodityType.VCPU, "VCPU"),
    /** The commodity VCPU_REQUEST. */
    VCPU_REQUEST("VCPURequest", CommodityType.VCPU_REQUEST, "VCPU Request"),
    /** The commodity VDC. */
    VDC("VDCCommodity", CommodityType.VDC, "VDC Commodity"),
    /** The commodity VMEM. */
    VMEM("VMem", CommodityType.VMEM, "VMem"),
    /** The commodity VMEM_REQUEST. */
    VMEM_REQUEST("VMemRequest", CommodityType.VMEM_REQUEST, "VMem Request"),
    /** The commodity VMPM_ACCESS. */
    VMPM_ACCESS("VMPMAccessCommodity", CommodityType.VMPM_ACCESS, "VMPM Access Commodity"),
    /** The commodity VSTORAGE. */
    VSTORAGE("VStorage", CommodityType.VSTORAGE, "VStorage"),
    /** The commodity ZONE. */
    ZONE("Zone", CommodityType.ZONE, "Zone"),
    /** The commodity LICENSE_COMMODITY. */
    LICENSE_COMMODITY("LicenseCommodity", CommodityType.LICENSE_COMMODITY, "License Commodity"),
    /** The commodity Q3_VCPU. */
    Q3_VCPU("Q3VCPU", CommodityType.Q3_VCPU, "Q3 VCPU"),
    /** The commodity NUMBER_CONSUMERS_PM. */
    NUMBER_CONSUMERS_PM("NumberConsumersPM", CommodityType.NUMBER_CONSUMERS_PM, "Number Consumers PM"),
    /** The commodity Q6_VCPU. */
    Q6_VCPU("Q6VCPU", CommodityType.Q6_VCPU, "Q6 VCPU"),
    /** The commodity Q7_VCPU. */
    Q7_VCPU("Q7VCPU", CommodityType.Q7_VCPU, "Q7 VCPU"),
    /** The commodity QN_VCPU. */
    QN_VCPU("QNVCPU", CommodityType.QN_VCPU, "QN VCPU"),
    /** The commodity RIGHT_SIZE_SVC. */
    RIGHT_SIZE_SVC("RightSizeSVC", CommodityType.RIGHT_SIZE_SVC, "Right Size SVC"),
    /** The commodity RIGHT_SIZE_DOWN. */
    RIGHT_SIZE_DOWN("RightSizeDown", CommodityType.RIGHT_SIZE_DOWN, "Right Size Down"),
    /** The commodity MOVE. */
    MOVE("Move", CommodityType.MOVE, "Move"),
    /** The commodity Q5_VCPU. */
    Q5_VCPU("Q5VCPU", CommodityType.Q5_VCPU, "Q5 VCPU"),
    /** The commodity STORAGE. */
    STORAGE("Storage", CommodityType.STORAGE, "Storage"),
    /** The commodity NUMBER_CONSUMERS_STORAGE. */
    NUMBER_CONSUMERS_STORAGE("NumberConsumersStorage", CommodityType.NUMBER_CONSUMERS_STORAGE, "Number Consumers Storage"),
    /** The commodity ACCESS. */
    ACCESS("Access", CommodityType.ACCESS, "Access"),
    /** The commodity RIGHT_SIZE_UP. */
    RIGHT_SIZE_UP("RightSizeUp", CommodityType.RIGHT_SIZE_UP, "Right Size Up"),
    /** The commodity VAPP_ACCESS. */
    VAPP_ACCESS("VAppAccess", CommodityType.VAPP_ACCESS, "VApp Access"),
    /** The commodity HOT_STORAGE. */
    HOT_STORAGE("HotStorage", CommodityType.HOT_STORAGE, "Hot Storage"),
    /** The commodity HA_COMMODITY. */
    HA_COMMODITY("HACommodity", CommodityType.HA_COMMODITY, "HA Commodity"),
    /** The commodity NETWORK_POLICY. */
    NETWORK_POLICY("NetworkPolicy", CommodityType.NETWORK_POLICY, "Network Policy"),
    /** The commodity SERVICE_LEVEL_CLUSTER. */
    SERVICE_LEVEL_CLUSTER("ServiceLevelCluster", CommodityType.SERVICE_LEVEL_CLUSTER, "Service Level Cluster"),
    /** The commodity PROCESSING_UNITS. */
    PROCESSING_UNITS("ProcessingUnits", CommodityType.PROCESSING_UNITS, "Processing Units"),
    /** The commodity TENANCY_ACCESS. */
    TENANCY_ACCESS("TenancyAccess", CommodityType.TENANCY_ACCESS, "Tenancy Access"),
    /** The commodity TEMPLATE_ACCESS. */
    TEMPLATE_ACCESS("TemplateAccess", CommodityType.TEMPLATE_ACCESS, "Template Access"),
    /** The commodity BURST_BALANCE. */
    BURST_BALANCE("BurstBalance", CommodityType.BURST_BALANCE, "Burst Balance"),
    /** The commodity DESIRED_COUPON. */
    DESIRED_COUPON("DesiredCoupon", CommodityType.DESIRED_COUPON, "Desired Coupon"),
    /** The commodity NETWORK_INTERFACE_COUNT. */
    NETWORK_INTERFACE_COUNT("NetworkInterfaceCount", CommodityType.NETWORK_INTERFACE_COUNT, "Network Interface Count"),
    /** Biclique commodity. **/
    BICLIQUE("Biclique", CommodityType.BICLIQUE, "Biclique"),
    /** KPI commodity. **/
    KPI("KPI", CommodityType.KPI, "KPI"),
    /** The commodity UNKNOWN. */
    UNKNOWN("Unknown", CommodityType.UNKNOWN, "Unknown");

    private final String apiStr;
    private final CommodityType sdkType;
    // Someday we may want to localize the display names, in which case we could call a translation
    // method instead of generating these here.
    private final String displayName;

    UICommodityType(@Nonnull final String apiStr, @Nonnull final CommodityType sdkType,
                    @Nonnull final String displayName) {
        this.apiStr = apiStr;
        this.sdkType = sdkType;
        this.displayName = displayName;
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

    public String displayName() { return displayName; }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    private static final BiMap<Integer, UICommodityType> COMM_TYPE_MAPPINGS;
    private static final BiMap<String, UICommodityType> COMM_STR_MAPPINGS;
    private static final BiMap<String, UICommodityType> COMM_STR_ALL_LOWER_CASE_MAPPINGS;

    static {
        ImmutableBiMap.Builder<Integer, UICommodityType> commTypeMappingBldr = new ImmutableBiMap.Builder<>();
        ImmutableBiMap.Builder<String, UICommodityType> commStrMappingBldr = new ImmutableBiMap.Builder<>();
        ImmutableBiMap.Builder<String, UICommodityType> commStrAllLowerCaseMappingBldr = new ImmutableBiMap.Builder<>();
        for (UICommodityType type : UICommodityType.values()) {
            commTypeMappingBldr.put(type.typeNumber(), type);
            commStrMappingBldr.put(type.apiStr(), type);
            commStrAllLowerCaseMappingBldr.put(type.apiStr.toLowerCase(), type);
        }
        COMM_TYPE_MAPPINGS = commTypeMappingBldr.build();
        COMM_STR_MAPPINGS = commStrMappingBldr.build();
        COMM_STR_ALL_LOWER_CASE_MAPPINGS = commStrAllLowerCaseMappingBldr.build();
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

    /**
     * Ignoring case, converts type from a string to the enum type.
     * @param type string representation of service entity type
     * @return UI entity type enum
     */
    @Nonnull
    public static UICommodityType fromStringIgnoreCase(@Nonnull final String type) {
        return COMM_STR_ALL_LOWER_CASE_MAPPINGS.getOrDefault(type.toLowerCase(), UICommodityType.UNKNOWN);
    }
}
