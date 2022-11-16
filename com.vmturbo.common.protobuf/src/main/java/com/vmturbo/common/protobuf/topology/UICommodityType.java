package com.vmturbo.common.protobuf.topology;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Utility class to help map SDK commodity types to their UI/API string equivalents, as well as
 * to provide an intra-XL representation for the types.
 */
public enum UICommodityType {
    /** The commodity ACTION_PERMIT. */
    ACTION_PERMIT(CommodityType.ACTION_PERMIT, "Action Permit"),
    /** The commodity ACTIVE_SESSIONS. */
    ACTIVE_SESSIONS(CommodityType.ACTIVE_SESSIONS, "Active Sessions"),
    /** The commodity APPLICATION. */
    APPLICATION(CommodityType.APPLICATION, "Application"),
    /** The commodity BALLOONING. */
    BALLOONING(CommodityType.BALLOONING, "Ballooning"),
    /** The commodity BUFFER_COMMODITY. */
    BUFFER_COMMODITY(CommodityType.BUFFER_COMMODITY, "Buffer"),
    /** The commodity CLUSTER. */
    CLUSTER(CommodityType.CLUSTER, "Cluster"),
    /** The commodity COLLECTION_TIME. Use {@link #REMAINING_GC_CAPACITY} instead. */
    @Deprecated
    COLLECTION_TIME(CommodityType.COLLECTION_TIME, "Collection Time"),
    /** The commodity REMAINING_GC_CAPACITY. */
    REMAINING_GC_CAPACITY(CommodityType.REMAINING_GC_CAPACITY, "Remaining GC Capacity"),
    /** The commodity COOLING. */
    COOLING(CommodityType.COOLING, "Cooling"),
    /** The commodity CONNECTION. */
    CONNECTION(CommodityType.CONNECTION, "Connection"),
    /** The commodity COUPON. */
    COUPON(CommodityType.COUPON, "Coupon"),
    /** The commodity CPU. */
    CPU(CommodityType.CPU, "CPU"),
    /** The commodity CPU_ALLOCATION. */
    CPU_ALLOCATION(CommodityType.CPU_ALLOCATION, "CPU Allocation"),
    /** The commodity CPU_PROVISIONED. */
    CPU_PROVISIONED(CommodityType.CPU_PROVISIONED, "CPU Provisioned"),
    /** The commodity CPU_REQUEST_ALLOCATION. */
    CPU_REQUEST_ALLOCATION(CommodityType.CPU_REQUEST_ALLOCATION, "CPU Request Allocation"),
    /** The commodity CROSS_CLOUD_MOVE_SVC. */
    CROSS_CLOUD_MOVE_SVC(CommodityType.CROSS_CLOUD_MOVE_SVC, "Cross Cloud Move SVC"),
    /** The commodity CROSS_CLUSTER_MOVE_SVC. */
    CROSS_CLUSTER_MOVE_SVC(CommodityType.CROSS_CLUSTER_MOVE_SVC, "Cross Cluster Move SVC"),
    /** The commodity DATACENTER. */
    DATACENTER(CommodityType.DATACENTER, "Data Center"),
    /** The commodity DATASTORE. */
    DATASTORE(CommodityType.DATASTORE, "Storage Accessibility"),
    /** The commodity DB_CACHE_HIT_RATE. */
    DB_CACHE_HIT_RATE(CommodityType.DB_CACHE_HIT_RATE, "DB Cache Hit Rate"),
    /** The commodity DB_MEM. */
    DB_MEM(CommodityType.DB_MEM, "DB Mem"),
    /** The commodity DISK_ARRAY_ACCESS. */
    DISK_ARRAY_ACCESS(CommodityType.DISK_ARRAY_ACCESS, "Disk Array Access"),
    /** The commodity DRS_SEGMENTATION. */
    DRS_SEGMENTATION(CommodityType.DRS_SEGMENTATION, "DRS Segmentation"),
    /** The commodity DSPM_ACCESS. */
    DSPM_ACCESS(CommodityType.DSPM_ACCESS, "Host Accessibility"),
    /** The commodity EXTENT. */
    EXTENT(CommodityType.EXTENT, "Extent"),
    /** The commodity FLOW. */
    FLOW(CommodityType.FLOW, "Flow"),
    /** The commodity FLOW_ALLOCATION. */
    FLOW_ALLOCATION(CommodityType.FLOW_ALLOCATION, "Flow Allocation"),
    /** The commodity HEAP. */
    HEAP(CommodityType.HEAP, "Heap"),
    /** The commodity HOST_LUN_ACCESS. */
    HOST_LUN_ACCESS(CommodityType.HOST_LUN_ACCESS, "Host LUN Access"),
    /** The commodity IMAGE_CPU. */
    IMAGE_CPU(CommodityType.IMAGE_CPU, "Image CPU"),
    /** The commodity IMAGE_MEM. */
    IMAGE_MEM(CommodityType.IMAGE_MEM, "Image Mem"),
    /** The commodity IMAGE_STORAGE. */
    IMAGE_STORAGE(CommodityType.IMAGE_STORAGE, "Image Storage"),
    /** The commodity INSTANCE_DISK_SIZE. */
    INSTANCE_DISK_SIZE(CommodityType.INSTANCE_DISK_SIZE, "Instance Disk Size"),
    /** The commodity INSTANCE_DISK_TYPE. */
    INSTANCE_DISK_TYPE(CommodityType.INSTANCE_DISK_TYPE, "Instance Disk Type"),
    /** The commodity INSTANCE_DISK_COUNT. */
    INSTANCE_DISK_COUNT(CommodityType.INSTANCE_DISK_COUNT, "Instance Disk Count"),
    /** The commodity IO_THROUGHPUT. */
    IO_THROUGHPUT(CommodityType.IO_THROUGHPUT, "IO Throughput"),
    /** The commodity LICENSE_ACCESS. */
    LICENSE_ACCESS(CommodityType.LICENSE_ACCESS, "License Access"),
    /** The commodity MEM. */
    MEM(CommodityType.MEM, "Mem"),
    /** The commodity MEM_ALLOCATION. */
    MEM_ALLOCATION(CommodityType.MEM_ALLOCATION, "Mem Allocation"),
    /** The commodity MEM_REQUEST_ALLOCATION. */
    MEM_REQUEST_ALLOCATION(CommodityType.MEM_REQUEST_ALLOCATION, "Mem Request Allocation"),
    /** The commodity MEM_PROVISIONED. */
    MEM_PROVISIONED(CommodityType.MEM_PROVISIONED, "Mem Provisioned"),
    /** The commodity NET_THROUGHPUT. */
    NET_THROUGHPUT(CommodityType.NET_THROUGHPUT, "Net Throughput"),
    /** The commodity NETWORK. */
    NETWORK(CommodityType.NETWORK, "Network"),
    /** The commodity NUM_DISK. */
    NUM_DISK(CommodityType.NUM_DISK, "Num Disk"),
    /** The commodity NUMBER_COMSUMERS. */
    NUMBER_CONSUMERS(CommodityType.NUMBER_CONSUMERS, "Number Consumers"),
    /** The commodity NUM_VCORE. */
    NUM_VCORE(CommodityType.NUM_VCORE, "Num VCore"),
    /** The commodity POOL_CPU. */
    POOL_CPU(CommodityType.POOL_CPU, "Pool CPU"),
    /** The commodity POOL_MEM. */
    POOL_MEM(CommodityType.POOL_MEM, "Pool Mem"),
    /** The commodity POOL_STORAGE. */
    POOL_STORAGE(CommodityType.POOL_STORAGE, "Pool Storage"),
    /** The commodity PORT_CHANEL. */
    PORT_CHANEL(CommodityType.PORT_CHANEL, "Port Channel"),
    /** The commodity POWER. */
    POWER(CommodityType.POWER, "Power"),
    /** The commodity Q16_VCPU. */
    Q16_VCPU(CommodityType.Q16_VCPU, "Q16 VCPU"),
    /** The commodity Q1_VCPU. */
    Q1_VCPU(CommodityType.Q1_VCPU, "Q1 VCPU"),
    /** The commodity Q2_VCPU. */
    Q2_VCPU(CommodityType.Q2_VCPU, "Q2 VCPU"),
    /** The commodity Q32_VCPU. */
    Q32_VCPU(CommodityType.Q32_VCPU, "Q32 VCPU"),
    /** The commodity Q4_VCPU. */
    Q4_VCPU(CommodityType.Q4_VCPU, "Q4 VCPU"),
    /** The commodity Q64_VCPU. */
    Q64_VCPU(CommodityType.Q64_VCPU, "Q64 VCPU"),
    /** The commodity Q8_VCPU. */
    Q8_VCPU(CommodityType.Q8_VCPU, "Q8 VCPU"),
    /** The commodity RESPONSE_TIME. */
    RESPONSE_TIME(CommodityType.RESPONSE_TIME, "Response Time"),
    /** The commodity SAME_CLUSTER_MOVE_SVC. */
    SAME_CLUSTER_MOVE_SVC(CommodityType.SAME_CLUSTER_MOVE_SVC, "Same Cluster Move SVC"),
    /** The commodity SEGMENTATION. */
    SEGMENTATION(CommodityType.SEGMENTATION, "Segmentation"),
    /** The commodity SLA_COMMODITY. */
    SLA_COMMODITY(CommodityType.SLA_COMMODITY, "SLA"),
    /** The commodity SOFTWARE_LICENSE_COMMODITY. */
    SOFTWARE_LICENSE_COMMODITY(CommodityType.SOFTWARE_LICENSE_COMMODITY, "Software License"),
    /** The commodity SPACE. */
    SPACE(CommodityType.SPACE, "Space"),
    /** The commodity STORAGE_ACCESS. */
    STORAGE_ACCESS(CommodityType.STORAGE_ACCESS, "Storage Access"),
    /** The commodity STORAGE_ALLOCATION. */
    STORAGE_ALLOCATION(CommodityType.STORAGE_ALLOCATION, "Storage Allocation"),
    /** The commodity STORAGE_AMOUNT. */
    STORAGE_AMOUNT(CommodityType.STORAGE_AMOUNT, "Storage Amount"),
    /** The commodity STORAGE_CLUSTER. */
    STORAGE_CLUSTER(CommodityType.STORAGE_CLUSTER, "Storage Cluster"),
    /** The commodity STORAGE_LATENCY. */
    STORAGE_LATENCY(CommodityType.STORAGE_LATENCY, "Storage Latency"),
    /** The commodity STORAGE_PROVISIONED. */
    STORAGE_PROVISIONED(CommodityType.STORAGE_PROVISIONED, "Storage Provisioned"),
    /** The commodity SWAPPING. */
    SWAPPING(CommodityType.SWAPPING, "Swapping"),
    /** The commodity THREADS. */
    THREADS(CommodityType.THREADS, "Threads"),
    /** The commodity TRANSACTION. */
    TRANSACTION(CommodityType.TRANSACTION, "Transaction"),
    /** The commodity TRANSACTION_LOG. */
    TRANSACTION_LOG(CommodityType.TRANSACTION_LOG, "Transaction Log"),
    /** The commodity VCPU. */
    VCPU(CommodityType.VCPU, "VCPU"),
    /** The commodity VCPU_LIMIT_QUOTA. */
    VCPU_LIMIT_QUOTA(CommodityType.VCPU_LIMIT_QUOTA, "VCPU Limit Quota"),
    /** The commodity VCPU_REQUEST. */
    VCPU_REQUEST(CommodityType.VCPU_REQUEST, "VCPU Request"),
    /** The commodity VCPU_REQUEST_QUOTA. */
    VCPU_REQUEST_QUOTA(CommodityType.VCPU_REQUEST_QUOTA, "VCPU Request Quota"),
    /** The commodity VDC. */
    VDC(CommodityType.VDC, "VDC"),
    /** The commodity VMEM. */
    VMEM(CommodityType.VMEM, "VMem"),
    /** The commodity VMEM_LIMIT_QUOTA. */
    VMEM_LIMIT_QUOTA(CommodityType.VMEM_LIMIT_QUOTA, "VMem Limit Quota"),
    /** The commodity VMEM_REQUEST. */
    VMEM_REQUEST(CommodityType.VMEM_REQUEST, "VMem Request"),
    /** The commodity VMEM_REQUEST_QUOTA. */
    VMEM_REQUEST_QUOTA(CommodityType.VMEM_REQUEST_QUOTA, "VMem Request Quota"),
    /** The commodity VMPM_ACCESS. */
    VMPM_ACCESS(CommodityType.VMPM_ACCESS, "VMPM Access"),
    /** The commodity VSTORAGE. */
    VSTORAGE(CommodityType.VSTORAGE, "VStorage"),
    /** The commodity ZONE. */
    ZONE(CommodityType.ZONE, "Zone"),
    /** The commodity LICENSE_COMMODITY. */
    LICENSE_COMMODITY(CommodityType.LICENSE_COMMODITY, "License"),
    /** The commodity Q3_VCPU. */
    Q3_VCPU(CommodityType.Q3_VCPU, "Q3 VCPU"),
    /** The commodity NUMBER_CONSUMERS_PM. */
    NUMBER_CONSUMERS_PM(CommodityType.NUMBER_CONSUMERS_PM, "Number Consumers PM"),
    /** The commodity Q6_VCPU. */
    Q6_VCPU(CommodityType.Q6_VCPU, "Q6 VCPU"),
    /** The commodity Q7_VCPU. */
    Q7_VCPU(CommodityType.Q7_VCPU, "Q7 VCPU"),
    /** The commodity QN_VCPU. */
    QN_VCPU(CommodityType.QN_VCPU, "QN VCPU"),
    /** The commodity RIGHT_SIZE_SVC. */
    RIGHT_SIZE_SVC(CommodityType.RIGHT_SIZE_SVC, "Right Size SVC"),
    /** The commodity RIGHT_SIZE_DOWN. */
    RIGHT_SIZE_DOWN(CommodityType.RIGHT_SIZE_DOWN, "Right Size Down"),
    /** The commodity MOVE. */
    MOVE(CommodityType.MOVE, "Move"),
    /** The commodity Q5_VCPU. */
    Q5_VCPU(CommodityType.Q5_VCPU, "Q5 VCPU"),
    /** The commodity STORAGE. */
    STORAGE(CommodityType.STORAGE, "Storage"),
    /** The commodity NUMBER_CONSUMERS_STORAGE. */
    NUMBER_CONSUMERS_STORAGE(CommodityType.NUMBER_CONSUMERS_STORAGE, "Number Consumers Storage"),
    /** The commodity ACCESS. */
    ACCESS(CommodityType.ACCESS, "Access"),
    /** The commodity RIGHT_SIZE_UP. */
    RIGHT_SIZE_UP(CommodityType.RIGHT_SIZE_UP, "Right Size Up"),
    /** The commodity VAPP_ACCESS. */
    VAPP_ACCESS(CommodityType.VAPP_ACCESS, "VApp Access"),
    /** The commodity HOT_STORAGE. */
    HOT_STORAGE(CommodityType.HOT_STORAGE, "Hot Storage"),
    /** The commodity HA_COMMODITY. */
    HA_COMMODITY(CommodityType.HA_COMMODITY, "HA"),
    /** The commodity NETWORK_POLICY. */
    NETWORK_POLICY(CommodityType.NETWORK_POLICY, "Network Policy"),
    /** The commodity SERVICE_LEVEL_CLUSTER. */
    SERVICE_LEVEL_CLUSTER(CommodityType.SERVICE_LEVEL_CLUSTER, "Service Level Cluster"),
    /** The commodity PROCESSING_UNITS. */
    PROCESSING_UNITS(CommodityType.PROCESSING_UNITS, "Processing Units"),
    /** The commodity PROCESSING_UNITS_PROVISIONED. */
    PROCESSING_UNITS_PROVISIONED(CommodityType.PROCESSING_UNITS_PROVISIONED, "Processing Units Provisioned"),
    /** The commodity TENANCY_ACCESS. */
    TENANCY_ACCESS(CommodityType.TENANCY_ACCESS, "Tenancy Access"),
    /** The commodity TEMPLATE_ACCESS. */
    TEMPLATE_ACCESS(CommodityType.TEMPLATE_ACCESS, "Template Access"),
    /** The commodity BURST_BALANCE. */
    BURST_BALANCE(CommodityType.BURST_BALANCE, "Burst Balance"),
    /** The commodity DESIRED_COUPON. */
    DESIRED_COUPON(CommodityType.DESIRED_COUPON, "Desired Coupon"),
    /** The commodity NETWORK_INTERFACE_COUNT. */
    NETWORK_INTERFACE_COUNT(CommodityType.NETWORK_INTERFACE_COUNT, "Network Interface Count"),
    /** Biclique commodity. **/
    BICLIQUE(CommodityType.BICLIQUE, "Biclique"),
    /** KPI commodity. **/
    KPI(CommodityType.KPI, "KPI"),
    /** The commodity TOTAL_SESSIONS. */
    TOTAL_SESSIONS(CommodityType.TOTAL_SESSIONS, "Total Sessions"),
    /** Database Transaction Unit **/
    DTU(CommodityType.DTU, "DTU"),
    /** Concurrent Sessions refers to the number of concurrent connections allowed to a SQL database at a time. **/
    CONCURRENT_SESSION(CommodityType.CONCURRENT_SESSION, "Concurrent Session"),
    /** Concurrent Workers can be thought of as the concurrent processes in the SQL database that are processing queries. **/
    CONCURRENT_WORKER(CommodityType.CONCURRENT_WORKER, "Concurrent Worker"),
    /** The commodity NUMBER_REPLICAS **/
    NUMBER_REPLICAS(CommodityType.NUMBER_REPLICAS, "Number Replicas"),
    /** The commodity VCPU_THROTTLING **/
    VCPU_THROTTLING(CommodityType.VCPU_THROTTLING, "VCPU Throttling"),
    /** The commodity CPU_READY **/
    CPU_READY(CommodityType.CPU_READY, "CPU READY"),
    /** The commodity NET_THROUGHPUT_OUT **/
    NET_THROUGHPUT_OUT(CommodityType.NET_THROUGHPUT_OUT, "Net Throughput Outbound"),
    /** The commodity NET_THROUGHPUT_IN **/
    NET_THROUGHPUT_IN(CommodityType.NET_THROUGHPUT_IN, "Net Throughput Inbound"),
    /** The commodity STORAGE_ACCESS_SSD_READ. */
    STORAGE_ACCESS_SSD_READ(CommodityType.STORAGE_ACCESS_SSD_READ, "IOPS SSD Read"),
    /** The commodity STORAGE_ACCESS_SSD_WRITE. */
    STORAGE_ACCESS_SSD_WRITE(CommodityType.STORAGE_ACCESS_SSD_WRITE, "IOPS SSD Write"),
    /** The commodity STORAGE_ACCESS_STANDARD_READ. */
    STORAGE_ACCESS_STANDARD_READ(CommodityType.STORAGE_ACCESS_STANDARD_READ, "IOPS Standard Read"),
    /** The commodity STORAGE_ACCESS_STANDARD_WRITE. */
    STORAGE_ACCESS_STANDARD_WRITE(CommodityType.STORAGE_ACCESS_STANDARD_WRITE, "IOPS Standard Write"),
    /** The commodity IO_THROUGHPUT_READ **/
    IO_THROUGHPUT_READ(CommodityType.IO_THROUGHPUT_READ, "IO Throughput Read"),
    /** The commodity IO_THROUGHPUT_WRITE **/
    IO_THROUGHPUT_WRITE(CommodityType.IO_THROUGHPUT_WRITE, "IO Throughput Write"),
    /** The commodity TAINT **/
    TAINT(CommodityType.TAINT, "Kubernetes Taint"),
    /** The commodity LABEL */
    LABEL(CommodityType.LABEL, "Kubernetes Label"),
    /** The commodity HARDWARE_VERSION **/
    HARDWARE_VERSION(CommodityType.HARDWARE_VERSION, "Hardware Version"),
    /** The commodity ENERGY. */
    ENERGY(CommodityType.ENERGY, "Energy"),
    /** The commodity UNKNOWN. */
    UNKNOWN(CommodityType.UNKNOWN, "Unknown");

    private final CommodityType sdkType;
    // Someday we may want to localize the display names, in which case we could call a translation
    // method instead of generating these here.
    private final String displayName;

    UICommodityType(@Nonnull final CommodityType sdkType,
                    @Nonnull final String displayName) {
        this.sdkType = sdkType;
        this.displayName = displayName;
    }

    @Nonnull
    public String apiStr() {
        return CommodityTypeMapping.getApiCommodityType(sdkType);
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
            commStrAllLowerCaseMappingBldr.put(type.apiStr().toLowerCase(), type);
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
