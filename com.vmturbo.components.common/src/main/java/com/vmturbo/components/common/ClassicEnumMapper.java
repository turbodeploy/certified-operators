package com.vmturbo.components.common;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;

/**
 * Maps values from the classic OpsManager system to values in the SDK.
 */
public class ClassicEnumMapper {
    private static final Logger logger = LogManager.getLogger();

    public static CommodityType commodityType(@Nonnull final String classicCommodityTypeName) {
        final CommodityType commodityType = COMMODITY_TYPE_MAPPINGS.get(classicCommodityTypeName);
        if (commodityType == null) {
            throw new IllegalArgumentException("No mapping for commodityType " + classicCommodityTypeName);
        }
        return commodityType;
    }

    public static PowerState powerState(@Nullable final String classicPowerStateName) {
        if (classicPowerStateName == null) {
            return null;
        }

        final PowerState powerState = POWER_STATE_MAPPINGS.get(classicPowerStateName);
        if (powerState == null) {
            logger.info("Missing power type information for: " + classicPowerStateName);
        }

        return powerState;
    }

    public static String getCommodityString(@Nonnull final CommodityType classicCommodityTypeName){
        String commodityString = COMMODITY_TYPE_MAPPINGS.inverse().get(classicCommodityTypeName);
        if (commodityString == null) {
            throw new IllegalArgumentException("No mapping for commodityType "
                    + classicCommodityTypeName);
        }
        return commodityString;
    }

    /**
     * Mappings between commodityType enum values in SDK DTO's to strings that are stored
     * in Classic OpsManager topology files.
     */
    public static final ImmutableBiMap<String, CommodityType> COMMODITY_TYPE_MAPPINGS =
        new ImmutableBiMap.Builder<String, CommodityType>()
            .put("ActionPermit",                CommodityType.ACTION_PERMIT)
            .put("ApplicationCommodity",        CommodityType.APPLICATION)
            .put("Ballooning",                  CommodityType.BALLOONING)
            .put("BufferCommodity",             CommodityType.BUFFER_COMMODITY)
            .put("ClusterCommodity",            CommodityType.CLUSTER)
            .put("CollectionTime",              CommodityType.REMAINING_GC_CAPACITY)
            .put("Cooling",                     CommodityType.COOLING)
            .put("Connection",                  CommodityType.CONNECTION)
            .put("CPU",                         CommodityType.CPU)
            .put("CPUAllocation",               CommodityType.CPU_ALLOCATION)
            .put("CPUProvisioned",              CommodityType.CPU_PROVISIONED)
            .put("CPURequestAllocation",        CommodityType.CPU_REQUEST_ALLOCATION)
            .put("CrossCloudMoveSvc",           CommodityType.CROSS_CLOUD_MOVE_SVC)
            .put("CrossClusterMoveSvc",         CommodityType.CROSS_CLUSTER_MOVE_SVC)
            .put("DataCenterCommodity",         CommodityType.DATACENTER)
            .put("DatastoreCommodity",          CommodityType.DATASTORE)
            .put("DBCacheHitRate",              CommodityType.DB_CACHE_HIT_RATE)
            .put("DBMem",                       CommodityType.DB_MEM)
            .put("DISK_ARRAY_ACCESS",           CommodityType.DISK_ARRAY_ACCESS)
            .put("DrsSegmentationCommodity",    CommodityType.DRS_SEGMENTATION)
            .put("DSPMAccessCommodity",         CommodityType.DSPM_ACCESS)
            .put("Extent",                      CommodityType.EXTENT)
            .put("Flow",                        CommodityType.FLOW)
            .put("FlowAllocation",              CommodityType.FLOW_ALLOCATION)
            .put("Heap",                        CommodityType.HEAP)
            .put("HOST_LUN_ACCESS",             CommodityType.HOST_LUN_ACCESS)
            .put("IOThroughput",                CommodityType.IO_THROUGHPUT)
            .put("LICENSE_ACCESS",              CommodityType.LICENSE_ACCESS)
            .put("Mem",                         CommodityType.MEM)
            .put("MemAllocation",               CommodityType.MEM_ALLOCATION)
            .put("MemProvisioned",              CommodityType.MEM_PROVISIONED)
            .put("MemRequestAllocation",        CommodityType.MEM_REQUEST_ALLOCATION)
            .put("NetThroughput",               CommodityType.NET_THROUGHPUT)
            .put("NetworkCommodity",            CommodityType.NETWORK)
            .put("NumberConsumers",             CommodityType.NUMBER_CONSUMERS)
            .put("PortChannel",                 CommodityType.PORT_CHANEL)
            .put("Power",                       CommodityType.POWER)
            .put("Q16VCPU",                     CommodityType.Q16_VCPU)
            .put("Q1VCPU",                      CommodityType.Q1_VCPU)
            .put("Q2VCPU",                      CommodityType.Q2_VCPU)
            .put("Q32VCPU",                     CommodityType.Q32_VCPU)
            .put("Q4VCPU",                      CommodityType.Q4_VCPU)
            .put("Q64VCPU",                     CommodityType.Q64_VCPU)
            .put("Q8VCPU",                      CommodityType.Q8_VCPU)
            .put("ResponseTime",                CommodityType.RESPONSE_TIME)
            .put("SameClusterMoveSvc",          CommodityType.SAME_CLUSTER_MOVE_SVC)
            .put("SegmentationCommodity",       CommodityType.SEGMENTATION)
            .put("SLACommodity",                CommodityType.SLA_COMMODITY)
            .put("SoftwareLicenseCommodity",    CommodityType.SOFTWARE_LICENSE_COMMODITY)
            .put("Space",                       CommodityType.SPACE)
            .put("StorageAccess",               CommodityType.STORAGE_ACCESS)
            .put("StorageAllocation",           CommodityType.STORAGE_ALLOCATION)
            .put("StorageAmount",               CommodityType.STORAGE_AMOUNT)
            .put("StorageClusterCommodity",     CommodityType.STORAGE_CLUSTER)
            .put("StorageLatency",              CommodityType.STORAGE_LATENCY)
            .put("StorageProvisioned",          CommodityType.STORAGE_PROVISIONED)
            .put("Swapping",                    CommodityType.SWAPPING)
            .put("TenancyAccess",               CommodityType.TENANCY_ACCESS)
            .put("Threads",                     CommodityType.THREADS)
            .put("Transaction",                 CommodityType.TRANSACTION)
            .put("TransactionLog",              CommodityType.TRANSACTION_LOG)
            .put("VCPU",                        CommodityType.VCPU)
            .put("VCPULimitQuota",              CommodityType.VCPU_LIMIT_QUOTA)
            .put("VCPURequest",                 CommodityType.VCPU_REQUEST)
            .put("VCPURequestQuota",            CommodityType.VCPU_REQUEST_QUOTA)
            .put("VDCCommodity",                CommodityType.VDC)
            .put("VMem",                        CommodityType.VMEM)
            .put("VMemLimitQuota",              CommodityType.VMEM_LIMIT_QUOTA)
            .put("VMemRequest",                 CommodityType.VMEM_REQUEST)
            .put("VMemRequestQuota",            CommodityType.VMEM_REQUEST_QUOTA)
            .put("VMPMAccessCommodity",         CommodityType.VMPM_ACCESS)
            .put("VStorage",                    CommodityType.VSTORAGE)
            .put("Unknown",                     CommodityType.UNKNOWN)
            .put("Zone",                        CommodityType.ZONE)
            .put("KPI",                         CommodityType.KPI)
            .put("DTU",                         CommodityType.DTU)
            .put("ConcurrentSession",           CommodityType.CONCURRENT_SESSION)
            .put("ConcurrentWorker",            CommodityType.CONCURRENT_WORKER)
            .build();

    /**
     * Mappings between PowerState enum values in SDK DTO's to strings that are stored
     * in Classic OpsManager topology files.
     *
     * TODO: This is not a very good mapping. MAINTENANCE and FAILOVER should actually be
     * PMState fields in the SDK parlance.
     */
    private static final ImmutableMap<String, PowerState> POWER_STATE_MAPPINGS =
        new ImmutableMap.Builder<String, PowerState>()
            .put("ACTIVE",                  PowerState.POWERED_ON)
            .put("RUNNING",                 PowerState.POWERED_ON)
            .put("IDLE",                    PowerState.POWERED_OFF)
            .put("RESOURCE_ALLOCATION",     PowerState.SUSPENDED)
            .put("SUSPEND",                 PowerState.SUSPENDED)
            .put("SUSPEND_PENDING",         PowerState.SUSPENDED)
            .put("MAINTENANCE",             PowerState.SUSPENDED)
            .put("FAILOVER",                PowerState.SUSPENDED)
            .put("UNKNOWN",                 PowerState.POWERSTATE_UNKNOWN)
            .build();


    /**
     * Map from all upcase commodity types to mixed case and associated units.
     * Note that this is defined in opsmgr/com.vmturbo.reports.db but never used (AFAIK).
     * We didn't want to include the 'db' project here.
     **/
    public enum CommodityTypeUnits {
        ACCESS("Access", ""),
        ACTION_PERMIT("ActionPermit", ""),
        ACTIVE_SESSIONS("ActiveSessions", ""),
        BALLOONING("Ballooning", "KB"),
        BICLIQUE("Biclique", ""),
        BUFFER_COMMODITY("BufferCommodity", ""),
        BURST_BALANCE("BurstBalance", ""),
        COLLECTION_TIME("CollectionTime", "%"),
        REMAINING_GC_CAPACITY("RemainingGcCapacity", "%"),
        CONNECTION("Connection", "Connections"),
        COOLING("Cooling", "C"),
        COUPON("Coupon", ""),
        CPU("CPU", "MHz"),
        CPU_ALLOCATION("CPUAllocation", "MHz"),
        CPU_REQUEST_ALLOCATION("CPURequestAllocation", "MHz"),
        CPU_HEADROOM("CPUHeadroom", "VM"),
        CPU_EXHAUSTION("CPUExhaustion", "Day"),
        CPU_PROVISIONED("CPUProvisioned", "MHz"),
        CROSS_CLOUD_MOVE_SVC("CrossCloudMoveSVC", ""),
        CROSS_CLUSTER_MOVE_SVC("CrossClusterMoveSVC", ""),
        DB_CACHE_HIT_RATE("DBCacheHitRate", "%"),
        DB_MEM("DBMem", "KB"),
        DISK_ARRAY_ACCESS("DiskArrayAccess", ""),
        DESIRED_COUPON("DesiredCoupon", ""),
        EXTENT("Extent", ""),
        FLOW("Flow", "KByte/sec"),
        FLOW_ALLOCATION("FlowAllocation", "Bytes"),
        HA_COMMODITY("HACommodity", ""),
        HEAP("Heap", "KB"),
        HOT_STORAGE("HotStorage", ""),
        IMAGE_CPU("ImageCPU", "MHz"),
        IMAGE_MEM("ImageMem", "KB"),
        IMAGE_STORAGE("ImageStorage", "MB"),
        INSTANCE_DISK_SIZE("InstanceDiskSize", "MB"),
        INSTANCE_DISK_TYPE("InstanceDiskType", ""),
        INSTANCE_DISK_COUNT("InstanceDiskCount", ""),
        IO_THROUGHPUT("IOThroughput", "KByte/sec"),
        KPI("KPI", ""),
        LICENSE_ACCESS("LicenseAccess", ""),
        LICENSE_COMMODITY("LicenseCommodity", ""),
        MEM("Mem", "KB"),
        MEM_ALLOCATION("MemAllocation", "KB"),
        MEM_REQUEST_ALLOCATION("MemRequestAllocation", "KB"),
        MEM_HEADROOM("MemHeadroom", "VM"),
        MEM_EXHAUSTION("MemExhaustion", "Day"),
        MEM_PROVISIONED("MemProvisioned", "KB"),
        MOVE("Move", ""),
        NET_THROUGHPUT("NetThroughput", "KByte/sec"),
        NETWORK_INTERFACE_COUNT("NetworkInterfaceCount", ""),
        NETWORK_POLICY("NetworkPolicy", ""),
        NUM_CPUS("numCPUs", ""),
        NUM_DISK("NumDisk", ""),
        NUM_SOCKETS("numSockets", ""),
        NUM_CORES("numCores", ""),
        NUM_VCORE("NumVCore", ""),
        NUM_VCPUS("numVCPUs", ""),
        NUMBER_CONSUMERS("NumberConsumers", ""),
        NUMBER_CONSUMERS_PM("NumberConsumersPM", ""),
        NUMBER_CONSUMERS_STORAGE("NumberConsumersStorage", ""),
        POOL_CPU("PoolCPU", "MHz"),
        POOL_MEM("PoolMem", "KB"),
        POOL_STORAGE("PoolStorage", "MB"),
        PORT_CHANEL("PortChannel", "KByte/sec"),
        POWER("Power", "W"),
        PROCESSING_UNITS("ProcessingUnits", ""),
        PRODUCES("Produces", ""),
        SAME_CLUSTER_MOVE_SVC("SameClusterMoveSVC", ""),
        SERVICE_LEVEL_CLUSTER("ServiceLevelCluster", ""),
        SOFTWARE_LICENSE_COMMODITY("SoftwareLicenseCommodity", ""),
        SPACE("Space", ""),
        STORAGE("Storage", "MB"),
        STORAGE_AMOUNT("StorageAmount", "MB"),
        STORAGE_PROVISIONED("StorageProvisioned", "MB"),
        STORAGE_ACCESS("StorageAccess", "IOPS"),
        STORAGE_LATENCY("StorageLatency", "msec"),
        STORAGE_HEADROOM("StorageHeadroom", "VM"),
        STORAGE_EXHAUSTION("StorageExhaustion", "Day"),
        Q1_VCPU("Q1VCPU", "msec"),
        Q2_VCPU("Q2VCPU", "msec"),
        Q3_VCPU("Q3VCPU", "msec"),
        Q4_VCPU("Q4VCPU", "msec"),
        Q5_VCPU("Q5VCPU", "msec"),
        Q6_VCPU("Q6VCPU", "msec"),
        Q7_VCPU("Q7VCPU", "msec"),
        Q8_VCPU("Q8VCPU", "msec"),
        Q16_VCPU("Q16VCPU", "msec"),
        Q32_VCPU("Q32VCPU", "msec"),
        Q64_VCPU("Q64VCPU", "msec"),
        QN_VCPU("QNVCPU", "msec"),
        RESPONSE_TIME("ResponseTime", "msec"),
        RIGHT_SIZE_DOWN("RightsizeDown", ""),
        RIGHT_SIZE_SVC("RightsizeSVC", ""),
        RIGHT_SIZE_UP("RightsizeUp", ""),
        STORAGE_ALLOCATION("StorageAllocation", "MB"),
        SLA_COMMODITY("SLACommodity", ""),
        SWAPPING("Swapping", "Byte/sec"),
        TENANCY_ACCESS("TenancyAccess", ""),
        TEMPLATE_ACCESS("TemplateAccess", ""),
        TEMPLATE_FAMILY("TemplateFamily", ""),
        THREADS("Threads", "Threads"),
        TRANSACTION("Transaction", "TPS"),
        TRANSACTION_LOG("TransactionLog", "MB"),
        VCPU("VCPU", "MHz"),
        VCPU_ALLOCATION("VCPUAllocation", "MHz"),
        VCPU_LIMIT_QUOTA("VCPULimitQuota", "MHz"),
        VCPU_REQUEST("VCPURequest", "MHz"),
        VCPU_REQUEST_QUOTA("VCPURequestQuota", "MHz"),
        VMEM("VMem", "KB"),
        VMEM_ALLOCATION("VMemAllocation", "MB"),
        VMEM_LIMIT_QUOTA("VMemLimitQuota", "KB"),
        VMEM_REQUEST("VMemRequest", "KB"),
        VMEM_REQUEST_QUOTA("VMemRequestQuota", "KB"),
        VSTORAGE("VStorage", "MB"),
        DTU("DTU", ""),
        CONCURRENT_WORKER("ConcurrentWorker", ""),
        CONCURRENT_SESSION("ConcurrentSession", ""),
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
        HOST_LUN_ACCESS("HostLunAccess", ""),
        TOTAL_SESSIONS("TotalSessions", ""),
        ZONE("Zone", ""),
        // End of Access Commodities
        UNKNOWN("Unknown", "");


        private final String mixedCase;
        private final String units;

        CommodityTypeUnits(String mixedCase, String units) {
            this.mixedCase = mixedCase;
            this.units = units;
        }

        public String getMixedCase() {
            return mixedCase;
        }

        public String getUnits() {
            return units;
        }

        public static CommodityTypeUnits fromString(String mixedCaseName) {
            return COMMODITY_TYPE_UNITS_MAP.get(mixedCaseName);
        }

        /**
         * Match commodityType string to Enum ignoring case.
         *
         * @param commodityType Used to match against Enums available
         * @return Matched {@link CommodityTypeUnits} if matched or else null
         */
        public static CommodityTypeUnits fromStringIgnoreCase(String commodityType) {
            return COMMODITY_TYPE_UNITS_MAP_LOWER_CASE.get(commodityType.toLowerCase());
        }

        private static final Map<String, CommodityTypeUnits> COMMODITY_TYPE_UNITS_MAP;
        private static final Map<String, CommodityTypeUnits> COMMODITY_TYPE_UNITS_MAP_LOWER_CASE;

        static {
            ImmutableMap.Builder<String, CommodityTypeUnits> commodityTypeMapBuilder =
                    new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, CommodityTypeUnits> commodityTypeMapBuilderLowerCase =
                    new ImmutableMap.Builder<>();
            for (CommodityTypeUnits t : CommodityTypeUnits.values()) {
                commodityTypeMapBuilder.put(t.getMixedCase(), t);
                commodityTypeMapBuilderLowerCase.put(t.getMixedCase().toLowerCase(), t);
            }
            COMMODITY_TYPE_UNITS_MAP = commodityTypeMapBuilder.build();
            COMMODITY_TYPE_UNITS_MAP_LOWER_CASE = commodityTypeMapBuilderLowerCase.build();
        }
    }

    /**
     * Set of CPU commodity types to use "millicore" as unit.
     */
    private static final Set<Integer> CPU_COMMODITY_TYPES = ImmutableSet.of(CommodityType.VCPU_VALUE,
            CommodityType.VCPU_REQUEST_VALUE);
    // Millicore unit for container CPU commodity types.
    private static final String MILLICORE_UNIT = "millicore";

    /**
     * Get units for the given commodity type.
     *
     * @param commodityTypeInt proto integer type of commodity
     * @param atomicResizeTargetEntityTypeInt type of the target entity in atomic resize action
     * @return optional of units, or empty if no units
     */
    public static Optional<String> getCommodityUnits(int commodityTypeInt,
                @Nullable Integer atomicResizeTargetEntityTypeInt) {
        final CommodityType commodityType = CommodityType.forNumber(commodityTypeInt);
        try {
            String units = CommodityTypeUnits.valueOf(commodityType.name()).getUnits();
            // If resize info is container CPU commodity, set unit as "millicore".
            if (atomicResizeTargetEntityTypeInt != null
                    && atomicResizeTargetEntityTypeInt == EntityType.CONTAINER_SPEC_VALUE
                    && CPU_COMMODITY_TYPES.contains(commodityTypeInt)) {
                units = MILLICORE_UNIT;
            }
            return StringUtils.isEmpty(units) ? Optional.empty() : Optional.of(units);
        } catch (IllegalArgumentException e) {
            // the Enum is missing, it may be expected if there is no units associated with the
            // commodity, or unexpected if someone forgot to define units for the commodity
            logger.warn("No units for commodity {}", commodityType);
            return Optional.empty();
        }
    }
}
