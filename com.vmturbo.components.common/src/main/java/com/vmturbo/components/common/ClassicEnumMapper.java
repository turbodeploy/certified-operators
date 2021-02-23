package com.vmturbo.components.common;

import static com.vmturbo.api.conversion.entity.CommodityTypeMapping.COMMODITY_TYPE_TO_API_STRING;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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
        CPU_HEADROOM("CPUHeadroom", "VM"),
        CPU_EXHAUSTION("CPUExhaustion", "Day"),
        MEM_HEADROOM("MemHeadroom", "VM"),
        MEM_EXHAUSTION("MemExhaustion", "Day"),
        NUM_CPUS("numCPUs", ""),
        NUM_SOCKETS("numSockets", ""),
        NUM_CORES("numCores", ""),
        NUM_VCPUS("numVCPUs", ""),
        PRODUCES("Produces", ""),
        STORAGE_HEADROOM("StorageHeadroom", "VM"),
        STORAGE_EXHAUSTION("StorageExhaustion", "Day"),
        TEMPLATE_FAMILY("TemplateFamily", ""),
        VCPU_ALLOCATION("VCPUAllocation", "MHz"),
        VMEM_ALLOCATION("VMemAllocation", "MB");


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

        public static String unitFromString(String mixedCaseName) {
            return COMMODITY_TYPE_UNITS_MAP.get(mixedCaseName);
        }

        /**
         * Match get unit for commodity from mixed case ignoring case.
         *
         * @param commodityType The mixed case.
         * @return The unit if matched or else null
         */
        public static String unitFromStringIgnoreCase(String commodityType) {
            return COMMODITY_TYPE_UNITS_MAP_LOWER_CASE.get(commodityType.toLowerCase());
        }

        private static final Map<String, String> COMMODITY_TYPE_UNITS_MAP;
        private static final Map<String, String> COMMODITY_TYPE_UNITS_MAP_LOWER_CASE;

        static {
            ImmutableMap.Builder<String, String> commodityTypeMapBuilder =
                    new ImmutableMap.Builder<>();
            ImmutableMap.Builder<String, String> commodityTypeMapBuilderLowerCase =
                    new ImmutableMap.Builder<>();
            for (CommodityTypeUnits t : CommodityTypeUnits.values()) {
                commodityTypeMapBuilder.put(t.getMixedCase(), t.getUnits());
                commodityTypeMapBuilderLowerCase.put(t.getMixedCase().toLowerCase(), t.getUnits());
            }

            // add commodities to the map
            for (CommodityTypeMapping.CommodityInfo t : COMMODITY_TYPE_TO_API_STRING.values()) {
                commodityTypeMapBuilder.put(t.getMixedCase(), t.getUnits());
                commodityTypeMapBuilderLowerCase.put(t.getMixedCase().toLowerCase(), t.getUnits());
            }

            COMMODITY_TYPE_UNITS_MAP = commodityTypeMapBuilder.build();
            COMMODITY_TYPE_UNITS_MAP_LOWER_CASE = commodityTypeMapBuilderLowerCase.build();
        }
    }

}
