package com.vmturbo.components.common;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;

/**
 * Maps values from the classic OpsManager system to values in the SDK.
 */
public class ClassicEnumMapper {
    private static final Logger logger = LogManager.getLogger();

    public static EntityType entityType(@Nonnull final String classicEntityTypeName) {
        final EntityType entityType = ENTITY_TYPE_MAPPINGS.get(classicEntityTypeName);
        if (entityType == null) {
            logger.info("Missing entity type information for: " + classicEntityTypeName);
        }

        return entityType;
    }

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

    /**
     * Mappings between entityType enum values in SDK DTO's to strings that are stored
     * in Classic OpsManager topology files.
     */
    public static final BiMap<String, EntityType> ENTITY_TYPE_MAPPINGS =
        new ImmutableBiMap.Builder<String, EntityType>()
            .put("VirtualMachine",          EntityType.VIRTUAL_MACHINE)
            .put("PhysicalMachine",         EntityType.PHYSICAL_MACHINE)
            .put("Storage",                 EntityType.STORAGE)
            .put("DiskArray",               EntityType.DISK_ARRAY)
            .put("DataCenter",              EntityType.DATACENTER)
            .put("VirtualDataCenter",       EntityType.VIRTUAL_DATACENTER)
            .put("Application",             EntityType.APPLICATION)
            .put("VirtualApplication",      EntityType.VIRTUAL_APPLICATION)
            .put("Container",               EntityType.CONTAINER)
            .put("StorageController",       EntityType.STORAGE_CONTROLLER)
            .put("IOModule",                EntityType.IO_MODULE)
            .put("Internet",                EntityType.INTERNET)
            .put("Switch",                  EntityType.SWITCH)
            .put("Chassis",                 EntityType.CHASSIS)
            .put("Network",                 EntityType.NETWORK)
            .put("Database",                EntityType.DATABASE)
            .put("ApplicationServer",       EntityType.APPLICATION_SERVER)
            .put("DPod",                    EntityType.DPOD)
            .put("VPod",                    EntityType.VPOD)
            .put("ContainerPod",            EntityType.CONTAINER_POD)
            .put("LogicalPool",             EntityType.LOGICAL_POOL)
            .put("CloudService",            EntityType.CLOUD_SERVICE)
            .put("DistributedVirtualPortgroup", EntityType.DISTRIBUTED_VIRTUAL_PORTGROUP)
            .put("BusinessAccount",         EntityType.BUSINESS_ACCOUNT)
            .put("Unknown",                 EntityType.UNKNOWN)
            .build();

    /**
     * Mappings between commodityType enum values in SDK DTO's to strings that are stored
     * in Classic OpsManager topology files.
     */
    public static final ImmutableMap<String, CommodityType> COMMODITY_TYPE_MAPPINGS =
        new ImmutableMap.Builder<String, CommodityType>()
            .put("IOThroughput",                CommodityType.IO_THROUGHPUT)
            .put("NetThroughput",               CommodityType.NET_THROUGHPUT)
            .put("VMem",                        CommodityType.VMEM)
            .put("VStorage",                    CommodityType.VSTORAGE)
            .put("CPU",                         CommodityType.CPU)
            .put("CPUProvisioned",              CommodityType.CPU_PROVISIONED)
            .put("CPUAllocation",               CommodityType.CPU_ALLOCATION)
            .put("Mem",                         CommodityType.MEM)
            .put("MemProvisioned",              CommodityType.MEM_PROVISIONED)
            .put("MemAllocation",               CommodityType.MEM_ALLOCATION)
            .put("StorageAmount",               CommodityType.STORAGE_AMOUNT)
            .put("StorageProvisioned",          CommodityType.STORAGE_PROVISIONED)
            .put("StorageAccess",               CommodityType.STORAGE_ACCESS)
            .put("StorageLatency",              CommodityType.STORAGE_LATENCY)
            .put("Cooling",                     CommodityType.COOLING)
            .put("Power",                       CommodityType.POWER)
            .put("Space",                       CommodityType.SPACE)
            .put("Ballooning",                  CommodityType.BALLOONING)
            .put("Q1VCPU",                      CommodityType.Q1_VCPU)
            .put("Q2VCPU",                      CommodityType.Q2_VCPU)
            .put("Q4VCPU",                      CommodityType.Q4_VCPU)
            .put("Q8VCPU",                      CommodityType.Q8_VCPU)
            .put("Q16VCPU",                     CommodityType.Q16_VCPU)
            .put("Q32VCPU",                     CommodityType.Q32_VCPU)
            .put("Q64VCPU",                     CommodityType.Q64_VCPU)
            .put("Flow",                        CommodityType.FLOW)
            .put("Swapping",                    CommodityType.SWAPPING)
            .put("VCPU",                        CommodityType.VCPU)
            .put("StorageAllocation",           CommodityType.STORAGE_ALLOCATION)
            .put("FlowAllocation",              CommodityType.FLOW_ALLOCATION)
            .put("Transaction",                 CommodityType.TRANSACTION)
            .put("SLACommodity",                CommodityType.SLA_COMMODITY)
            .put("DSPMAccessCommodity",         CommodityType.DSPM_ACCESS)
            .put("ApplicationCommodity",        CommodityType.APPLICATION)
            .put("BufferCommodity",             CommodityType.BUFFER_COMMODITY)
            .put("DataCenterCommodity",         CommodityType.DATACENTER)
            .put("DatastoreCommodity",          CommodityType.DATASTORE)
            .put("SoftwareLicenseCommodity",    CommodityType.SOFTWARE_LICENSE_COMMODITY)
            .put("ActionPermit",                CommodityType.ACTION_PERMIT)
            .put("CrossCloudMoveSvc",           CommodityType.CROSS_CLOUD_MOVE_SVC)
            .put("CrossClusterMoveSvc",         CommodityType.CROSS_CLUSTER_MOVE_SVC)
            .put("SameClusterMoveSvc",          CommodityType.SAME_CLUSTER_MOVE_SVC)
            .put("StorageClusterCommodity",     CommodityType.STORAGE_CLUSTER)
            .put("Extent",                      CommodityType.EXTENT)
            .put("ClusterCommodity",            CommodityType.CLUSTER)
            .put("NetworkCommodity",            CommodityType.NETWORK)
            .put("DrsSegmentationCommodity",    CommodityType.DRS_SEGMENTATION)
            .put("SegmentationCommodity",       CommodityType.SEGMENTATION)
            .put("VDCCommodity",                CommodityType.VDC)
            .put("HOST_LUN_ACCESS",             CommodityType.HOST_LUN_ACCESS)
            .put("DISK_ARRAY_ACCESS",           CommodityType.DISK_ARRAY_ACCESS)
            .put("LICENSE_ACCESS",              CommodityType.LICENSE_ACCESS)
            .put("PORT_CHANEL",                 CommodityType.PORT_CHANEL)
            .put("Unknown",                     CommodityType.UNKNOWN)
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
    public  enum CommodityTypeUnits {
        BALLOONING("Ballooning", "KB"),
        COLLECTION_TIME("CollectionTime", "%"),
        CONNECTION("Connection", "Connections"),
        COOLING("Cooling", "C"),
        CPU("CPU", "MHz"),
        CPU_ALLOCATION("CPUAllocation", "MHz"),
        CPU_HEADROOM("CPUHeadroom", "VM"),
        CPU_EXHAUSTION("CPUExhaustion", "Day"),
        CPU_PROVISIONED("CPUProvisioned", "MHz"),
        DB_CACHE_HIT_RATE("DBCacheHitRate", "%"),
        DB_MEM("DBMem", "KB"),
        EXTENT("Extent", ""),
        FLOW("Flow", "Bytes"),
        FLOW_ALLOCATION("FlowAllocation", "Bytes"),
        HEAP("Heap", "KB"),
        IO_THROUGHPUT("IOThroughput", "KByte/sec"),
        MEM("Mem", "KB"),
        MEM_ALLOCATION("MemAllocation", "KB"),
        MEM_HEADROOM("MemHeadroom", "VM"),
        MEM_EXHAUSTION("MemExhaustion", "Day"),
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
        STORAGE_HEADROOM("StorageHeadroom", "VM"),
        STORAGE_EXHAUSTION("StorageExhaustion", "Day"),
        Q1_VCPU("Q1VCPU", "msec"),
        Q2_VCPU("Q2VCPU", "msec"),
        Q4_VCPU("Q4VCPU", "msec"),
        Q8_VCPU("Q8VCPU", "msec"),
        Q16_VCPU("Q16VCPU", "msec"),
        Q32_VCPU("Q32VCPU", "msec"),
        Q64_VCPU("Q64VCPU", "msec"),
        RESPONSE_TIME("ResponseTime", "msec"),
        STORAGE_ALLOCATION("StorageAllocation", "MB"),
        SLA_COMMODITY("SLACommodity", ""),
        SWAPPING("Swapping", "Byte/sec"),
        THREADS("Threads", "Threads"),
        TRANSACTION("Transaction", "TPS"),
        VCPU("VCPU", "MHz"),
        VCPU_ALLOCATION("VCPUAllocation", "MHz"),
        VMEM("VMem", "KB"),
        VMEM_ALLOCATION("VMemAllocation", "MB"),
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
        HOST_LUN_ACCESS("HOST_LUN_ACCESS", ""),
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

        private static final Map<String, CommodityTypeUnits> COMMODITY_TYPE_UNITS_MAP;

        static {
            ImmutableMap.Builder<String, CommodityTypeUnits> commodityTypeMapBuilder =
                    new ImmutableMap.Builder<>();
            for (CommodityTypeUnits t : CommodityTypeUnits.values()) {
                commodityTypeMapBuilder.put(t.getMixedCase(), t);
            }
            COMMODITY_TYPE_UNITS_MAP = commodityTypeMapBuilder.build();
        }
    }

}
