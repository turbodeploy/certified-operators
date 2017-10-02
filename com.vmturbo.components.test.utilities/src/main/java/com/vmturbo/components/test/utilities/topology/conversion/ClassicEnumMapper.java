package com.vmturbo.components.test.utilities.topology.conversion;

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
            logger.info("Missing commodity type information for: " + classicCommodityTypeName);
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
            .put("Unknown",                 EntityType.UNKNOWN)
            .build();

    /**
     * Mappings between commodityType enum values in SDK DTO's to strings that are stored
     * in Classic OpsManager topology files.
     */
    private static final ImmutableMap<String, CommodityType> COMMODITY_TYPE_MAPPINGS =
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
}
