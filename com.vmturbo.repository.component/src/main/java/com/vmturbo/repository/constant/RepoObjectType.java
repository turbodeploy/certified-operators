package com.vmturbo.repository.constant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Contains enum values for object types of service entities and commodities.
 * Also converts the object types from the one used in TopologyDTO to the one
 * used in the repository. One main purpose of such conversion is to match the
 * type strings hard-coded in the UI side.
 *
 * TODO: A better way to match the type strings for multiple components, including UI,
 *       is to use the types in @{link EntityType}, i.e., replacing the hard-coded strings
 *       in UI side.
 */
public class RepoObjectType {
    public enum RepoEntityType {
        VIRTUAL_MACHINE("VirtualMachine"),
        PHYSICAL_MACHINE("PhysicalMachine"),
        STORAGE("Storage"),
        DATACENTER("DataCenter"),
        DISKARRAY("DiskArray"),
        VIRTUAL_DATACENTER("VirtualDataCenter"),
        APPLICATION("Application"),
        VIRTUAL_APPLICATION("VirtualApplication"),
        CONTAINER("Container"),
        CONTAINER_POD("ContainerPod"),
        VPOD("VPod"),
        DPOD("DPod"),
        STORAGECONTROLLER("StorageController"),
        IOMODULE("IOModule"),
        INTERNET("Internet"),
        SWITCH("Switch"),
        CHASSIS("Chassis"),
        NETWORK("Network"),
        LOGICALPOOL("LogicalPool"),
        DATABASE("Database"),
        DATABASE_SERVER("DatabaseServer"),
        LOAD_BALANCER("LoadBalancer"),
        BUSINESS_ACCOUNT("BusinessAccount"),
        CLOUD_SERVICE("CloudService"),
        COMPUTE_TIER("ComputeTier"),
        STORAGE_TIER("StorageTier"),
        DATABASE_TIER("DatabaseTier"),
        DATABASE_SERVER_TIER("DatabaseServerTier"),
        AVAILABILITY_ZONE("AvailabilityZone"),
        REGION("Region"),
        VIRTUAL_VOLUME("VirtualVolume"),
        UNKNOWN("Unknown");

        private final String value;

        RepoEntityType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Converts state from a string to the enum type.
         *
         * @param type The type string to convert.
         * @return The enum type converted.
         */
        public static RepoEntityType fromString(String type) {
            if (type != null) {
                for (RepoEntityType t : RepoEntityType.values()) {
                    if (type.equals(t.value)) {
                        return t;
                    }
                }
            }

            throw new IllegalArgumentException(
                       "No RepoEntityType constant with type " + type + " found");
        }
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that the UI understands.
     * TODO: VPOD and DPOD are not found in EntityType.
     */
    private static final BiMap<EntityType, RepoEntityType> ENTITY_TYPE_MAPPINGS =
               new ImmutableBiMap.Builder<EntityType, RepoEntityType>()
            .put(EntityType.VIRTUAL_MACHINE,        RepoEntityType.VIRTUAL_MACHINE)
            .put(EntityType.PHYSICAL_MACHINE,       RepoEntityType.PHYSICAL_MACHINE)
            .put(EntityType.STORAGE,                RepoEntityType.STORAGE)
            .put(EntityType.DISK_ARRAY,             RepoEntityType.DISKARRAY)
            .put(EntityType.DATACENTER,             RepoEntityType.DATACENTER)
            .put(EntityType.VIRTUAL_DATACENTER,     RepoEntityType.VIRTUAL_DATACENTER)
            .put(EntityType.APPLICATION,            RepoEntityType.APPLICATION)
            .put(EntityType.VIRTUAL_APPLICATION,    RepoEntityType.VIRTUAL_APPLICATION)
            .put(EntityType.CONTAINER,              RepoEntityType.CONTAINER)
            .put(EntityType.CONTAINER_POD,          RepoEntityType.CONTAINER_POD)
            .put(EntityType.STORAGE_CONTROLLER,     RepoEntityType.STORAGECONTROLLER)
            .put(EntityType.IO_MODULE,              RepoEntityType.IOMODULE)
            .put(EntityType.INTERNET,               RepoEntityType.INTERNET)
            .put(EntityType.SWITCH,                 RepoEntityType.SWITCH)
            .put(EntityType.CHASSIS,                RepoEntityType.CHASSIS)
            .put(EntityType.NETWORK,                RepoEntityType.NETWORK)
            .put(EntityType.LOGICAL_POOL,           RepoEntityType.LOGICALPOOL)
            .put(EntityType.DATABASE,               RepoEntityType.DATABASE)
            .put(EntityType.DATABASE_SERVER,        RepoEntityType.DATABASE_SERVER)
            .put(EntityType.LOAD_BALANCER,          RepoEntityType.LOAD_BALANCER)
            .put(EntityType.BUSINESS_ACCOUNT,       RepoEntityType.BUSINESS_ACCOUNT)
            .put(EntityType.CLOUD_SERVICE,          RepoEntityType.CLOUD_SERVICE)
            .put(EntityType.COMPUTE_TIER,           RepoEntityType.COMPUTE_TIER)
            .put(EntityType.STORAGE_TIER,           RepoEntityType.STORAGE_TIER)
            .put(EntityType.DATABASE_TIER,          RepoEntityType.DATABASE_TIER)
            .put(EntityType.DATABASE_SERVER_TIER,   RepoEntityType.DATABASE_SERVER_TIER)
            .put(EntityType.AVAILABILITY_ZONE,      RepoEntityType.AVAILABILITY_ZONE)
            .put(EntityType.REGION,                 RepoEntityType.REGION)
            .put(EntityType.VIRTUAL_VOLUME,         RepoEntityType.VIRTUAL_VOLUME)

            .put(EntityType.UNKNOWN,                RepoEntityType.UNKNOWN)
            .build();

    /**
     * Maps the entity type in TopologyEntityDTO to strings of entity types used in the repository.
     *
     * @param type The entity type in the TopologyEntityDTO
     * @return     The corresponding entity type string in the repository
     */
    public static String mapEntityType(int type) {
        final RepoEntityType repoEntityType = ENTITY_TYPE_MAPPINGS.get(EntityType.forNumber(type));

        if (repoEntityType == null) {
            return EntityType.valueOf(type).toString();
        }

        return repoEntityType.getValue();
    }

    /**
     * Maps the entity type used in UI to the integer used in TopologyEntityDTO.
     *
     * @param repoType The type String used in UI
     * @return the TopologyEntityDTO entity type for the given type string
     */
    public static int toTopologyEntityType(String repoType) {
        final EntityType topologyEntityType = ENTITY_TYPE_MAPPINGS.inverse()
                        .get(RepoEntityType.fromString(repoType));

        return topologyEntityType != null ? topologyEntityType.getNumber()
                                           : EntityState.UNKNOWN.getNumber();
    }

    public enum RepoCommodityType {
        MEM("Mem"),
        VMEM("VMem"),
        IO_THROUGHPUT("IOThroughput"),
        NET_THROUGHPUT("NetThroughput"),
        VSTORAGE("VStorage"),
        CPU("CPU"),
        CPU_PROVISIONED("CPUProvisioned"),
        CPU_ALLOCATION("CPUAllocation"),
        MEM_PROVISIONED("MemProvisioned"),
        MEM_ALLOCATION("MemAllocation"),
        STORAGE_AMOUNT("StorageAmount"),
        STORAGE_PROVISIONED("StorageProvisioned"),
        STORAGE_ACCESS("StorageAccess"),
        STORAGE_LATENCY("StorageLatency"),
        COOLING("Cooling"),
        POWER("Power"),
        SPACE("Space"),
        BALLOONING("Ballooning"),
        Q1VCPU("Q1VCPU"),
        Q2VCPU("Q2VCPU"),
        Q4VCPU("Q4VCPU"),
        FLOW("Flow"),
        SWAPPING("Swapping"),
        VCPU("VCPU"),
        STORAGEALLOCATION("StorageAllocation"),
        FLOWALLOCATION("FlowAllocation"),
        VCPUALLOCATION("VCPUAllocation"),
        VMEMALLOCATION("VMemAllocation"),
        TRANSACTION("Transaction"),
        SLACOMMODITY("SLACommodity"),

        UNKNOWN("Unknown");


        private final String value;

        RepoCommodityType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Convert String type to {@link RepoCommodityType). There are some Commodity type, such as
         * Datacenter, Cluster, are not type of {@link RepoCommodityType}. In this case, it will return
         * null.
         *
         * @param type string type of Commodity.
         * @return null if can not find relate {@link RepoCommodityType}.
         */
        @Nullable
        public static RepoCommodityType fromString(String type) {
            if (type != null) {
                for (RepoCommodityType t : RepoCommodityType.values()) {
                    if (type.equals(t.value)) {
                        return t;
                    }
                }
            }
            return null;
        }
    }


    /**
     * Mappings between commodityType enum values in TopologyEntityDTO to strings that the UI understands.
     */
    private static final BiMap<CommodityType, RepoCommodityType> COMMODITY_TYPE_MAPPINGS =
                    new ImmutableBiMap.Builder<CommodityType, RepoCommodityType>()
                    .put(CommodityType.IO_THROUGHPUT,       RepoCommodityType.IO_THROUGHPUT)
                    .put(CommodityType.NET_THROUGHPUT,      RepoCommodityType.NET_THROUGHPUT)
                    .put(CommodityType.VMEM,                RepoCommodityType.VMEM)
                    .put(CommodityType.VSTORAGE,            RepoCommodityType.VSTORAGE)
                    .put(CommodityType.CPU,                 RepoCommodityType.CPU)
                    .put(CommodityType.CPU_PROVISIONED,     RepoCommodityType.CPU_PROVISIONED)
                    .put(CommodityType.CPU_ALLOCATION,      RepoCommodityType.CPU_ALLOCATION)
                    .put(CommodityType.MEM,                 RepoCommodityType.MEM)
                    .put(CommodityType.MEM_PROVISIONED,     RepoCommodityType.MEM_PROVISIONED)
                    .put(CommodityType.MEM_ALLOCATION,      RepoCommodityType.MEM_ALLOCATION)
                    .put(CommodityType.STORAGE_AMOUNT,      RepoCommodityType.STORAGE_AMOUNT)
                    .put(CommodityType.STORAGE_PROVISIONED, RepoCommodityType.STORAGE_PROVISIONED)
                    .put(CommodityType.STORAGE_ACCESS,      RepoCommodityType.STORAGE_ACCESS)
                    .put(CommodityType.STORAGE_LATENCY,     RepoCommodityType.STORAGE_LATENCY)
                    .put(CommodityType.COOLING,             RepoCommodityType.COOLING)
                    .put(CommodityType.POWER,               RepoCommodityType.POWER)
                    .put(CommodityType.SPACE,               RepoCommodityType.SPACE)
                    .put(CommodityType.BALLOONING,          RepoCommodityType.BALLOONING)
                    .put(CommodityType.Q1_VCPU,             RepoCommodityType.Q1VCPU)
                    .put(CommodityType.Q2_VCPU,             RepoCommodityType.Q2VCPU)
                    .put(CommodityType.Q4_VCPU,             RepoCommodityType.Q4VCPU)
                    .put(CommodityType.FLOW,                RepoCommodityType.FLOW)
                    .put(CommodityType.SWAPPING,            RepoCommodityType.SWAPPING)
                    .put(CommodityType.VCPU,                RepoCommodityType.VCPU)
                    .put(CommodityType.STORAGE_ALLOCATION,  RepoCommodityType.STORAGEALLOCATION)
                    .put(CommodityType.FLOW_ALLOCATION,     RepoCommodityType.FLOWALLOCATION)
                    .put(CommodityType.TRANSACTION,         RepoCommodityType.TRANSACTION)
                    .put(CommodityType.SLA_COMMODITY,       RepoCommodityType.SLACOMMODITY)
                    .put(CommodityType.UNKNOWN,             RepoCommodityType.UNKNOWN)
                    .build();


    /**
     * Convert a {@link CommodityType} into a string that UI expects.
     * Otherwise, will not display the right data, because it is hard-coded to look for certain string.
     *
     * @param type The commodity type from TopologyDTO
     * @return     A string that UI expects
     */
    public static String mapCommodityType(int type) {
        final RepoCommodityType repoCommType = COMMODITY_TYPE_MAPPINGS.get(CommodityType.valueOf(type));

        if (repoCommType == null) {
            return CommodityType.valueOf(type).toString();
        }

        return repoCommType.getValue();
    }

    /**
     * Convert a UI String commodity type to a {@link CommodityType}. There are some commodity type are
     * not appeared in Commodity Map, such as DataCenter, Cluster. In this case, it will directly
     * converted to {@link CommodityType}.
     *
     * @param type type of Commodity need to convert.
     * @return a int value of {@link CommodityType}.
     */
    public static int mapCommodityType(@Nonnull final String type) {
        final CommodityType commodityType = COMMODITY_TYPE_MAPPINGS.inverse()
                .get(RepoCommodityType.fromString(type));
        return commodityType != null ? commodityType.getNumber() :
                CommodityType.valueOf(type).getNumber();
    }
}
