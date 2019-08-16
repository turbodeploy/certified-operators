package com.vmturbo.common.protobuf.topology;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public enum UIEntityType {
    VIRTUAL_MACHINE("VirtualMachine", EntityType.VIRTUAL_MACHINE),
    PHYSICAL_MACHINE("PhysicalMachine", EntityType.PHYSICAL_MACHINE),
    STORAGE("Storage", EntityType.STORAGE),
    DATACENTER("DataCenter", EntityType.DATACENTER),
    DISKARRAY("DiskArray", EntityType.DISK_ARRAY),
    VIRTUAL_DATACENTER("VirtualDataCenter", EntityType.VIRTUAL_DATACENTER),
    BUSINESS_APPLICATION("BusinessApplication", EntityType.BUSINESS_APPLICATION),
    APPLICATION_SERVER("ApplicationServer", EntityType.APPLICATION_SERVER),
    APPLICATION("Application", EntityType.APPLICATION),
    VIRTUAL_APPLICATION("VirtualApplication", EntityType.VIRTUAL_APPLICATION),
    CONTAINER("Container", EntityType.CONTAINER),
    CONTAINER_POD("ContainerPod", EntityType.CONTAINER_POD),
    VPOD("VPod", EntityType.VPOD),
    DPOD("DPod", EntityType.DPOD),
    STORAGECONTROLLER("StorageController", EntityType.STORAGE_CONTROLLER),
    IOMODULE("IOModule", EntityType.IO_MODULE),
    INTERNET("Internet", EntityType.INTERNET),
    SWITCH("Switch", EntityType.SWITCH),
    CHASSIS("Chassis", EntityType.CHASSIS),
    NETWORK("Network", EntityType.NETWORK),
    LOGICALPOOL("LogicalPool", EntityType.LOGICAL_POOL),
    DATABASE("Database", EntityType.DATABASE),
    DATABASE_SERVER("DatabaseServer", EntityType.DATABASE_SERVER),
    LOAD_BALANCER("LoadBalancer", EntityType.LOAD_BALANCER),
    BUSINESS_ACCOUNT("BusinessAccount", EntityType.BUSINESS_ACCOUNT),
    CLOUD_SERVICE("CloudService", EntityType.CLOUD_SERVICE),
    COMPUTE_TIER("ComputeTier", EntityType.COMPUTE_TIER),
    STORAGE_TIER("StorageTier", EntityType.STORAGE_TIER),
    DATABASE_TIER("DatabaseTier", EntityType.DATABASE_TIER),
    DATABASE_SERVER_TIER("DatabaseServerTier", EntityType.DATABASE_SERVER_TIER),
    AVAILABILITY_ZONE("AvailabilityZone", EntityType.AVAILABILITY_ZONE),
    REGION("Region", EntityType.REGION),
    VIRTUAL_VOLUME("VirtualVolume", EntityType.VIRTUAL_VOLUME),
    PROCESSOR_POOL("ProcessorPool", EntityType.PROCESSOR_POOL),
    HYPERVISOR_SERVER("Hypervisor Server", EntityType.HYPERVISOR_SERVER),
    VIEW_POD("ViewPod", EntityType.VIEW_POD),
    DESKTOP_POOL("DesktopPool", EntityType.DESKTOP_POOL),
    BUSINESS_USER("BusinessUser", EntityType.BUSINESS_USER),
    UNKNOWN("Unknown", EntityType.UNKNOWN);

    private final String uiStr;

    private final EntityType type;

    UIEntityType(String uiStr, EntityType type) {
        this.uiStr = uiStr;
        this.type = type;
    }

    public String apiStr() {
        return uiStr;
    }

    public int typeNumber() {
        return type.getNumber();
    }

    @Nonnull
    public EntityType sdkType() {
        return type;
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    private static final BiMap<Integer, UIEntityType> ENTITY_TYPE_MAPPINGS;
    private static final BiMap<String, UIEntityType> ENTITY_STR_MAPPINGS;

    static {
        ImmutableBiMap.Builder<Integer, UIEntityType> entityTypeMappingBldr = new ImmutableBiMap.Builder<>();
        ImmutableBiMap.Builder<String, UIEntityType> entityStrMappingBldr = new ImmutableBiMap.Builder<>();
        for (UIEntityType type : UIEntityType.values()) {
            entityTypeMappingBldr.put(type.typeNumber(), type);
            entityStrMappingBldr.put(type.apiStr(), type);
        }
        ENTITY_TYPE_MAPPINGS = entityTypeMappingBldr.build();
        ENTITY_STR_MAPPINGS = entityStrMappingBldr.build();
    }

    /**
     * Maps the entity type in TopologyEntityDTO to strings of entity types used in UI.
     *
     * @param type The entity type in the TopologyEntityDTO
     * @return     The corresponding entity type string in UI
     */
    @Nonnull
    public static UIEntityType fromType(final int type) {
        return ENTITY_TYPE_MAPPINGS.getOrDefault(type, UIEntityType.UNKNOWN);
    }

    @Nonnull
    public static UIEntityType fromEntity(@Nonnull final TopologyEntityDTOOrBuilder entity) {
        return fromType(entity.getEntityType());
    }

    /**
     * Converts type from a string to the enum type.
     * @param type string representation of service entity type
     * @return UI entity type enum
     */
    @Nonnull
    public static UIEntityType fromString(String type) {
        return ENTITY_STR_MAPPINGS.getOrDefault(type, UIEntityType.UNKNOWN);
    }
}
