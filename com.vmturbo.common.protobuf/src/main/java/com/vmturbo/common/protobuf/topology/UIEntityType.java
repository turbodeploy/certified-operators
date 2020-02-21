package com.vmturbo.common.protobuf.topology;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntityOrBuilder;
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
    BUSINESS_TRANSACTION("BusinessTransaction", EntityType.BUSINESS_TRANSACTION),
    APPLICATION_SERVER("ApplicationServer", EntityType.APPLICATION_SERVER),
    APPLICATION("Application", EntityType.APPLICATION),
    APPLICATION_COMPONENT("ApplicationComponent", EntityType.APPLICATION_COMPONENT),
    VIRTUAL_APPLICATION("VirtualApplication", EntityType.VIRTUAL_APPLICATION),
    AVAILABILITY_ZONE("AvailabilityZone", EntityType.AVAILABILITY_ZONE),
    BUSINESS_ACCOUNT("BusinessAccount", EntityType.BUSINESS_ACCOUNT),
    BUSINESS_USER("BusinessUser", EntityType.BUSINESS_USER),
    CHASSIS("Chassis", EntityType.CHASSIS),
    CLOUD_SERVICE("CloudService", EntityType.CLOUD_SERVICE),
    COMPUTE_TIER("ComputeTier", EntityType.COMPUTE_TIER),
    CONTAINER("Container", EntityType.CONTAINER),
    CONTAINER_POD("ContainerPod", EntityType.CONTAINER_POD),
    DATABASE("Database", EntityType.DATABASE),
    DATABASE_SERVER("DatabaseServer", EntityType.DATABASE_SERVER),
    DATABASE_SERVER_TIER("DatabaseServerTier", EntityType.DATABASE_SERVER_TIER),
    DATABASE_TIER("DatabaseTier", EntityType.DATABASE_TIER),
    DESKTOP_POOL("DesktopPool", EntityType.DESKTOP_POOL),
    DPOD("DPod", EntityType.DPOD),
    HYPERVISOR_SERVER("Hypervisor Server", EntityType.HYPERVISOR_SERVER),
    INTERNET("Internet", EntityType.INTERNET),
    IOMODULE("IOModule", EntityType.IO_MODULE),
    LOAD_BALANCER("LoadBalancer", EntityType.LOAD_BALANCER),
    LOGICALPOOL("LogicalPool", EntityType.LOGICAL_POOL),
    NETWORK("Network", EntityType.NETWORK),
    PROCESSOR_POOL("ProcessorPool", EntityType.PROCESSOR_POOL),
    REGION("Region", EntityType.REGION),
    SERVICE_PROVIDER("ServiceProvider", EntityType.SERVICE_PROVIDER),
    STORAGECONTROLLER("StorageController", EntityType.STORAGE_CONTROLLER),
    STORAGE_TIER("StorageTier", EntityType.STORAGE_TIER),
    SWITCH("Switch", EntityType.SWITCH),
    UNKNOWN("Unknown", EntityType.UNKNOWN),
    VIEW_POD("ViewPod", EntityType.VIEW_POD),
    SERVICE("Service", EntityType.SERVICE),
    VIRTUAL_VOLUME("VirtualVolume", EntityType.VIRTUAL_VOLUME),
    VPOD("VPod", EntityType.VPOD);

    /**
     * These are the entity types that count as "Workloads" in our system.
     *
     * <p>Workloads are "arbitrary units of functionality", but we use it to encapsulate certain
     * important entity types for management and licensing purposes.
     */
    public static final Set<UIEntityType> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(
        UIEntityType.VIRTUAL_MACHINE, UIEntityType.DATABASE, UIEntityType.DATABASE_SERVER);

    /**
     * For these entity types exist information about cost in cost component.
     */
    public static final ImmutableSet<String> ENTITY_TYPES_WITH_COST = ImmutableSet.of(
            UIEntityType.VIRTUAL_MACHINE.apiStr(),
            UIEntityType.DATABASE.apiStr(),
            UIEntityType.DATABASE_SERVER.apiStr(),
            UIEntityType.VIRTUAL_VOLUME.apiStr()
    );


    private final String uiStr;

    private final EntityType type;

    private final String displayName;

    UIEntityType(String uiStr, EntityType type) {
        this.uiStr = uiStr;
        this.type = type;
        this.displayName = StringUtil.beautifyString(type.name());
    }

    public String apiStr() {
        return uiStr;
    }

    public int typeNumber() {
        return type.getNumber();
    }

    public String displayName() { return displayName; }

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
     * Converts a {@link com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity}
     * to the corresponding {@link UIEntityType}, based on the entity's type
     * @param entity The target {@link MinimalEntityOrBuilder}
     * @return The {@link UIEntityType} of the entity
     */
    @Nonnull
    public static UIEntityType fromMinimalEntity(@Nonnull MinimalEntityOrBuilder entity) {
        return fromType(entity.getEntityType());
    }

    /**
     * Converts type from a string to the enum type.
     *
     * @param type string representation of service entity type
     * @return UI entity type enum
     */
    @Nonnull
    public static UIEntityType fromString(String type) {
        return ENTITY_STR_MAPPINGS.getOrDefault(type, UIEntityType.UNKNOWN);
    }

    /**
     * Converts from a string to the {@link EntityType}.
     *
     * @param type string representation of service entity type
     * @return int numeric representation of EnitityType
     */
    @Nonnull
    public static int fromStringToSdkType(String type) {
        return fromString(type).typeNumber();
    }

    /**
     * Converts from a {@link EntityType} numeric value to {@link UIEntityType} string.
     *
     * @param sdkType numeric representation of EnitityType
     * @return type String representation of service entity type
     */
    @Nonnull
    public static String fromSdkTypeToEntityTypeString(int sdkType) {
        return fromType(sdkType).apiStr();
    }
}
