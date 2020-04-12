package com.vmturbo.common.protobuf.topology;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntityOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTOREST.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This enum lists all the entity types that can be named in API calls, linking them with
 * the underlying SDK entity types.
 */
public enum ApiEntityType {
    /** Application Component entity type. */
    APPLICATION_COMPONENT(StringConstants.APPLICATION_COMPONENT, EntityType.APPLICATION_COMPONENT),
    /** Service entity type. */
    SERVICE(StringConstants.SERVICE, EntityType.SERVICE),
    /** Application entity type. */
    APPLICATION(StringConstants.APPLICATION, EntityType.APPLICATION),
    /** ApplicationServer entity type. */
    APPLICATION_SERVER(StringConstants.APPSRV, EntityType.APPLICATION_SERVER),
    /** AvailabilityZone entity type. */
    AVAILABILITY_ZONE(StringConstants.AVAILABILITY_ZONE_ENTITY, EntityType.AVAILABILITY_ZONE),
    /** BusinessAccount entity type. */
    BUSINESS_ACCOUNT(StringConstants.BUSINESS_ACCOUNT, EntityType.BUSINESS_ACCOUNT),
    /** BusinessApplication entity type. */
    BUSINESS_APPLICATION(StringConstants.BUSINESS_APPLICATION, EntityType.BUSINESS_APPLICATION),
    /** Business Transaction entity type. */
    BUSINESS_TRANSACTION(StringConstants.BUSINESS_TRANSACTION, EntityType.BUSINESS_TRANSACTION),
    /** BusinessUser entity type. */
    BUSINESS_USER(StringConstants.BUSINESS_USER, EntityType.BUSINESS_USER),
    /** Chassis entity type. */
    CHASSIS(StringConstants.CHASSIS, EntityType.CHASSIS),
    /** CloudService entity type. */
    CLOUD_SERVICE(StringConstants.CLOUD_SERVICE, EntityType.CLOUD_SERVICE),
    /** ComputeTier entity type. */
    COMPUTE_TIER(StringConstants.COMPUTE_TIER, EntityType.COMPUTE_TIER),
    /** Container entity type. */
    CONTAINER(StringConstants.CONTAINER, EntityType.CONTAINER),
    /** ContainerPod entity type. */
    CONTAINER_POD(StringConstants.CONTAINERPOD, EntityType.CONTAINER_POD),
    /** Database entity type. */
    DATABASE(StringConstants.DATABASE, EntityType.DATABASE),
    /** DatabaseServer entity type. */
    DATABASE_SERVER(StringConstants.DATABASE_SERVER, EntityType.DATABASE_SERVER),
    /** DatabaseServerTier entity type. */
    DATABASE_SERVER_TIER(StringConstants.DATABASE_SERVER_TIER, EntityType.DATABASE_SERVER_TIER),
    /** DatabaseTier entity type. */
    DATABASE_TIER(StringConstants.DATABASE_TIER, EntityType.DATABASE_TIER),
    /** DataCenter entity type. */
    DATACENTER(StringConstants.DATA_CENTER, EntityType.DATACENTER),
    /** DesktopPool entity type. */
    DESKTOP_POOL(StringConstants.DESKTOP_POOL, EntityType.DESKTOP_POOL),
    /** DiskArray entity type. */
    DISKARRAY(StringConstants.DISK_ARRAY, EntityType.DISK_ARRAY),
    /** DPod entity type. */
    DPOD(StringConstants.DPOD, EntityType.DPOD),
    /** Hypervisor Server entity type. */
    HYPERVISOR_SERVER(StringConstants.HYPERVISOR_SERVER, EntityType.HYPERVISOR_SERVER),
    /** Internet entity type. */
    INTERNET(StringConstants.INTERNET, EntityType.INTERNET),
    /** IOModule entity type. */
    IOMODULE(StringConstants.IO_MODULE, EntityType.IO_MODULE),
    /** LoadBalancer entity type. */
    LOAD_BALANCER(StringConstants.LOAD_BALANCER, EntityType.LOAD_BALANCER),
    /** LogicalPool entity type. */
    LOGICALPOOL(StringConstants.LOGICAL_POOL, EntityType.LOGICAL_POOL),
    /** Network entity type. */
    NETWORK(StringConstants.NETWORK, EntityType.NETWORK),
    /** PhysicalMachine entity type. */
    PHYSICAL_MACHINE(StringConstants.PHYSICAL_MACHINE, EntityType.PHYSICAL_MACHINE),
    /** ProcessorPool entity type. */
    PROCESSOR_POOL(StringConstants.PROCESSOR_POOL, EntityType.PROCESSOR_POOL),
    /** Region entity type. */
    REGION(StringConstants.REGION, EntityType.REGION),
    /** ReservedInstance entity type. */
    RESERVED_INSTANCE(StringConstants.RESERVED_INSTANCE, EntityType.RESERVED_INSTANCE),
    /** ServiceProvider entity type. */
    SERVICE_PROVIDER(StringConstants.SERVICE_PROVIDER, EntityType.SERVICE_PROVIDER),
    /** StorageController entity type. */
    STORAGECONTROLLER(StringConstants.STORAGE_CONTROLLER, EntityType.STORAGE_CONTROLLER),
    /** Storage entity type. */
    STORAGE(StringConstants.STORAGE, EntityType.STORAGE),
    /** StorageTier entity type. */
    STORAGE_TIER(StringConstants.STORAGE_TIER, EntityType.STORAGE_TIER),
    /** Switch entity type. */
    SWITCH(StringConstants.SWITCH, EntityType.SWITCH),
    /** Unknown entity type. */
    UNKNOWN(StringConstants.UNKNOWN, EntityType.UNKNOWN),
    /** ViewPod entity type. */
    VIEW_POD(StringConstants.VIEW_POD, EntityType.VIEW_POD),
    /** VirtualApplication entity type. */
    VIRTUAL_APPLICATION(StringConstants.VIRTUAL_APPLICATION, EntityType.VIRTUAL_APPLICATION),
    /** VirtualDataCenter entity type. */
    VIRTUAL_DATACENTER(StringConstants.VDC, EntityType.VIRTUAL_DATACENTER),
    /** VirtualMachine entity type. */
    VIRTUAL_MACHINE(StringConstants.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE),
    /** VirtualVolume entity type. */
    VIRTUAL_VOLUME(StringConstants.VIRTUAL_VOLUME, EntityType.VIRTUAL_VOLUME),
    /** VPod entity type. */
    VPOD(StringConstants.VPOD, EntityType.VPOD);

    /**
     * These are the entity types that count as "Workloads" in our system.
     *
     * <p>Workloads are "arbitrary units of functionality", but we use it to encapsulate certain
     * important entity types for management and licensing purposes.
     */
    public static final Set<ApiEntityType> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(
            ApiEntityType.VIRTUAL_MACHINE, ApiEntityType.DATABASE, ApiEntityType.DATABASE_SERVER);

    /**
     * For these entity types exist information about cost in cost component.
     */
    public static final ImmutableSet<String> ENTITY_TYPES_WITH_COST = ImmutableSet.of(
            ApiEntityType.VIRTUAL_MACHINE.apiStr(),
            ApiEntityType.DATABASE.apiStr(),
            ApiEntityType.DATABASE_SERVER.apiStr(),
            ApiEntityType.VIRTUAL_VOLUME.apiStr()
    );


    private final String apiStr;

    private final EntityType type;

    private final String displayName;

    ApiEntityType(String apiStr, EntityType type) {
        this.apiStr = apiStr;
        this.type = type;
        this.displayName = StringUtil.beautifyString(type.name());
    }

    /**
     * Get the string used in the API to represent this entity type.
     *
     * @return the API string
     */
    public String apiStr() {
        return apiStr;
    }

    /**
     * Get the SDK entity type number corresponding this entity type.
     *
     * @return the SDK entity type number
     */
    public int typeNumber() {
        return type.getNumber();
    }

    /**
     * Get the entity type name rendered for human consumption.
     *
     * @return the rendered type name
     */
    public String displayName() {
        return displayName;
    }

    /**
     * Get the SDK entity type corresponding to this entity type.
     *
     * @return the SDK entity type
     */
    @Nonnull
    public EntityType sdkType() {
        return type;
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    private static final BiMap<Integer, ApiEntityType> ENTITY_TYPE_MAPPINGS;
    private static final BiMap<String, ApiEntityType> ENTITY_STR_MAPPINGS;

    static {
        ImmutableBiMap.Builder<Integer, ApiEntityType> entityTypeMappingBldr = new ImmutableBiMap.Builder<>();
        ImmutableBiMap.Builder<String, ApiEntityType> entityStrMappingBldr = new ImmutableBiMap.Builder<>();
        for (ApiEntityType type : ApiEntityType.values()) {
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
     * @return The corresponding entity type string in UI
     */
    @Nonnull
    public static ApiEntityType fromType(final int type) {
        return ENTITY_TYPE_MAPPINGS.getOrDefault(type, ApiEntityType.UNKNOWN);
    }

    /**
     * Get the {@link ApiEntityType} corresponding to the given {@link TopologyEntityDTO}.
     *
     * @param entity the {@link TopologyEntityDTO}
     * @return the {@link ApiEntityType}
     */
    @Nonnull
    public static ApiEntityType fromEntity(@Nonnull final TopologyEntityDTOOrBuilder entity) {
        return fromType(entity.getEntityType());
    }

    /**
     * Converts a {@link MinimalEntity} to the corresponding {@link ApiEntityType}, based on the
     * entity's type.
     *
     * @param entity The target {@link MinimalEntityOrBuilder}
     * @return The {@link ApiEntityType} of the entity
     */
    @Nonnull
    public static ApiEntityType fromMinimalEntity(@Nonnull MinimalEntityOrBuilder entity) {
        return fromType(entity.getEntityType());
    }

    /**
     * Converts type from a string to the enum type.
     *
     * @param type string representation of service entity type
     * @return UI entity type enum
     */
    @Nonnull
    public static ApiEntityType fromString(String type) {
        return ENTITY_STR_MAPPINGS.getOrDefault(type, ApiEntityType.UNKNOWN);
    }

    /**
     * Converts from a string to the {@link EntityType}.
     *
     * @param type string representation of service entity type
     * @return int numeric representation of EnitityType
     */
    public static int fromStringToSdkType(String type) {
        return fromString(type).typeNumber();
    }

    /**
     * Converts from a {@link EntityType} numeric value to {@link ApiEntityType} string.
     *
     * @param sdkType numeric representation of EnitityType
     * @return type String representation of service entity type
     */
    @Nonnull
    public static String fromSdkTypeToEntityTypeString(int sdkType) {
        return fromType(sdkType).apiStr();
    }
}
