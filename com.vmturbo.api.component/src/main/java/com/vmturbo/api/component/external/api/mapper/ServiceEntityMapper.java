package com.vmturbo.api.component.external.api.mapper;

import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ServiceEntityMapper {

    /**
     * The entity types used in UI.
     */
    public enum UIEntityType {
        VIRTUAL_MACHINE("VirtualMachine"),
        PHYSICAL_MACHINE("PhysicalMachine"),
        STORAGE("Storage"),
        DATACENTER("DataCenter"),
        DISKARRAY("DiskArray"),
        VIRTUAL_DATACENTER("VirtualDataCenter"),
        BUSINESS_APPLICATION("BusinessApplication"),
        APPLICATION_SERVER("ApplicationServer"),
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
        PROCESSOR_POOL("ProcessorPool"),
        UNKNOWN("Unknown");

        private final String value;

        UIEntityType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Converts type from a string to the enum type.
         * @param type string representation of service entity type
         * @return UI entity type enum
         */
        public static UIEntityType fromString(String type) {
            if (type != null) {
                for (UIEntityType t : UIEntityType.values()) {
                    if (type.equals(t.value)) {
                        return t;
                    }
                }
            }

            throw new IllegalArgumentException(
                       "No UIEntityType constant with type " + type + " found");
        }
    }

    /**
     * Mappings between entityType enum values in TopologyEntityDTO to strings that UI
     * understands.
     */
    static final BiMap<Integer, UIEntityType> ENTITY_TYPE_MAPPINGS =
               new ImmutableBiMap.Builder<Integer, UIEntityType>()
            .put(EntityType.VIRTUAL_MACHINE.getNumber(),        UIEntityType.VIRTUAL_MACHINE)
            .put(EntityType.PHYSICAL_MACHINE.getNumber(),       UIEntityType.PHYSICAL_MACHINE)
            .put(EntityType.STORAGE.getNumber(),                UIEntityType.STORAGE)
            .put(EntityType.DISK_ARRAY.getNumber(),             UIEntityType.DISKARRAY)
            .put(EntityType.DATACENTER.getNumber(),             UIEntityType.DATACENTER)
            .put(EntityType.VIRTUAL_DATACENTER.getNumber(),     UIEntityType.VIRTUAL_DATACENTER)
            .put(EntityType.BUSINESS_APPLICATION.getNumber(),   UIEntityType.BUSINESS_APPLICATION)
            .put(EntityType.APPLICATION_SERVER.getNumber(),     UIEntityType.APPLICATION_SERVER)
            .put(EntityType.APPLICATION.getNumber(),            UIEntityType.APPLICATION)
            .put(EntityType.VIRTUAL_APPLICATION.getNumber(),    UIEntityType.VIRTUAL_APPLICATION)
            .put(EntityType.CONTAINER.getNumber(),              UIEntityType.CONTAINER)
            .put(EntityType.CONTAINER_POD.getNumber(),          UIEntityType.CONTAINER_POD)
            .put(EntityType.STORAGE_CONTROLLER.getNumber(),     UIEntityType.STORAGECONTROLLER)
            .put(EntityType.IO_MODULE.getNumber(),              UIEntityType.IOMODULE)
            .put(EntityType.INTERNET.getNumber(),               UIEntityType.INTERNET)
            .put(EntityType.SWITCH.getNumber(),                 UIEntityType.SWITCH)
            .put(EntityType.CHASSIS.getNumber(),                UIEntityType.CHASSIS)
            .put(EntityType.NETWORK.getNumber(),                UIEntityType.NETWORK)
            .put(EntityType.LOGICAL_POOL.getNumber(),           UIEntityType.LOGICALPOOL)
            .put(EntityType.DATABASE.getNumber(),               UIEntityType.DATABASE)
            .put(EntityType.DATABASE_SERVER.getNumber(),        UIEntityType.DATABASE_SERVER)
            .put(EntityType.LOAD_BALANCER.getNumber(),          UIEntityType.LOAD_BALANCER)
            .put(EntityType.BUSINESS_ACCOUNT.getNumber(),       UIEntityType.BUSINESS_ACCOUNT)
            .put(EntityType.CLOUD_SERVICE.getNumber(),          UIEntityType.CLOUD_SERVICE)
            .put(EntityType.COMPUTE_TIER.getNumber(),           UIEntityType.COMPUTE_TIER)
            .put(EntityType.STORAGE_TIER.getNumber(),           UIEntityType.STORAGE_TIER)
            .put(EntityType.DATABASE_TIER.getNumber(),          UIEntityType.DATABASE_TIER)
            .put(EntityType.DATABASE_SERVER_TIER.getNumber(),   UIEntityType.DATABASE_SERVER_TIER)
            .put(EntityType.AVAILABILITY_ZONE.getNumber(),      UIEntityType.AVAILABILITY_ZONE)
            .put(EntityType.REGION.getNumber(),                 UIEntityType.REGION)
            .put(EntityType.VIRTUAL_VOLUME.getNumber(),         UIEntityType.VIRTUAL_VOLUME)
            .put(EntityType.PROCESSOR_POOL.getNumber(),         UIEntityType.PROCESSOR_POOL)
            .put(EntityType.UNKNOWN.getNumber(),                UIEntityType.UNKNOWN)
            .build();

    /**
     * Maps the entity type in TopologyEntityDTO to strings of entity types used in UI.
     *
     * @param type The entity type in the TopologyEntityDTO
     * @return     The corresponding entity type string in UI
     */
    public static String toUIEntityType(final int type) {
        final UIEntityType uiEntityType = ENTITY_TYPE_MAPPINGS.get(type);

        if (uiEntityType == null) {
            return EntityType.forNumber(type).toString();
        }

        return uiEntityType.getValue();
    }

    /**
     * Maps the entity type used in UI to the type used in TopologyEntityDTO.
     *
     * @param uiEntityType The entity type string used in UI
     * @return The type used in TopologyEntityDTO
     */
    public static int fromUIEntityType(@Nonnull final String uiEntityType) {
        return ENTITY_TYPE_MAPPINGS.inverse()
                        .getOrDefault(UIEntityType.fromString(uiEntityType),
                                      EntityType.UNKNOWN.getNumber());
    }

    /**
     * Copy the the basic fields of a {@link BaseApiDTO} object from a {@link TopologyEntityDTO}
     * object.  Basic fields are: display name, class name, and uuid.
     *
     * @param baseApiDTO the object whose basic fields are to be set.
     * @param topologyEntityDTO the object whose basic fields are to be copied.
     */
    public static void setBasicFields(
            @Nonnull BaseApiDTO baseApiDTO,
            @Nonnull TopologyEntityDTO topologyEntityDTO) {
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(ServiceEntityMapper.toUIEntityType(topologyEntityDTO.getEntityType()));
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
    }

    /**
     * Convert a {@link TopologyEntityDTO} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API
     *
     * @param topologyEntityDTO the internal {@link TopologyEntityDTO} to convert
     * @param aspectMapper aspect mapper to use for including aspects.
     *                     if null, then no aspects will be returned.
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public static ServiceEntityApiDTO toServiceEntityApiDTO(
            @Nonnull TopologyEntityDTO topologyEntityDTO,
            @Nullable EntityAspectMapper aspectMapper) {
        // basic information
        final ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        setBasicFields(seDTO, topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            seDTO.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).getValue());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(seDTO::setEnvironmentType);
        }

        // aspects, if required
        if (aspectMapper != null) {
            seDTO.setAspects(aspectMapper.getAspectsByEntity(topologyEntityDTO));
        }

        //tags
        seDTO.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getValuesList())));

        return seDTO;
    }

    /**
     * Creates a shallow clone of a {@link ServiceEntityApiDTO} object.
     *
     * @param serviceEntityApiDTO the object to clone.
     * @return the new object.
     */
    @Nonnull
    public static ServiceEntityApiDTO copyServiceEntityAPIDTO(
            @Nonnull ServiceEntityApiDTO serviceEntityApiDTO) {
        // basic information
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.setDisplayName(Objects.requireNonNull(serviceEntityApiDTO).getDisplayName());
        result.setClassName(serviceEntityApiDTO.getClassName());
        result.setUuid(serviceEntityApiDTO.getUuid());
        result.setState(serviceEntityApiDTO.getState());
        result.setEnvironmentType(serviceEntityApiDTO.getEnvironmentType());

        // aspects, if required
        result.setAspects(serviceEntityApiDTO.getAspects());

        //tags
        result.setTags(serviceEntityApiDTO.getTags());

        return result;
    }
}
