package com.vmturbo.search.metadata;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Mapping between {@link com.vmturbo.api.enums.EntityType},
 * {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType} and jooq EntityType.
 * todo: coordinate with Viktor's mappers.
 */
public class EntityTypeMapper {

    /**
     * Mapping between {@link com.vmturbo.api.enums.EntityType} and
     * {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}.
     * It contains all the supported entity types which are stored in search db.
     */
    public static final BiMap<EntityType, EntityDTO.EntityType> SUPPORTED_ENTITY_TYPE_MAPPING =
            new ImmutableBiMap.Builder<com.vmturbo.api.enums.EntityType, EntityDTO.EntityType>()
                    .put(com.vmturbo.api.enums.EntityType.APPLICATION, EntityDTO.EntityType.APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.APPLICATION_COMPONENT, EntityDTO.EntityType.APPLICATION_COMPONENT)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_ACCOUNT, EntityDTO.EntityType.BUSINESS_ACCOUNT)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_APPLICATION, EntityDTO.EntityType.BUSINESS_APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_TRANSACTION, EntityDTO.EntityType.BUSINESS_TRANSACTION)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_USER, EntityDTO.EntityType.BUSINESS_USER)
                    .put(com.vmturbo.api.enums.EntityType.CHASSIS, EntityDTO.EntityType.CHASSIS)
                    .put(com.vmturbo.api.enums.EntityType.CONTAINER, EntityDTO.EntityType.CONTAINER)
                    .put(com.vmturbo.api.enums.EntityType.CONTAINER_POD, EntityDTO.EntityType.CONTAINER_POD)
                    .put(com.vmturbo.api.enums.EntityType.DATABASE, EntityDTO.EntityType.DATABASE)
                    .put(com.vmturbo.api.enums.EntityType.DATABASE_SERVER, EntityDTO.EntityType.DATABASE_SERVER)
                    .put(com.vmturbo.api.enums.EntityType.DATACENTER, EntityDTO.EntityType.DATACENTER)
                    .put(com.vmturbo.api.enums.EntityType.DESKTOP_POOL, EntityDTO.EntityType.DESKTOP_POOL)
                    .put(com.vmturbo.api.enums.EntityType.DISKARRAY, EntityDTO.EntityType.DISK_ARRAY)
                    .put(com.vmturbo.api.enums.EntityType.IOMODULE, EntityDTO.EntityType.IO_MODULE)
                    .put(com.vmturbo.api.enums.EntityType.NETWORK, EntityDTO.EntityType.NETWORK)
                    .put(com.vmturbo.api.enums.EntityType.REGION, EntityDTO.EntityType.REGION)
                    .put(com.vmturbo.api.enums.EntityType.PHYSICAL_MACHINE, EntityDTO.EntityType.PHYSICAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.SERVICE, EntityDTO.EntityType.SERVICE)
                    .put(com.vmturbo.api.enums.EntityType.STORAGE, EntityDTO.EntityType.STORAGE)
                    .put(com.vmturbo.api.enums.EntityType.STORAGECONTROLLER, EntityDTO.EntityType.STORAGE_CONTROLLER)
                    .put(com.vmturbo.api.enums.EntityType.SWITCH, EntityDTO.EntityType.SWITCH)
                    .put(com.vmturbo.api.enums.EntityType.VIRTUAL_MACHINE, EntityDTO.EntityType.VIRTUAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.VIEW_POD, EntityDTO.EntityType.VIEW_POD)
                    .put(com.vmturbo.api.enums.EntityType.VIRTUAL_VOLUME, EntityDTO.EntityType.VIRTUAL_VOLUME)
                    .build();

    /**
     * Convert an {@link EntityType} to {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}.
     *
     * @param apiEntityType api {@link EntityType}
     * @return {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}
     */
    public static EntityDTO.EntityType fromApiEntityTypeToProto(
            com.vmturbo.api.enums.EntityType apiEntityType) {
        EntityDTO.EntityType protoEntityType = SUPPORTED_ENTITY_TYPE_MAPPING.get(apiEntityType);
        if (protoEntityType != null) {
            return protoEntityType;
        }
        throw new IllegalArgumentException("Unsupported api EntityType: " + apiEntityType);
    }
}