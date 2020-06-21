package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.EntityType;

/**
 * Utility for mapping been ENUMs {@link com.vmturbo.api.enums.EntityType} and {@link EntityType}.
 */
public class EntityTypeMapper {

    //TODO: Search db Schema EntityType also contains enums for group types
    //We will have to add another map of searcdb.enums.entityType to api.groupEntityTypes when available
    /**
     * Mappings between {@link EntityType} and {@link com.vmturbo.api.enums.EntityType}.
     */
    private static final BiMap<EntityType, com.vmturbo.api.enums.EntityType> ENTITY_STATE_MAPPINGS =
        new ImmutableBiMap.Builder()
            .put( EntityType.SWITCH, com.vmturbo.api.enums.EntityType.SWITCH)
            .put( EntityType.STORAGE, com.vmturbo.api.enums.EntityType.STORAGE)
            .put( EntityType.VIRTUAL_MACHINE, com.vmturbo.api.enums.EntityType.VIRTUAL_MACHINE)
            .put( EntityType.DISK_ARRAY, com.vmturbo.api.enums.EntityType.DISKARRAY)
            .put( EntityType.DATACENTER, com.vmturbo.api.enums.EntityType.DATACENTER)
            .put( EntityType.PHYSICAL_MACHINE, com.vmturbo.api.enums.EntityType.PHYSICAL_MACHINE)
            .put( EntityType.CHASSIS, com.vmturbo.api.enums.EntityType.CHASSIS)
            .put( EntityType.BUSINESS_USER, com.vmturbo.api.enums.EntityType.BUSINESS_USER)
            .put( EntityType.STORAGE_CONTROLLER, com.vmturbo.api.enums.EntityType.STORAGECONTROLLER)
            .put( EntityType.IO_MODULE, com.vmturbo.api.enums.EntityType.IOMODULE)
            .put( EntityType.BUSINESS_ACCOUNT, com.vmturbo.api.enums.EntityType.BUSINESS_ACCOUNT)
            .put( EntityType.NETWORK, com.vmturbo.api.enums.EntityType.NETWORK)
            .put( EntityType.APPLICATION, com.vmturbo.api.enums.EntityType.APPLICATION)
            .put( EntityType.CONTAINER, com.vmturbo.api.enums.EntityType.CONTAINER)
            .put( EntityType.CONTAINER_POD, com.vmturbo.api.enums.EntityType.CONTAINER_POD)
            .put( EntityType.DATABASE, com.vmturbo.api.enums.EntityType.DATABASE)
            .put( EntityType.REGION, com.vmturbo.api.enums.EntityType.REGION)
            .put( EntityType.VIRTUAL_VOLUME, com.vmturbo.api.enums.EntityType.VIRTUAL_VOLUME)
            .put( EntityType.VIEW_POD, com.vmturbo.api.enums.EntityType.VIEW_POD)
            .put( EntityType.DESKTOP_POOL, com.vmturbo.api.enums.EntityType.DESKTOP_POOL)
            .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private EntityTypeMapper() {}

    /**
     * Get the {@link com.vmturbo.api.enums.EntityType} associated with a {@link
     * com.vmturbo.extractor.schema.enums.EntityType}.
     *
     * @param entityType The {@link EntityType}.
     * @return The associated {@link com.vmturbo.api.enums.EntityType}, or null
     */
    public static com.vmturbo.api.enums.EntityType fromSearchSchemaToApi(@Nullable final EntityType entityType) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(entityType, null);
    }

    /**
     * Get the {@link EntityType} associated with a {@link com.vmturbo.api.enums.EntityType}.
     *
     * @param entityType The {@link EntityType}.
     * @return The associated {@link EntityType}, or null.
     */
    public static EntityType fromApiToSearchSchema(@Nullable final com.vmturbo.api.enums.EntityType entityType) {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(entityType, null);
    }


    /**
     * Functional Interface of {@link EntityTypeMapper#fromSearchSchemaToApi}
     */
    public static final Function<EntityType, com.vmturbo.api.enums.EntityType>
        fromSearchSchemaToApiFunction = (en) -> EntityTypeMapper.fromSearchSchemaToApi(en);


    /**
     * Functional Interface of {@link EntityTypeMapper#fromApiToSearchSchema}
     */
    public static final Function<com.vmturbo.api.enums.EntityType, EntityType>
        fromApiToSearchSchemaFunction = (en) -> EntityTypeMapper.fromApiToSearchSchema(en);
}

