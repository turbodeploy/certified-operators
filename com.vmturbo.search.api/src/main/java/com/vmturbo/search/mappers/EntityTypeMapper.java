package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.EntityType;

/**
 * Utility for mapping between ENUMs {@link com.vmturbo.api.enums.EntityType} and {@link EntityType}.
 */
public class EntityTypeMapper {

    /**
     * Mappings between {@link EntityType} and {@link com.vmturbo.api.enums.EntityType}.
     */
    protected static final BiMap<EntityType, com.vmturbo.api.enums.EntityType> ENTITY_TYPE_MAPPINGS =
        new ImmutableBiMap.Builder()
            .put( EntityType.APPLICATION, com.vmturbo.api.enums.EntityType.Application)
            .put( EntityType.APPLICATION_COMPONENT, com.vmturbo.api.enums.EntityType.ApplicationComponent)
            .put( EntityType.BUSINESS_ACCOUNT, com.vmturbo.api.enums.EntityType.BusinessAccount)
            .put( EntityType.BUSINESS_APPLICATION, com.vmturbo.api.enums.EntityType.BusinessApplication)
            .put( EntityType.BUSINESS_USER, com.vmturbo.api.enums.EntityType.BusinessUser)
            .put( EntityType.BUSINESS_TRANSACTION, com.vmturbo.api.enums.EntityType.BusinessTransaction)
            .put( EntityType.CHASSIS, com.vmturbo.api.enums.EntityType.Chassis)
            .put( EntityType.CONTAINER, com.vmturbo.api.enums.EntityType.Container)
            .put( EntityType.CONTAINER_POD, com.vmturbo.api.enums.EntityType.ContainerPod)
            .put( EntityType.CONTAINER_SPEC, com.vmturbo.api.enums.EntityType.ContainerSpec)
            .put( EntityType.DATABASE, com.vmturbo.api.enums.EntityType.Database)
            .put( EntityType.DATABASE_SERVER, com.vmturbo.api.enums.EntityType.DatabaseServer)
            .put( EntityType.DATACENTER, com.vmturbo.api.enums.EntityType.DataCenter)
            .put( EntityType.DESKTOP_POOL, com.vmturbo.api.enums.EntityType.DesktopPool)
            .put( EntityType.DISK_ARRAY, com.vmturbo.api.enums.EntityType.DiskArray)
            .put( EntityType.IO_MODULE, com.vmturbo.api.enums.EntityType.IOModule)
            .put( EntityType.NETWORK, com.vmturbo.api.enums.EntityType.Network)
            .put( EntityType.NAMESPACE, com.vmturbo.api.enums.EntityType.Namespace)
            .put( EntityType.PHYSICAL_MACHINE, com.vmturbo.api.enums.EntityType.PhysicalMachine)
            .put( EntityType.REGION, com.vmturbo.api.enums.EntityType.Region)
            .put( EntityType.SERVICE, com.vmturbo.api.enums.EntityType.Service)
            .put( EntityType.STORAGE, com.vmturbo.api.enums.EntityType.Storage)
            .put( EntityType.STORAGE_CONTROLLER, com.vmturbo.api.enums.EntityType.StorageController)
            .put( EntityType.SWITCH, com.vmturbo.api.enums.EntityType.Switch)
            .put( EntityType.VIEW_POD, com.vmturbo.api.enums.EntityType.ViewPod)
            .put( EntityType.VIRTUAL_DATACENTER, com.vmturbo.api.enums.EntityType.VirtualDataCenter)
            .put( EntityType.VIRTUAL_MACHINE, com.vmturbo.api.enums.EntityType.VirtualMachine)
            .put( EntityType.VIRTUAL_VOLUME, com.vmturbo.api.enums.EntityType.VirtualVolume)
            .put( EntityType.WORKLOAD_CONTROLLER, com.vmturbo.api.enums.EntityType.WorkloadController)
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
        return ENTITY_TYPE_MAPPINGS.getOrDefault(entityType, com.vmturbo.api.enums.EntityType.Unknown);
    }

    /**
     * Get the {@link EntityType} associated with a {@link com.vmturbo.api.enums.EntityType}.
     *
     * @param entityType The {@link EntityType}.
     * @return The associated {@link EntityType}, or null.
     */
    public static EntityType fromApiToSearchSchema(@Nullable final com.vmturbo.api.enums.EntityType entityType) {
        return ENTITY_TYPE_MAPPINGS.inverse().getOrDefault(entityType, null);
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

