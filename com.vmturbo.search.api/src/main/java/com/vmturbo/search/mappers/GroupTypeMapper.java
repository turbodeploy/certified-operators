package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.GroupType;
import com.vmturbo.extractor.schema.enums.EntityType;

/**
 * Utility for mapping been ENUMs {@link GroupType} and {@link EntityType}.
 * Note: Entity type and group type are stored in the same DB column, thus the JOOQ EntityType enum
 *   also includes group types.
 */
public class GroupTypeMapper {

    /**
     * Mappings between {@link EntityType} and {@link GroupType}.
     */
    private static final BiMap<EntityType, GroupType> GROUP_TYPE_MAPPINGS =
        new ImmutableBiMap.Builder()
            .put( EntityType.GROUP, GroupType.GROUP)
            .put( EntityType.BILLING_FAMILY, GroupType.BILLING_FAMILY)
            .put( EntityType.COMPUTE_CLUSTER, GroupType.COMPUTE_HOST_CLUSTER)
            .put( EntityType.K8S_CLUSTER, GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
            .put( EntityType.RESOURCE_GROUP, GroupType.RESOURCE)
            .put( EntityType.STORAGE_CLUSTER, GroupType.STORAGE_CLUSTER)
            .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private GroupTypeMapper() {}

    /**
     * Get the {@link GroupType} associated with a
     * {@link EntityType}.
     *
     * @param entityType The {@link EntityType}.
     * @return The associated {@link GroupType}, or null
     */
    public static GroupType fromSearchSchemaToApi(@Nullable final EntityType entityType) {
        return GROUP_TYPE_MAPPINGS.getOrDefault(entityType, null);
    }

    /**
     * Get the {@link EntityType} associated with a {@link GroupType}.
     *
     * @param groupType The {@link GroupType}.
     * @return The associated {@link EntityType}, or null.
     */
    public static EntityType fromApiToSearchSchema(@Nullable final GroupType groupType) {
        return GROUP_TYPE_MAPPINGS.inverse().getOrDefault(groupType, null);
    }


    /**
     * Functional Interface of {@link GroupTypeMapper#fromSearchSchemaToApi}.
     */
    public static final Function<EntityType, GroupType>
        fromSearchSchemaToApiFunction = GroupTypeMapper::fromSearchSchemaToApi;


    /**
     * Functional Interface of {@link GroupTypeMapper#fromApiToSearchSchema}.
     */
    public static final Function<GroupType, EntityType>
        fromApiToSearchSchemaFunction = GroupTypeMapper::fromApiToSearchSchema;
}
