package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.GroupType;
import com.vmturbo.extractor.schema.enums.EntityType;

/**
 * Utility for mapping API EntityType/GroupType Enums to jooq EntityTypeEnums.
 */
public class TypeMapper {

    /**
     * Jooq Type mappings to entityType and GroupType.
     */
    private static final BiMap<EntityType, Enum<?>> TYPE_MAPPINGS =
            new ImmutableBiMap.Builder()
            .putAll(EntityTypeMapper.ENTITY_TYPE_MAPPINGS)
            .putAll(GroupTypeMapper.GROUP_TYPE_MAPPINGS)
            .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private TypeMapper() {}

    /**
     * Get the {@link EntityType} or {@link GroupType} associated with a {@link EntityType}.
     * @param entityType The {@link EntityType}.
     * @return The associated {@link EntityType} or {@link GroupType} or null
     */
    public static Enum<?> fromSearchSchemaToApi(@Nullable final EntityType entityType) {
        return TYPE_MAPPINGS.getOrDefault(entityType, null);
    }

    /**
     * Get the {@link EntityType} associated with a {@link GroupType}.
     *
     * @param entityOrGroupType The {@link EntityType} or {@link GroupType}.
     * @return The associated {@link EntityType}, or null.
     */
    public static EntityType fromApiToSearchSchema(@Nullable final Enum<?> entityOrGroupType) {
        EntityType entityType = TYPE_MAPPINGS.inverse().getOrDefault(entityOrGroupType, null);

        if (entityType == null) {
            throw new IllegalArgumentException("Unsupporterd type :" + entityOrGroupType.toString());
        }
        return TYPE_MAPPINGS.inverse().getOrDefault(entityOrGroupType, null);
    }

    /**
     * Functional Interface of {@link TypeMapper#fromSearchSchemaToApi}.
     */
    public static final Function<EntityType, Enum<?>>
            fromSearchSchemaToApiFunction = TypeMapper::fromSearchSchemaToApi;


    /**
     * Functional Interface of {@link TypeMapper#fromApiToSearchSchema}.
     */
    public static final Function<Enum<?>, EntityType>
            fromApiToSearchSchemaFunction = TypeMapper::fromApiToSearchSchema;
}
