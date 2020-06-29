package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.EntitySeverity;

/**
 * Utility for mapping been ENUMs {@link com.vmturbo.api.enums.EntitySeverity} and {@link EntitySeverity}.
 */
public class EntitySeverityMapper {
    /**
     * Mappings between {@link EntitySeverity} and {@link com.vmturbo.api.enums.EntitySeverity}.
     */
    private static final BiMap<EntitySeverity, com.vmturbo.api.enums.EntitySeverity> ENTITY_STATE_MAPPINGS =
        new ImmutableBiMap.Builder()
            .put(EntitySeverity.CRITICAL, com.vmturbo.api.enums.EntitySeverity.CRITICAL )
            .put(EntitySeverity.MAJOR, com.vmturbo.api.enums.EntitySeverity.MAJOR )
            .put(EntitySeverity.MINOR, com.vmturbo.api.enums.EntitySeverity.MINOR )
            .put(EntitySeverity.NORMAL, com.vmturbo.api.enums.EntitySeverity.NORMAL )
            .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private EntitySeverityMapper() {}

    /**
     * Get the {@link com.vmturbo.api.enums.EntitySeverity} associated with a {@link
     * com.vmturbo.extractor.schema.enums.EntitySeverity}.
     *
     * @param entitySeverity The {@link EntitySeverity}.
     * @return The associated {@link com.vmturbo.api.enums.EntitySeverity}, or null
     */
    @Nonnull
    public static com.vmturbo.api.enums.EntitySeverity fromSearchSchemaToApi(@Nullable final EntitySeverity entitySeverity) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(entitySeverity, null);
    }

    /**
     * Get the {@link EntitySeverity} associated with a {@link com.vmturbo.api.enums.EntitySeverity}.
     *
     * @param entitySeverity The {@link EntitySeverity}.
     * @return The associated {@link EntitySeverity}, or null.
     */
    @Nonnull
    public static EntitySeverity fromApiToSearchSchema(@Nullable final com.vmturbo.api.enums.EntitySeverity entitySeverity) {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(entitySeverity, null);
    }

    /**
     * Functional Interface of {@link EntitySeverityMapper#fromSearchSchemaToApi}
     */
    public static final Function<EntitySeverity, com.vmturbo.api.enums.EntitySeverity>
        fromSearchSchemaToApiFunction = (en) -> EntitySeverityMapper.fromSearchSchemaToApi(en);


    /**
     * Functional Interface of {@link EntitySeverityMapper#fromApiToSearchSchema}
     */
    public static final Function<com.vmturbo.api.enums.EntitySeverity, EntitySeverity>
        fromApiToSearchSchemaFunction = (en) -> EntitySeverityMapper.fromApiToSearchSchema(en);

}
