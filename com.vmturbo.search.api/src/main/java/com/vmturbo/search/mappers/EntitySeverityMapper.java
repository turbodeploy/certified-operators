package com.vmturbo.search.mappers;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.extractor.schema.enums.Severity;

/**
 * Utility for mapping been ENUMs {@link com.vmturbo.api.enums.EntitySeverity} and {@link Severity}.
 */
public class EntitySeverityMapper {
    /**
     * Mappings between {@link Severity} and {@link com.vmturbo.api.enums.EntitySeverity}.
     */
    private static final BiMap<Severity, com.vmturbo.api.enums.EntitySeverity> ENTITY_STATE_MAPPINGS =
        new ImmutableBiMap.Builder()
            .put(Severity.CRITICAL, com.vmturbo.api.enums.EntitySeverity.CRITICAL )
            .put(Severity.MAJOR, com.vmturbo.api.enums.EntitySeverity.MAJOR )
            .put(Severity.MINOR, com.vmturbo.api.enums.EntitySeverity.MINOR )
            .put(Severity.NORMAL, com.vmturbo.api.enums.EntitySeverity.NORMAL )
            .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private EntitySeverityMapper() {}

    /**
     * Get the {@link com.vmturbo.api.enums.EntitySeverity} associated with a {@link
     * com.vmturbo.extractor.schema.enums.Severity}.
     *
     * @param entitySeverity The {@link Severity}.
     * @return The associated {@link com.vmturbo.api.enums.EntitySeverity}, or null
     */
    @Nonnull
    public static com.vmturbo.api.enums.EntitySeverity fromSearchSchemaToApi(@Nullable final Severity entitySeverity) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(entitySeverity, null);
    }

    /**
     * Get the {@link Severity} associated with a {@link com.vmturbo.api.enums.EntitySeverity}.
     *
     * @param entitySeverity The {@link Severity}.
     * @return The associated {@link Severity}, or null.
     */
    @Nonnull
    public static Severity fromApiToSearchSchema(@Nullable final com.vmturbo.api.enums.EntitySeverity entitySeverity) {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(entitySeverity, null);
    }

    /**
     * Functional Interface of {@link EntitySeverityMapper#fromSearchSchemaToApi}
     */
    public static final Function<Severity, com.vmturbo.api.enums.EntitySeverity>
        fromSearchSchemaToApiFunction = (en) -> EntitySeverityMapper.fromSearchSchemaToApi(en);


    /**
     * Functional Interface of {@link EntitySeverityMapper#fromApiToSearchSchema}
     */
    public static final Function<com.vmturbo.api.enums.EntitySeverity, Severity>
        fromApiToSearchSchemaFunction = (en) -> EntitySeverityMapper.fromApiToSearchSchema(en);

}
