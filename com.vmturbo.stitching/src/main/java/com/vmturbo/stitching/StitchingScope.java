package com.vmturbo.stitching;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A {@link StitchingScope} determines which entities will be fed to the
 * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)} method.
 *
 * @param <ENTITY> The class of entity that stitching will operate on. Examples are {@link StitchingEntity}
 *                 or {@link TopologyEntity}.
 */
public interface StitchingScope<ENTITY> {
    /**
     * Get the entities in the stitching scope.
     *
     * @return A stream of the entities in the stitching scope.
     */
    @Nonnull
    Stream<ENTITY> entities();

    /**
     * A factory for construction {@link StitchingScope}s.
     *
     * Use this factory in {@link PreStitchingOperation#getScope(StitchingScopeFactory)} to
     * construct the appropriate scope for the calculation.
     */
    interface StitchingScopeFactory<ENTITY> {
        /**
         * Return a global calculation scope. Calculations with this scope will operate on all entities
         * in the entire topology.
         *
         * @return A global {@link StitchingScope}.
         */
        StitchingScope<ENTITY> globalScope();

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only
         * entities discovered by a specific probe type.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @return A probe type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> probeScope(@Nonnull final String probeTypeName);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given entity type. Calculations with this scope will be fed all entities of a given
         * {@link EntityType} regardless of which target or probe discovered those entities.
         *
         * @param entityType The entity type of the entities
         * @return An entity type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> entityTypeScope(@Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of the given entity types. Calculations with this scope will be fed all entities which match
         * the list of the the given {@link EntityType}s regardless of which target or probe discovered
         * those entities.
         *
         * @param entityTypes List of entity type of the entities
         * @return An entity type {@link StitchingScope}.
         */
        StitchingScope<ENTITY> multiEntityTypesScope(@Nonnull final List<EntityType> entityTypes);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         type of probe.
         */
        StitchingScope<ENTITY> probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                    @Nonnull final EntityType entityType);

        /**
         * Return a {@link StitchingScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe category.
         *
         * @param probeCategory The category of probe whose entities should be fed to the calculation.
         *                      Example: HYPERVISOR or STORAGE
         * @param entityType The entity type of the entities
         * @return A {@link StitchingScope} used for retrieving entities of a given type discovered by a given
         *         category of probe.
         */
        StitchingScope<ENTITY> probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                            @Nonnull final EntityType entityType);
    }
}
