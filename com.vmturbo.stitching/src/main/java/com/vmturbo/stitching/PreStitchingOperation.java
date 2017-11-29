package com.vmturbo.stitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * A {@link PreStitchingOperation} runs prior to or after probe stitching {@link StitchingOperation}s.
 *
 * Pre-stitching Operations may be used to modify the topology or individual entities.
 *
 * {@link PreStitchingOperation} may manipulate the topology by, for example, merging multiple discovered
 * instances of a single entity down into just one instance.
 *
 * {@link PreStitchingOperation}s contrast with {@link StitchingOperation}s in that they do not perform a matching
 * phase. Instead, they specify a pre-defined scope to the calculation and then operate on the entities within
 * that scope. See {@link CalculationScope} for additional details. For the list of available pre-defined scopes
 * see {@link CalculationScopeFactory}.
 */
public interface PreStitchingOperation {
    /**
     * Get the scope for this {@link PreStitchingOperation}. The {@link CalculationScope} returned determines
     * which entities are provided as input to the {@link #performOperation(Stream, StitchingResult.Builder)}
     * method for this {@link PreStitchingOperation}. See {@link CalculationScopeFactory} for further details.
     *
     * @param calculationScopeFactory The factory to use to construct the {@link CalculationScope} for this
     *                                {@link PreStitchingOperation}.
     * @return The {@link CalculationScope} to use fo this {@link PreStitchingOperation}.
     */
    @Nonnull
    CalculationScope getCalculationScope(@Nonnull final CalculationScopeFactory calculationScopeFactory);

    /**
     * Perform this {@link PreStitchingOperation}. Any changes the calculation wants to make to
     * the entities or their relationships should be made by adding an appropriate
     * {@link StitchingResult.StitchingChange} to using the {@link StitchingResult.Builder}.
     *
     * The precise semantics of a calculation vary, but in general calculations may want to update
     * values in entity commodities or merge multiple views of a single entity discovered by
     * multiple probes down to a single view.
     *
     * Any updates to relationships in the entities must be noted in the returned {@link StitchingResult}
     * so that the graph and certain other acceleration structures that track entities and relationships
     * can be updated for further stitching.
     *
     * @param entities The entities in the calculation scope. For details on scope see
     *                 {@link #getCalculationScope(CalculationScopeFactory)}
     * @param resultBuilder A builder for the result containing the changes this operation wants to make
     *                      to the entities and their relationships. The calculation should use this builder
     *                      to create the result it returns.
     * @return A {@link StitchingResult} that describes the result of stitching. The result should be built using
     *         the {@link StitchingResult.Builder} provided as input.
     */
    @Nonnull
    StitchingResult performOperation(@Nonnull final Stream<StitchingEntity> entities,
                                     @Nonnull final StitchingResult.Builder resultBuilder);

    /**
     * A calculation scope determines which entities will be fed to the
     * {@link PreStitchingOperation#performOperation(Stream, StitchingResult.Builder)} method.
     */
    interface CalculationScope {
        /**
         * Get the entities in the calculation scope.
         *
         * @return The entities in the calculation scope.
         */
        @Nonnull
        Stream<StitchingEntity> entities();
    }

    /**
     * A factory for construction {@link CalculationScope}s.
     *
     * Use this factory in {@link PreStitchingOperation#getCalculationScope(CalculationScopeFactory)} to
     * construct the appropriate scope for the calculation.
     */
    interface CalculationScopeFactory {
        /**
         * Return a global calculation scope. Calculations with this scope will operation on all entities
         * in the entire topology.
         *
         * @return A global {@link CalculationScope}.
         */
        CalculationScope globalScope();

        /**
         * Return a {@link CalculationScope} that restricts the calculation to operate on only
         * entities discovered by a specific probe type.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @return A probe type {@link CalculationScope}.
         */
        CalculationScope probeScope(@Nonnull final String probeTypeName);

        /**
         * Return a {@link CalculationScope} that restricts the calculation to operate on only entities
         * of a given entity type. Calculations with this scope will be fed all entities of a given
         * {@link EntityType} regardless of which target or probe discovered those entities.
         *
         * @param entityType The entity type of the entities
         * @return An entity type {@link CalculationScope}.
         */
        CalculationScope entityTypeScope(@Nonnull final EntityType entityType);

        /**
         * Return a {@link CalculationScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe.
         *
         * @param probeTypeName The name of the type of probe whose entities should be fed to the calculation.
         *                      Example: "Hyper-V" or "NetApp".
         * @param entityType The entity type of the entities
         * @return A {@link CalculationScope} used for retrieving entities of a given type discovered by a given
         *         type of probe.
         */
        CalculationScope probeEntityTypeScope(@Nonnull final String probeTypeName,
                                              @Nonnull final EntityType entityType);

        /**
         * Return a {@link CalculationScope} that restricts the calculation to operate on only entities
         * of a given {@link EntityType} discovered by a specific type of probe category.
         *
         * @param probeCategory The category of probe whose entities should be fed to the calculation.
         *                      Example: HYPERVISOR or STORAGE
         * @param entityType The entity type of the entities
         * @return A {@link CalculationScope} used for retrieving entities of a given type discovered by a given
         *         category of probe.
         */
        CalculationScope probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                      @Nonnull final EntityType entityType);
    }
}
