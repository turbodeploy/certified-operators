package com.vmturbo.stitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;

/**
 * A {@link PreStitchingOperation} runs prior to probe stitching {@link StitchingOperation}s.
 *
 * Pre-stitching Operations may be used to modify the topology or individual entities.
 *
 * {@link PreStitchingOperation} may manipulate the topology in ways that change entities and their relationships by,
 * for example, merging multiple discovered instances of a single entity down into just one instance.
 *
 * {@link PreStitchingOperation}s contrast with {@link StitchingOperation}s in that they do not perform a matching
 * phase. Instead, they specify a pre-defined scope to the operation and then operate on the entities within
 * that scope. See {@link StitchingScope} for additional details. For the list of available pre-defined scopes
 * see {@link StitchingScopeFactory}.
 */
public interface PreStitchingOperation {
    /**
     * Get the scope for this {@link PreStitchingOperation}. The {@link StitchingScope} returned determines
     * which entities are provided as input to the {@link #performOperation(Stream, StitchingChangesBuilder)}
     * method for this {@link PreStitchingOperation}. See {@link StitchingScopeFactory} for further details.
     *
     * @param stitchingScopeFactory The factory to use to construct the {@link StitchingScope} for this
     *                                {@link PreStitchingOperation}.
     * @return The {@link StitchingScope} to use fo this {@link PreStitchingOperation}.
     */
    @Nonnull
    StitchingScope<StitchingEntity> getScope(@Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory);

    /**
     * Perform this {@link PreStitchingOperation}. Any changes the operation wants to make to
     * the entities or their relationships should be made by adding an appropriate
     * {@link TopologicalChange} to using the {@link StitchingChangesBuilder}.
     *
     * The precise semantics of a {@link PreStitchingOperation} vary, but in general these operations
     * may want to update values in entity commodities or merge multiple views of a single entity discovered
     * by multiple probes down to a single view.
     *
     * Any updates to entities or their relationships must be noted in the returned {@link TopologicalChangelog}
     * so that the graph and certain other acceleration structures that track entities and relationships
     * can be updated for further stitching.
     *
     * @param entities The entities in the calculation scope. For details on scope see
     *                 {@link #getScope(StitchingScopeFactory)}
     * @param resultBuilder A builder for the result containing the changes this operation wants to make
     *                      to the entities and their relationships. The calculation should use this builder
     *                      to create the result it returns.
     * @return A {@link TopologicalChangelog} that describes the result of stitching. The result should be built using
     *         the {@link StitchingChangesBuilder} provided as input.
     */
    @Nonnull
    TopologicalChangelog performOperation(@Nonnull final Stream<StitchingEntity> entities,
                                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder);
}
