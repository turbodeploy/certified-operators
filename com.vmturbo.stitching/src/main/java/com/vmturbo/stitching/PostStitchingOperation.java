package com.vmturbo.stitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JournalableOperation;

/**
 * A {@link PostStitchingOperation} runs after probe stitching {@link StitchingOperation}s. In the TopologyPipeline,
 * it also runs after other pipeline stages such as group resolution, policy application, and settings application.
 * Unlike {@link PreStitchingOperation}s and {@link StitchingOperation}s, {@link PostStitchingOperation}s do not
 * operate on {@link StitchingEntity}s which contain the {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO}s.
 * They instead operate on {@link TopologyEntity}s which contain
 * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}s.
 *
 * {@link PostStitchingOperation}s may NOT modify the topology in ways that modify topological relationships.
 * They are capable of changing attributes of individual entities such as commodity capacity or usage values or
 * individual entity properties, but they MUST NOT change which entities are buying or selling which commodities
 * to each other. TODO: (DavidBlinn 12/17/2017) Revisit this decision if a sufficiently strong use case presents itself.
 * Topological relationship changes can be made via {@link PreStitchingOperation}s or {@link StitchingOperation}s.
 * {@link PostStitchingOperation}s may, however, look up which settings apply to which entities, a capability not
 * available to {@link PreStitchingOperation}s or {@link StitchingOperation}s.
 *
 * TODO: Implementation note - Relationship changes are not allowed because it permits more efficient data structures
 * (TopologyGraph) and simpler interfaces and no {@link PostStitchingOperation} yet requires the capability to do so.
 *
 * {@link PostStitchingOperation}s contrast with {@link StitchingOperation}s in that they do not perform a matching
 * phase. Instead, like {@link PreStitchingOperation}s, they specify a pre-defined scope to the calculation and
 * then operate on the entities within that scope.
 */
public interface PostStitchingOperation extends JournalableOperation {
    /**
     * Get the scope for this {@link PostStitchingOperation}. The {@link StitchingScope} returned determines
     * which entities are provided as input to the {@link #performOperation}
     * method for this {@link PostStitchingOperation}. See {@link StitchingScopeFactory} for further details.
     *
     * @param stitchingScopeFactory The factory to use to construct the {@link StitchingScope} for this
     *                                {@link PreStitchingOperation}.
     * @return The {@link StitchingScope} to use fo this {@link PreStitchingOperation}.
     */
    @Nonnull
    StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory);

    /**
     * Perform this {@link PostStitchingOperation}.
     *
     * The precise semantics of a {@link PostStitchingOperation} vary, but in general these operations
     * may want to update values in entity commodities or properties in a way that does not affect
     * relationships between entities. This means that commodities bought and sold by entities should
     * not be modified in a way that they change which entities are buying or selling commodities to each other,
     * however properties such as a commodity's used or capacity value may be modified without issue.
     *
     * Any updates to entities or must be noted in the returned {@link TopologicalChangelog}
     * so that the updates can be tracked.
     *
     * @param entities The entities in the calculation scope. For details on scope see
     *                 {@link #getScope(StitchingScopeFactory)}
     * @param settingsCollection A collection of settings for entities, permitting the lookup of a setting
     *                           with a given name for a particular entity.
     * @param resultBuilder A builder for the result containing the changes this operation wants to make
     *                      to the entities and their relationships. The calculation should use this builder
     *                      to create the result it returns.
     * @return A {@link TopologicalChangelog} that describes the result of stitching. The result should be built using
     *         the {@link StitchingChangesBuilder} provided as input.
     */
    @Nonnull
    TopologicalChangelog<TopologyEntity> performOperation(@Nonnull final Stream<TopologyEntity> entities,
                                                      @Nonnull final EntitySettingsCollection settingsCollection,
                                                      @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder);

    /**
     * {@inheritDoc}
     *
     * @return By default, {@link PostStitchingOperation}s are entered in compact format.
     */
    @Nonnull
    @Override
    default FormatRecommendation getFormatRecommendation() {
        return FormatRecommendation.COMPACT;
    }
}
