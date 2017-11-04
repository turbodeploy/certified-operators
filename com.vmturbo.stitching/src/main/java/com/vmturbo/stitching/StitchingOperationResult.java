package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A {@link StitchingOperationResult} is returned for each call to
 * {@link StitchingOperation#stitch(EntityDTO.Builder, Stream, StitchingGraph)}. It is used to represent
 * all the changes to the topology (either relationship updates or entity removals) made by a particular
 * {@link StitchingOperation} at a particular stitching point.
 *
 * Note that changes to individual entities that do not affect relationships do NOT need to be
 * recorded in the results. So if I need to multiply the capacity of a commodity by 10, because
 * this change does not impact relationships, the operation can make this change directly
 * rather than recording it on the results it returned. However, if the change affects relationships,
 * because this affects the relationships in the {@link StitchingGraph}, it should be recorded
 * in the {@link StitchingOperationResult} so that those changes can be properly reflected
 * in the graph.
 *
 * Collects together an ordered list of {@link StitchingChange}s that indicate the next change
 * made by the operation that should be processed.
 *
 * {@link StitchingChange}s in the result are processed in the order that they are received.
 *
 * A change only needs to be recorded in the result if it affects relationships in the topology graph.
 * Changes to entity relationships permitted during stitching:
 * 1. Removing an entity - Removing an entity from the graph propagates a change to all buyers of commodities
 *                         from the entity being removed.
 * 2. Commodities bought - Commodities bought by an entity may be added or removed. These changes will
 *                         automatically be propagated to the sellers of the commodities.
 *
 * Changes to entity relationships NOT permitted during stitching:
 * 1. The creation of new entities.
 * 2. Commodities sold - No destructive mutations are permitted to commodities sold (that is, changes
 *                       that would change or remove relationships to buyers of the commodities being changed).
 *                       If a use case for this arises, we may consider supporting it in the future.
 *
 * An invalid change (ie updating an entity that has already been removed) will cause
 * an exception to be thrown and all stitching to be abandoned.
 */
@Immutable
public class StitchingOperationResult {

    /**
     * The ordered list of changes in the result.
     */
    private final List<StitchingChange> changes;

    private StitchingOperationResult(@Nonnull final List<StitchingChange> changes) {
        this.changes = Collections.unmodifiableList(changes);
    }

    /**
     * Get the list of all changes in the result.
     *
     * @return the list of all changes in the result.
     */
    public List<StitchingChange> getChanges() {
        return changes;
    }

    /**
     * Create a new builder for a {@link StitchingOperationResult}.
     *
     * @return A builder for a {@link StitchingOperationResult}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for a {@link StitchingOperationResult}
     */
    public static class Builder {
        private final List<StitchingChange> changes = new ArrayList<>();

        public StitchingOperationResult build() {
            return new StitchingOperationResult(changes);
        }

        /**
         * Get the list of all changes in the result.
         *
         * @return the list of all changes in the result.
         */
        public List<StitchingChange> getChanges() {
            return changes;
        }

        /**
         * Request the removal of an entity from the topology.
         *
         * Requesting the removal of an entity that has already been removed results in a no-op.
         *
         * @param entity The entity to be removed.
         */
        public void removeEntity(@Nonnull final EntityDTO.Builder entity) {
            changes.add(new RemoveEntityChange(entity));
        }

        /**
         * Request the update of the relationships of an entity in the topology based on a change
         * to its commodities bought.
         *
         * Requesting the update of an entity that has already been removed results in an exception
         * that causes stitching to be abandoned.
         *
         * Note that this method does NOT perform any checks to verify that the updates made
         * are valid (ie it does not check that if adding a new commodity bought that the provider
         * is actually selling that commodity).
         *
         * Also note that a change to commodities sold that would propagate to buyers is NOT
         * handled by this change. This change only accounts for updates to commodities bought, not
         * sold. Destructive changes to commodities sold are not permitted during stitching.
         * TODO: If a use case arises for support for destructive changes to commodities sold, add support.
         *
         * @param entity The entity whose relationships should be updated.
         * @param updateMethod A method that receives the entity being updated and attaches new
         *                     commodities bought to update the relationships of the entity and its
         *                     potential new producers.
         */
        public void updateCommoditiesBought(@Nonnull final EntityDTO.Builder entity,
                                            @Nonnull final Consumer<EntityDTO.Builder> updateMethod) {
            changes.add(new CommoditiesBoughtChange(entity, updateMethod));
        }
    }

    /**
     * A {@link StitchingChange} represents an individual change to the topology made by a
     * {@link StitchingOperation}. These changes are collected together in order to represent
     * the change that a {@link StitchingOperation} wishes to make at a particular stitching point.
     *
     * When mutating entities during stitching, those changes only need to be explicitly
     * recorded when those changes modify relationships to other entities. See the comments
     * on {@link StitchingOperationResult} for further details.
     */
    @Immutable
    public abstract static class StitchingChange {
        /**
         * The builder for the entity whose relationships are being changed by the specific {@link StitchingChange}.
         * The builder will be used to construct the entity resulting in the final topology.
         */
        public final EntityDTO.Builder entityBuilder;

        /**
         * Construct a new stitching change.
         * Package-private to prevent unwanted overrides.
         *
         * @param entity The entity affected by the change.
         */
        StitchingChange(@Nonnull final EntityDTO.Builder entity) {
            this.entityBuilder = Objects.requireNonNull(entity);
        }
    }

    /**
     * Represents the removal of an individual {@link EntityDTO} from the eventual topology.
     */
    @Immutable
    public static class RemoveEntityChange extends StitchingChange {
        public RemoveEntityChange(@Nonnull final EntityDTO.Builder entityBuilder) {
            super(entityBuilder);
        }
    }

    /**
     * Represents the update of relationships of an individual {@link EntityDTO} in the eventual topology.
     *
     * The update method can add new commodities to an entity in order to add a directed relationship
     * that makes the entity with the commodities a consumer of the entity it is buying the commodity from
     * and it makes tne entity selling the commodity a producer of the entity purchasing the commodity.
     *
     * The update method can also remove commodities from an entity in order to remove a directed
     * relationship from the updating entity to the entity it was buying the commodity from if
     * it was the last commodity bought from that provider.
     *
     * If, for example, entity A is buying CPU and MEM from entity B and the {@link #updateMethod} removes
     * the CPU commodity, the graph retains a connection between A and B because of the MEM commodity.
     * However, if MEM is also removed, A will be disconnected from B in the graph.
     */
    @Immutable
    public static class CommoditiesBoughtChange extends StitchingChange {
        public final Consumer<EntityDTO.Builder> updateMethod;

        public CommoditiesBoughtChange(@Nonnull final EntityDTO.Builder entityBuilder,
                                       @Nonnull final Consumer<EntityDTO.Builder> updateMethod) {
            super(entityBuilder);

            this.updateMethod = Objects.requireNonNull(updateMethod);
        }
    }
}
