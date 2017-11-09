package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@link StitchingOperationResult} is returned for each call to
 * {@link StitchingOperation#stitch(Collection, Builder)}. It is used to represent
 * all the changes to the topology (either relationship updates or entity removals) made by a particular
 * {@link StitchingOperation} at a particular stitching point.
 *
 * Note that changes added to the Result are NOT immediately applied at the time the changes are created.
 * Instead, changes are applied AFTER the operation's {@link StitchingOperation#stitch(Collection, Builder)}
 * call completes and returns. This is done in order to maintain the expected supply chain of a target
 * until a stitching operation completes. For example, imagine a hypervisor probe discovers the following
 * entities
 *
 * ST1    ST2
 *    \  /
 *     DA
 *
 * And during storage stitching, the DA is removed. If the DA was immediately removed, by the time ST2 is
 * processed it would no longer be connected to the DA which would require operation developers to consider
 * all possible intermediate states for all possible entities that they process. By applying operations
 * AFTER the stitching code has finished running and making some topology edits such as removals idempotent
 * (ie removing an entity from the topology twice has the same effect as removing it once), it becomes
 * easier to reason about the state of the topological graph as stitching operations make edits. Changes
 * in {@link StitchingOperationResult}s are applied in the order that they are added to the result.
 *
 * Changes should be added to a result via the appropriate {@link StitchingOperationResult.Builder}
 * method.
 *
 * Note that changes to individual entities that do not affect relationships do not NEED to be
 * recorded in the results. So if I need to multiply the capacity of a commodity by 10, because
 * this change does not impact relationships, the operation can make this change directly
 * rather than recording it on the results it returned. However, if the change affects relationships,
 * it MUST be recorded in the {@link StitchingOperationResult} so that those changes can be properly
 * reflected in the graph. It is still recommended that edits to entities that do not affect relationships
 * still be made via {@link Builder#queueUpdateEntityAlone(StitchingEntity, Consumer)} because this will ensure
 * that the change can be recorded for debugging purposes and because it will cause that change to be applied
 * lazily in the order the change was added to the result instead of immediately.
 *
 * {@link StitchingChange}s in the result are processed in the order that they are received.
 *
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
     * A builder for a {@link StitchingOperationResult}.
     *
     * This builder will be subclassed to provide concrete implementations of the various methods
     * for queueing the appropriate changes in the TopologyProcessor component.
     */
    public static abstract class Builder {
        protected final List<StitchingChange> changes = new ArrayList<>();

        public abstract StitchingOperationResult build();

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
         * <p>
         * Requesting the removal of an entity that has already been removed results in a no-op.
         *
         * @param entity The entity to be removed.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract Builder queueEntityRemoval(@Nonnull final StitchingEntity entity);

        /**
         * Request the update of the relationships of an entity in the topology based on updates to
         * the commodities bought on that entity.
         * <p>
         * Note that this method does NOT perform any checks to verify that the updates made
         * are valid (ie it does not check that if adding a new commodity bought that the provider
         * is actually selling that commodity).
         * <p>
         * Updating an entity to buy a commodity from a provider it did not formerly buy from causes
         * that entity to add the new provider to its collection of providers and causes the entity to be
         * added to that provider's collection of consumers.
         * <p>
         * Removing a provider has the opposite effect - that provider will be removed from the entity's
         * collection of providers and the provider's collection of consumers will be updated to drop
         * the relationship to the entity that is no longer buying from it.
         *
         * @param entityToUpdate The entity whose relationships should be updated.
         * @param updateMethod A function that can be called to perform the update to the entity's commodities
         *                     bought. Changes to providers as identified through commodities bought on the
         *                     entity will propagate changes to providers and consumers in the topological graph.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract Builder queueChangeRelationships(@Nonnull final StitchingEntity entityToUpdate,
                                                         @Nonnull final Consumer<StitchingEntity> updateMethod);

        /**
         * Queue a change to the entity that DOES NOT affect the entity's relationships in the topological
         * graph. Examples of such a change including modifying the capacity of a commodity sold, setting
         * the active flag to false, etc.
         * <p>
         * Note that this change will be applied AFTER the call to
         * {@link StitchingOperation#stitch(Collection, Builder)} returns. The change will be applied in the same
         * order it was added to the {@link StitchingOperationResult} relative to the other changes that were
         * added to the result.
         *
         * @param entityToUpdate The entity to update.
         * @param updateMethod A function that can be called to update the entity.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract Builder queueUpdateEntityAlone(@Nonnull final StitchingEntity entityToUpdate,
                                                       @Nonnull final Consumer<StitchingEntity> updateMethod);

        /**
         * A method to complete the construction of the {@link StitchingOperationResult}.
         * Called internally by a subclass of the {@link StitchingOperationResult.Builder}
         * to construct the result object.
         *
         * @return A {@link StitchingOperationResult} containing the changes that were queued on this
         * {@link StitchingOperationResult.Builder}.
         */
        protected StitchingOperationResult buildInternal() {
            return new StitchingOperationResult(changes);
        }
    }

    /**
     * A {@link StitchingChange} represents an individual change to the topology made by a
     * {@link StitchingOperation}. These changes are collected together in order to represent
     * the change that a {@link StitchingOperation} wishes to make at a particular stitching point.
     * <p>
     * When mutating entities during stitching, those changes only need to be explicitly
     * recorded when those changes modify relationships to other entities. See the comments
     * on {@link StitchingOperationResult} for further details.
     */
    public interface StitchingChange {
        void applyChange();
    }
}