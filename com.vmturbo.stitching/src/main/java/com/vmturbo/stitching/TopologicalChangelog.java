package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;

/**
 * A {@link TopologicalChangelog} is returned for each call to
 * {@link StitchingOperation#stitch(Collection, StitchingChangesBuilder)} or
 * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)}. It is used to represent
 * all the changes to the topology (either relationship updates or entity removals) made by a particular
 * {@link StitchingOperation} or {@link PreStitchingOperation}.
 *
 * Note that changes added to the Result are NOT immediately applied at the time the changes are created.
 * Instead, changes are applied AFTER the operation's
 * {@link StitchingOperation#stitch(Collection, StitchingChangesBuilder)} call completes and returns.
 * This is done in order to maintain the expected supply chain of a target until a stitching operation completes.
 * For example, imagine a hypervisor probe discovers the following entities
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
 * in {@link TopologicalChangelog}s are applied in the order that they are added to the result.
 *
 * Changes should be added to a result via the appropriate {@link StitchingChangesBuilder}
 * method.
 *
 * Note that changes to individual entities that do not affect relationships do not NEED to be
 * recorded in the results. So if you need to multiply the capacity of a commodity by 10, because
 * this change does not impact relationships, the operation can make this change directly
 * rather than recording it on the results it returned. However, if the change affects relationships,
 * it MUST be recorded in the {@link TopologicalChangelog} so that those changes can be properly
 * reflected in the graph. It is still recommended that edits to entities that do not affect relationships
 * be made via {@link StitchingChangesBuilder#queueUpdateEntityAlone} because this will ensure
 * that the change can be recorded for debugging purposes and because it will cause that change to be applied
 * lazily in the order the change was added to the result instead of immediately.
 *
 * {@link TopologicalChange}s in the result are processed in the order that they are added to
 * the {@link TopologicalChangelog}.
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
 *
 * @param <ENTITY> The type of entity being stitched (ie {@link StitchingEntity} in pre- and main-stitching
 *                 and {@link TopologyEntity} in post-stitching.
 */
@Immutable
public class TopologicalChangelog<ENTITY extends JournalableEntity<ENTITY>> {

    /**
     * The ordered list of changes in the result.
     */
    private final List<TopologicalChange<ENTITY>> changes;

    private TopologicalChangelog(@Nonnull final List<TopologicalChange<ENTITY>> changes) {
        this.changes = Collections.unmodifiableList(changes);
    }

    /**
     * Get the list of all changes in the result.
     *
     * @return the list of all changes in the result.
     */
    public List<TopologicalChange<ENTITY>> getChanges() {
        return changes;
    }

    /**
     * A builder for {@link TopologicalChangelog}. A {@link EntityChangesBuilder}, only permits mutations to entities
     * that do not modify consumer/provider relationships between entities. These restricted changes are used
     * during post-stitching. See
     * {@link PostStitchingOperation#performOperation(Stream, EntitySettingsCollection, EntityChangesBuilder)}.
     *
     * This builder will be subclassed to provide concrete implementations of the various methods
     * for queueing the appropriate changes in the TopologyProcessor component.
     *
     * @param <ENTITY> The type of entity that to be modified by the changes. Examples are {@link StitchingEntity}
     *                 or {@link TopologyEntity}.
     */
    public static abstract class EntityChangesBuilder<ENTITY extends JournalableEntity<ENTITY>> {
        protected final List<TopologicalChange<ENTITY>> changes = new ArrayList<>();

        public abstract TopologicalChangelog<ENTITY> build();

        /**
         * Get the list of all changes in the result.
         *
         * @return the list of all changes in the result.
         */
        public List<TopologicalChange<ENTITY>> getChanges() {
            return changes;
        }

        /**
         * Queue a change to the entity that DOES NOT affect the entity's relationships in the topological
         * graph. Examples of such a change including modifying the capacity of a commodity sold, setting
         * the active flag to false, etc.
         * <p>
         * Note that this change will be applied AFTER the call to
         * {@link StitchingOperation#stitch(Collection, StitchingChangesBuilder)} returns. The change will be applied
         * in the same order it was added to the {@link TopologicalChangelog} relative to the other changes that were
         * added to the result.
         *
         * @param entityToUpdate The entity to update.
         * @param updateMethod A function that can be called to update the entity.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract EntityChangesBuilder<ENTITY> queueUpdateEntityAlone(
            @Nonnull final ENTITY entityToUpdate, @Nonnull final Consumer<ENTITY> updateMethod);

        /**
         * A method to complete the construction of the {@link TopologicalChangelog}.
         * Called internally by a subclass of the {@link StitchingChangesBuilder}
         * to construct the result object.
         *
         * @return A {@link TopologicalChangelog} containing the changes that were queued on this
         * {@link StitchingChangesBuilder}.
         */
        protected TopologicalChangelog<ENTITY> buildInternal() {
            return new TopologicalChangelog<>(changes);
        }
    }

    /**
     * A builder for a {@link TopologicalChangelog}. A {@link StitchingChangesBuilder}, unlock the parent
     * class {@link EntityChangesBuilder} can manipulate entities in a way that modifies buying and selling
     * relationships that can cause changes in consumer/provider relationships. These changes are used in the
     * pre-stitching and main stitching phases. See
     * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)} and
     * {@link StitchingOperation#stitch(Collection, StitchingChangesBuilder)}.
     *
     * This builder will be subclassed to provide concrete implementations of the various methods
     * for queueing the appropriate changes in the TopologyProcessor component.
     *
     * @param <ENTITY> The type of entity that to be modified by the changes. Examples are {@link StitchingEntity}
     *                 or {@link TopologyEntity}.
     */
    public static abstract class StitchingChangesBuilder<ENTITY extends JournalableEntity<ENTITY>>
        extends EntityChangesBuilder<ENTITY> {
        /**
         * Request the removal of an entity from the topology.
         * <p>
         * Requesting the removal of an entity that has already been removed results in a no-op.
         *
         * @param entity The entity to be removed.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract StitchingChangesBuilder<ENTITY> queueEntityRemoval(@Nonnull final ENTITY entity);

        /**
         * @see com.vmturbo.stitching.utilities.MergeEntities
         * <p>
         * Note that no changes are made to the commodities bought by the entities being merged.
         * If you wish to make changes to these commodities, consider using the
         * {@link #queueChangeRelationships} to have the
         * merged entity buy new or different commodities before queueing the merge change.
         * <p>
         * TODO: (DavidBlinn 12/1/2017)
         * TODO: Support for replacing an entity in discovered groups.
         *
         * As a side effect of merging entities, the lastUpdatedTime and mergeFromTargetIds of the
         * "onto" entity will be updated to reflect the result of the merge.
         *
         * @param details An object describing the entities to be merged.
         * @return A reference to {@link this} to support method chaining.
         */
        public abstract StitchingChangesBuilder<ENTITY> queueEntityMerger(@Nonnull final MergeEntitiesDetails details);

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
        public abstract StitchingChangesBuilder<ENTITY> queueChangeRelationships(
            @Nonnull final ENTITY entityToUpdate, @Nonnull final Consumer<ENTITY> updateMethod);
    }

    /**
     * A {@link TopologicalChange} represents an individual change to the topology made by a
     * {@link StitchingOperation}. These changes are collected together in order to represent
     * the change that a {@link StitchingOperation} wishes to make at a particular stitching point.
     * <p>
     * When mutating entities during stitching, those changes only need to be explicitly
     * recorded when those changes modify relationships to other entities. See the comments
     * on {@link TopologicalChangelog} for further details.
     *
     * @param <ENTITY> The type of the entity being recorded in the journal.
     */
    public interface TopologicalChange<ENTITY extends JournalableEntity<ENTITY>> {
        /**
         * Apply a change to the topology. Record the effects of the change to the stitching journal.
         *
         * @param stitchingJournal The stitching journal used to track changes.
         */
        void applyChange(@Nonnull final IStitchingJournal<ENTITY> stitchingJournal);
    }
}