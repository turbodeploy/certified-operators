package com.vmturbo.topology.processor.stitching.journal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.IStitchingJournal.IJournalChangeset;
import com.vmturbo.stitching.journal.IStitchingJournal.JournalChangeset;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingMetrics;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.journal.SemanticDiffer;

/**
 * Merge down changsets if they share any common members.
 *
 * The order of changeset additions is important. For example, suppose we have two changests that modify
 * entity A. Changset1 changes A -> A' and changeset2 changes A' -> A''. The merger should reflect the
 * change from A -> A''.
 *
 * Mergers are accomplished by a simple implementation of the union-find algorithm
 * (https://en.wikipedia.org/wiki/Disjoint-set_data_structure) where changesets are
 * merged if they have any common members.
 */
public class ChangesetMerger<T extends JournalableEntity<T>> {
    private final Map<T, ChangesetUnion<T>> changesToUnions = new IdentityHashMap<>();
    private final ArrayList<ChangesetUnion<T>> changesetUnions = new ArrayList<>();

    public ChangesetMerger() {
    }

    /**
     * Add a changeset to be merged with any other added changesets with which it has any members in common.
     *
     * For example, suppose the following changesets are added (where the numbers in parenthesis indicate
     * the IDs of the entities changed by those changesets):
     *
     * A: (1,2,3)
     * B: (4)
     * C: (3,5)
     * D: (5,6)
     *
     * the resulting merged changesets would be:
     * A/C/D: (1,2,3,5,6)
     * B: (4)
     *
     * @param changeset The changeset to add.
     */
    public void add(@Nonnull final JournalChangeset<T> changeset) {
        // Create a new ChangesetUnion for the changeset to be added.
        ChangesetUnion<T> union = new ChangesetUnion<>(changeset);
        changesetUnions.add(union);

        // Union all ChangesetUnions associated with each of the entities in the Changeset being added
        // as well as the new ChangesetUnion.
        for (T changedEntity : (Iterable<T>)changeset.changedEntities()::iterator) {
            final ChangesetUnion<T> unionForChangedEntity = changesToUnions.get(changedEntity);
            if (unionForChangedEntity != null) {
                unionForChangedEntity.unionByRank(union);
            } else {
                changesToUnions.put(changedEntity, union);
            }
        }
    }

    /**
     * Record all preambles and semantic differences for all entities in all changesets added to
     * this {@link ChangesetMerger}.
     *
     * Merged changesets are recorded with the merged changesets being ordered by the lowest
     * changeset index of any changest within the merged group.
     *
     * @param journal The journal to which the merged changesets should be recorded.
     */
    public void recordAll(@Nonnull final IStitchingJournal<T> journal) {
        changesetUnions.stream()
            .collect(Collectors.groupingBy(ChangesetUnion::find))
            .values()
            .stream()
            .map(unionMembers -> new MergedChangeset<>(unionMembers.stream()
                .map(ChangesetUnion::getChangeset)
                .collect(Collectors.toList()), journal.getJournalOptions().getVerbosity()))
            .sorted((a, b) -> Integer.compare(a.getChangesetIndex(), b.getChangesetIndex()))
            .forEach(journal::recordSemanticDifferences);
    }

    /**
     * Clear all merged changests in this {@link ChangesetMerger}.
     */
    public void clear() {
        changesetUnions.clear();
        changesToUnions.clear();
    }

    /**
     * A changeset that contains the results of merging one or more other changesets into it.
     *
     * @param <T> The type of entity being entered into the journal
     */
    @Immutable
    @VisibleForTesting
    static class MergedChangeset<T extends JournalableEntity<T>> implements IJournalChangeset<T> {

        /**
         * A map of Journalable -> Snapshot of that journalable.
         *
         * Entries in this map are not overwritten, requests to add a change to the changeset
         * when that Journalable was already added to the changeset results in keeping an original
         * snapshot, not an overwritten snapshot.
         */
        private final Map<T, T> mutated;

        /**
         * The entities removed as part of this changeset.
         *
         * Removed entities for a changeset are entered into the journal AFTER changes in the changeset
         * are entered into the journal.
         */
        private final Set<T> removed;

        /**
         * The entities added as part of this changeset.
         *
         * Added entities for a changeset are entered into the journal AFTER changes in the changeset
         * are entered into the journal.
         */
        private final Set<T> added;

        /**
         * The strings used to describe the action of the merged changesets.
         * A collection of the preambles from the changesets that were merged into this one.
         * This list is ordered in the order that the changesets were merged into this changeset.
         */
        private final List<String> changesetPreambles = new ArrayList<>();

        /**
         * The lowest index of all the changsets in the merged changeset.
         */
        private final int lowestChangesetIndex;

        /**
         * Options controlling how verbose the semantic differences should be when
         * generating semantic differences.
         */
        private final Verbosity verbosity;

        /**
         * The combined size of all changesets merged into this one.
         */
        private final int changesetSize;

        /**
         * Metrics to count the changes made as part of the changeset.
         */
        private final StitchingMetrics stitchingMetrics;

        /**
         * Create a new MergedChangeset. Mutations in the collection of other changesets
         * are merged in using the order provided by the list of other changesets.
         *
         * When merging the changesets in the input list we first order the changesets so that
         * a changeset created earlier in the stitching process is merged in before a
         * changeset created later in the stitching process. This is done to allow
         * the merged changeset to use the snapshot from the EARLIER changeset as the initial state
         * and the LATER changeset as the final state so that the merged diff shows the result
         * after all changesets have been applied to an entity they affect in the correct order.
         *
         * Thus, if ChangesetOne (earlier) changes entity A from A -> A' and ChangesetTwo changes
         * entity A from A' -> A'', we want the merged changeset to be able to render the semantic
         * difference from the composition of ChangesetTwo(ChangesetOne(A)): A -> A''.
         *
         * @param otherChangesets The changesets to be merged into this {@link MergedChangeset}.
         * @param verbosity How verbose the merged changeset should be when generating semantic differences.
         */
        public MergedChangeset(@Nonnull final Collection<JournalChangeset<T>> otherChangesets,
                               @Nonnull final Verbosity verbosity) {
            // Use an IdentityHashMap to avoid expensive equality checks and because reference equality
            // is correct for the purpose of recording entries to the journal changeset.
            final Map<T, T> mutated = new IdentityHashMap<>();
            final Set<T> removed = Collections.newSetFromMap(new IdentityHashMap<>());
            final Set<T> added = Collections.newSetFromMap(new IdentityHashMap<>());

            otherChangesets.stream()
                .sorted((a, b) -> Integer.compare(a.getChangesetIndex(), b.getChangesetIndex()))
                .forEach(changeset -> mergeChangeset(changeset, mutated, removed, added));
            lowestChangesetIndex = otherChangesets.stream()
                .mapToInt(JournalChangeset::getChangesetIndex)
                .min()
                .orElse(Integer.MAX_VALUE);

            changesetSize = otherChangesets.stream()
                .mapToInt(IJournalChangeset::getChangesetSize)
                .sum();

            // Make the member references unmodifiable so that the class can be immutable.
            this.mutated = Collections.unmodifiableMap(mutated);
            this.removed = Collections.unmodifiableSet(removed);
            this.added = Collections.unmodifiableSet(added);

            this.verbosity = Objects.requireNonNull(verbosity);
            // Since changesets all generally refer to the same metrics instance, it is fine
            // to take the metrics from any instance, so we take from the first.
            this.stitchingMetrics = otherChangesets.stream()
                .findFirst()
                .map(JournalChangeset::getMetrics)
                .orElse(new StitchingMetrics());
        }

        /**
         * Merge in a changeset to the merged changeset.
         *
         * @param otherChangeset The other changeset to merge in.
         * @param mutated The map of mutated entities. The key in the map is the mutated entity, the value
         *                is the original (earliest) snapshot of that entity. If an entity already exists
         *                in the map, its snpahsot should NOT be replaced by a later one.
         * @param removed The set of removed entities. Use a set because it does not make sense to consider
         *                an entity remove an entity multiple times from the same topology because attempting
         *                to remove an entity already removed is treated as a no-op.
         */
        private void mergeChangeset(@Nonnull final IJournalChangeset<T> otherChangeset,
                                    @Nonnull final Map<T, T> mutated,
                                    @Nonnull final Set<T> removed,
                                    @Nonnull final Set<T> added) {
            changesetPreambles.addAll(otherChangeset.getChangesetPreambles());
            otherChangeset.getMutatedEntities().forEach((entity, originalSnapshot) -> {
                // Because we want the snapshot to be the earliest snapshot and the changesets are added
                // in earliest-first order, only insert the entity's snapshot if it is not already there.
                if (!mutated.containsKey(entity)) {
                    mutated.put(entity, originalSnapshot);
                }
            });

            removed.addAll(otherChangeset.getRemovedEntities());
            added.addAll(otherChangeset.getAddedEntities());
        }

        @Override
        public List<String> semanticDifferences(@Nonnull final SemanticDiffer<T> semanticDiffer,
                                                @Nonnull final FormatRecommendation format) {
            final Stream<String> changeDifferences = getMutatedEntities().entrySet().stream()
                // For merged changesests, if an entity was both mutated and removed, don't print it unless
                // we are instructed to be extra verbose.
                .filter(entry -> verbosity == Verbosity.COMPLETE_VERBOSITY || !removed.contains(entry.getKey()))
                .map(entry -> semanticDiffer.semanticDiff(entry.getValue(), entry.getKey(), format))
                .filter(difference -> !difference.isEmpty());
            final Stream<String> removalDescriptions = getRemovedEntities().stream()
                .map(JournalableEntity::removalDescription)
                .filter(description -> !description.isEmpty());
            final Stream<String> additionDescriptions = getAddedEntities().stream()
                .map(JournalableEntity::additionDescription)
                .filter(description -> !description.isEmpty());

            return Stream.of(changeDifferences, removalDescriptions, additionDescriptions)
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList());
        }

        @Override
        public List<String> getChangesetPreambles() {
            return changesetPreambles;
        }

        @Override
        public Stream<T> changedEntities() {
            return Stream.concat(mutated.keySet().stream(), removed.stream());
        }

        @Override
        public int changedEntityCount() {
            return mutated.size() + removed.size();
        }

        @Override
        public Map<T, T> getMutatedEntities() {
            return mutated;
        }

        @Override
        public Collection<T> getRemovedEntities() {
            return removed;
        }

        @Override
        public Collection<T> getAddedEntities() {
            return added;
        }

        @Override
        public int getChangesetIndex() {
            return lowestChangesetIndex;
        }

        @Override
        public int getChangesetSize() {
            return changesetSize;
        }

        @Override
        public StitchingMetrics getMetrics() {
            return stitchingMetrics;
        }
    }

    @VisibleForTesting
    static class ChangesetUnion<T extends JournalableEntity<T>> {
        /**
         * Parent pointer.
         */
        private ChangesetUnion<T> parent;

        /**
         * The inner changeset.
         */
        private final JournalChangeset<T> changeset;

        /**
         * A rank for implementing path compression.
         */
        private int rank;

        ChangesetUnion(@Nonnull final JournalChangeset<T> changeset) {
            // Initially set parent pointer to self to indicate this is the representative member
            // of its own set.
            this.parent = this;
            this.changeset = changeset;
            this.rank = 0;
        }

        /**
         * Find the root element that is the representative member of the set to which this ChangesetUnion belongs.
         *
         * @return the representative member of the set to which this ChangesetUnion belongs.
         */
        ChangesetUnion<T> find() {
            if (!isRepresentativeMember()) {
                // Perform path compression during the find operation.
                final ChangesetUnion<T> representativeMember = parent.find();
                parent = representativeMember;
                return representativeMember;
            }

            return this;
        }

        /**
         * Union the two sets together by rank (that is, attach the shorter tree to the taller tree) and
         * return the representative member of the united set. Note that we use reference equality
         * to test equality.
         *
         * If the sets are already the same, makes no changes but returns the representative member
         * of the combined set.
         *
         * The rank of the united set will be increased by one if the original set and the other set
         * were equal in rank, otherwise the united set gets a rank equal to the larger of the two
         * original ranks.
         *
         * @param otherSet The other set to union with this set.
         * @return The representative member of the combined set.
         */
        ChangesetUnion<T> unionByRank(@Nonnull final ChangesetUnion<T> otherSet) {
            ChangesetUnion<T> xRoot = find();
            ChangesetUnion<T> yRoot = otherSet.find();

            // The sets are already the same.
            if (xRoot == yRoot) {
                return xRoot; // Nothing to do.
            }

            // Merge the sets.
            if (xRoot.rank < yRoot.rank) {
                // Swap the roots so that we always merge the shorter tree onto the root of the taller tree
                // so that the result is no taller than the original unless they were of equal height.
                final ChangesetUnion<T> temp = xRoot;
                xRoot = yRoot;
                yRoot = temp;
            }

            // Merge yRoot into xRoot.
            yRoot.parent = xRoot;
            if (xRoot.rank == yRoot.rank) {
                // When merging two trees of the same size, increment rank of the representative member.
                xRoot.rank++;
            }

            return xRoot; // Return the new representative element.
        }

        /**
         * Get the changeset associated with this {@link ChangesetUnion}.
         *
         * @return The changeset associated with this {@link ChangesetUnion}.
         */
        public JournalChangeset<T> getChangeset() {
            return changeset;
        }

        /**
         * Check whether this {@link ChangesetUnion} is the representative member of the set to which it belongs.
         *
         * @return whether this {@link ChangesetUnion} is the representative member of the set to which it belongs.
         */
        boolean isRepresentativeMember() {
            return parent == this;
        }
    }
}
