package com.vmturbo.stitching.journal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.TopologyDumpOptions;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A journal to record changes to the topology made during various stitching phases.
 */
@NotThreadSafe
public interface IStitchingJournal<T extends JournalableEntity<T>> {
    enum StitchingPhase {
        /**
         * See {@link com.vmturbo.stitching.PreStitchingOperation}.
         */
        PRE_STITCHING("BEGIN PRE-STITCHING PHASE",
            "This phase is run before Main Stitching early in the topology pipeline. Pre-\n" +
                "stitching operations may make the following modifications to entities:\n" +
                "1. Remove entities from the topology.\n" +
                "2. Merge multiple instances of an entity into a single instance.\n" +
                "3. Change relationships between entities in the topology (ie buy a commodity\n" +
                "from an entity you were not previously buying anything from).\n" +
                "4. Change the values or properties of an entity or its commodities.\n" +
                "See the documentation in the code for PreStitchingOperation for further details."),

        /**
         * See {@link com.vmturbo.stitching.StitchingOperation}.
         */
        MAIN_STITCHING("BEGIN MAIN STITCHING PHASE",
            "This phase is run immediately after Pre-stitching early in the topology\n" +
                "pipeline. Main stitching operations may make the following modifications to\n" +
                "entities:\n" +
                "1. Remove entities from the topology.\n" +
                "2. Merge multiple instances of an entity into a single instance.\n" +
                "3. Change relationships between entities in the topology (ie buy a commodity\n" +
                "from an entity you were not previously buying anything from).\n" +
                "4. Change the values or properties of an entity or its commodities.\n" +
                "See the documentation in the code for StitchingOperation for further details."),

        /**
         * See {@link com.vmturbo.stitching.PostStitchingOperation}.
         */
        POST_STITCHING("BEGIN POST-STITCHING PHASE",
            "This phase is run after Main Stitching and other phases in the topology pipeline\n" +
                "such as group resolution, placement policy application, settings resolution,\n" +
                "etc. in the topology pipeline. Unlike Pre-stitching or Main Stitching\n" +
                "operations, Post-stitching operations are able to look up the value of a\n" +
                "resolved setting for a particular entity. Post-stitching operations may ONLY\n" +
                "make the following limited type of modifications to entities:\n" +
                "1. Change the values or properties of an entity or its commodities.\n" +
                "See the documentation in the code for PostStitchingOperation for further\n" +
                "details.");

        private final String preamble;
        private final String phaseDescription;

        private static final String PHASE_MARKER =
            "================================================================================";

        StitchingPhase(@Nonnull final String preamble,
                       @Nonnull final String phaseDescription) {
            this.preamble = Objects.requireNonNull(preamble);
            this.phaseDescription = Objects.requireNonNull(phaseDescription);
        }

        public String getPreamble() {
            return preamble;
        }

        public String getPhaseDescription() {
            return phaseDescription;
        }

        public String transitionMessage() {
            return new StringBuilder(256)
                .append(JournalRecorder.prettyPrintCenter(preamble, '=', 80)).append("\n")
                .append(phaseDescription).append("\n")
                .append(PHASE_MARKER).append("\n")
                .toString();
        }
    }

    /**
     * When a user requests journal entries for a stitching pass and they pass a verbosity that does
     * not require all details of all changed entities to be printed, operations themselves can provide
     * hints to the journal about how the best means to format the journal entries for their changes.
     */
    enum FormatRecommendation {
        /**
         * Some operations make the same change to a large number of entities which do not require much
         * context to understand. Example: an operation that sets Movable -> false for all VMs discovered
         * by a certain type of probe. These sorts of operations should recommend that the journal record
         * their changes in a compact format and reduce the context displayed for their changes to the
         * minimal allowed by the verbosity specified by the user.
         *
         * Extraneous whitespace should be stripped from COMPACT format changes unless verbosity
         * is set to COMPLETE_VERBOSITY.
         */
        COMPACT,

        /**
         * Some operations make changes to a smaller number of entities that are more complex and require
         * more context to understand. Example: an operation that merges and removes entities according
         * to a complex set of rules. These sorts of operations should recommend that the journal record
         * relevant context and pretty-print their journal entries.
         */
        PRETTY
    }

    /**
     * Add a {@link JournalRecorder} to the list of recorders to which journal entries are added.
     *
     * @param recorder The recorder to be added to the list of recorders.
     */
    void addRecorder(@Nonnull final JournalRecorder recorder);

    /**
     * Remove a {@link JournalRecorder} from the list of recorders to which journal entries are added.
     * If the recorder is not currently in the list of recorders, returns false.
     *
     * @param recorder The recorder to be removed from the list of recorders.
     * @return true if the recorder was removed, false if not.
     */
    boolean removeRecorder(@Nonnull final JournalRecorder recorder);

    /**
     * Get an unmodifiable collection of the recorders to which journal entries are added.
     *
     * @return an unmodifiable collection of the recorders to which journal entries are added.
     */
    Collection<JournalRecorder> getRecorders();

    /**
     * Mark the beginning of a new phase of stitching. This records a relevant entry to all
     * recorders in the journal.
     *
     * @param phase The phase whose beginning should be marked.
     */
    void markPhase(@Nonnull final StitchingPhase phase);

    /**
     * Mark the beginning of a new stitching operation. This records a relevant entry to all
     * recorders in the journal. Mark an operation to indicate which piece of code is
     * responsible for a following set of changes.
     *
     * Sets the ongoing operation. It is illegal to call when there is already an ongoing operation.
     *
     * @param operation The {@link JournalableOperation} that is beginning.
     */
    void recordOperationBeginning(@Nonnull final JournalableOperation operation);

    /**
     * Mark the end of the current stitching operation. This records a relevant entry to all
     * recorders in the journal. Mark an operation to indicate which piece of code is
     * responsible for the preceding set of changes.
     *
     * Clears the ongoing operation. It is illegal to call when there is no ongoing operation.
     */
    void recordOperationEnding();

    /**
     * Get the ongoing operation whose changes to the topology are being recorded by the journal.
     * The ongoing operation is set by {@link #recordOperationBeginning(JournalableOperation)}
     * and cleared by {@link #recordOperationEnding()}. Note that
     * {@link #recordOperationException(String, Exception)} does NOT clear the ongoing operation.
     *
     * @return the ongoing operation whose changes to the topology are being recorded by the journal.
     *         If no operation is currently ongoing, returns {@link Optional#empty()}.
     */
    Optional<JournalableOperation> getOngoingOperation();

    /**
     * Record an exception triggered by performing an operation.
     * These exceptions end the phase but all other operations continue to be performed.
     *
     * @param message A message providing context to the exception.
     * @param e The exception triggered by the operation.
     */
    void recordOperationException(@Nonnull final String message, @Nonnull final Exception e);

    /**
     * Record a set of semantic differences to the journal.
     * If the semantic differences are empty or {@link JournalOptions} nothing is recorded.
     * If the verbosity level is set to preamble only, only the preamble is recorded.
     *
     * @param changeset The changeset whose semantic differences should be recorded.
     */
    void recordSemanticDifferences(@Nonnull final IJournalChangeset<T> changeset);

    /**
     * Record a grouped set of changes to {@link JournalableEntity} objects to this {@link IStitchingJournal}.
     * Typical uses of this method will usually record the results of a
     * {@link com.vmturbo.stitching.TopologicalChangelog.TopologicalChange} on one or more entities
     * in the topology.
     *
     * Note that implementations of this method MUST apply changes by passing a changeset to the consumer
     * in order for stitching to be correctly applied.
     *
     * @param changesetPreamble A preamble describing at a high level the set of changes to follow.
     * @param changesetConsumer A consumer of the changeset. This consumer should add {@link JournalableEntity}
     *                          entries to the changeset prior to mutating the {@link JournalableEntity}.
     *                          On exit of the consumer method, a collection of semantic differences
     *                          of all the changes made to all the entries in the changeset will be
     *                          recorded to this {@link IStitchingJournal}.
     */
    void recordChangeset(@Nonnull final String changesetPreamble,
        @Nonnull final Consumer<JournalChangeset<T>> changesetConsumer);

    /**
     * Record information about the topology associated with the journal entries in this stitching journal.
     * If the journal is non-empty, also updates Prometheus metrics related to stitching.
     *
     * @param topologyInfo information about the topology associated with the journal
     *                     entries in this stitching journal.
     * @param metrics The metrics to be recorded in the journal.
     */
    void recordTopologyInfoAndMetrics(@Nonnull final TopologyInfo topologyInfo,
                                      @Nonnull final StitchingMetrics metrics);

    /**
     * Record information about the size of the topology.
     *
     * @param topologySizes The size of the topology as defined by a count for the number of each type
     *                      of entity in the topology.
     */
    void recordTopologySizes(@Nonnull final Map<EntityType, Integer> topologySizes);

    /**
     * Record a miscellaneous message.
     */
    void recordMessage(@Nonnull final String message);

    /**
     * Record information about targets to the journal.
     *
     * @param targetEntrySupplier A supplier for a list of target entries.
     */
    void recordTargets(@Nonnull final Supplier<List<TargetEntry>> targetEntrySupplier);

    /**
     * Flush all recorders associated with this journal.
     *
     * See {@link JournalRecorder#flush()}.
     */
    void flushRecorders();

    /**
     * Dump the topology to the journal. Only entities that pass this journal's
     * {@link JournalFilter#shouldEnter(JournalableEntity)} filter will be dumped.
     *
     * @param entityStream A stream of the entities in the topology.
     */
    void dumpTopology(@Nonnull final Stream<T> entityStream);

    /**
     * Whether the topology for this journal should be dumped before pre-stitching.
     *
     * @return Whether the topology for this journal should be dumped before pre-stitching.
     */
    default boolean shouldDumpTopologyBeforePreStitching() {
        return getJournalOptions()
            .getTopologyDumpOptionsList()
            .contains(TopologyDumpOptions.DUMP_BEFORE_PRE_STITCHING);
    }

    /**
     * Whether the topology for this journal should be dumped after post-stitching.
     *
     * @return Whether the topology for this journal should be dumped after post-stitching.
     */
    default boolean shouldDumpTopologyAfterPostStitching() {
        return getJournalOptions()
            .getTopologyDumpOptionsList()
            .contains(TopologyDumpOptions.DUMP_AFTER_POST_STITCHING);
    }

    /**
     * Get the options controlling how the journal behaves such as its verbosity and how changes
     * are grouped.
     *
     * @return The journal's {@link JournalOptions}.
     */
    @Nonnull
    JournalOptions getJournalOptions();

    /**
     * Get metrics about the journal.
     *
     * @return metrics about the journal.
     */
    @Nonnull
    StitchingMetrics getMetrics();

    /**
     * Construct a child journal that operates on the {@link NEXT_ENTITY} enitty type instead of the
     * {@link T} entity type of the current journal.
     *
     * This method call exists to support transitioning the journal from operating on entities of type
     * {@link T} to entities of type {@link NEXT_ENTITY}. This happens, for example, when transitioning
     * from the main-stitching phase to the post-stitching phase (where we move from creating journal
     * entries from StitchingEntity types to TopologyEntity types.
     *
     * The created child journal will retain identical configuration and all associated recorders
     * of the parent journal. The metrics of the child journal will match the metrics of the
     * parent journal.
     *
     * @param semanticDiffer The semantic differ to use when obtaining differences for the entity of the
     *                       new type.
     * @param <NEXT_ENTITY> The entity type of the new journal.
     * @return A child journal that can operate on {@link NEXT_ENTITY} type entities.
     */
    <NEXT_ENTITY extends JournalableEntity<NEXT_ENTITY>>
        IStitchingJournal<NEXT_ENTITY> childJournal(@Nonnull final SemanticDiffer<NEXT_ENTITY> semanticDiffer);

    /**
     * Metrics about what happened during stitching.
     * For more details see {@link com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.StitchingMetrics}.
     */
    class StitchingMetrics {
        private int totalChangesetsGenerated;

        private int totalChangesetsIncluded;

        private int totalEmptyChangests;

        private Duration timeTaken;

        public StitchingMetrics() {
            this.totalChangesetsGenerated = 0;
            this.totalChangesetsIncluded = 0;
            this.totalEmptyChangests = 0;
            this.timeTaken = Duration.ofSeconds(0);
        }

        /**
         * Merge the other metrics into this one.
         *
         * @param metrics Metrics to merge into this one.
         */
        public void mergeWith(@Nonnull final StitchingMetrics metrics) {
            this.totalChangesetsGenerated += metrics.totalChangesetsGenerated;
            this.totalChangesetsIncluded += metrics.totalChangesetsIncluded;
            this.totalEmptyChangests += metrics.totalEmptyChangests;
            this.timeTaken = this.timeTaken.plus(metrics.timeTaken);
        }

        /**
         * Add in information about the ongoing operation into this one.
         *
         * @param changesetsGenerated The number of changesets generated to add into the metrics.
         * @param changesetsGenerated The number of changesets included to add into the metrics.
         * @param emptyChangests The number of empty changests to add to the metrics.
         * @param operationDuration The time taken by the operation to do its job.
         */
        public void add(final int changesetsGenerated,
                        final int changesetsIncluded,
                        final int emptyChangests,
                        @Nonnull final Duration operationDuration) {
            totalChangesetsGenerated += changesetsGenerated;
            totalChangesetsIncluded += changesetsIncluded;
            totalEmptyChangests += emptyChangests;
            timeTaken = timeTaken.plus(operationDuration);
        }

        public int getTotalChangesetsGenerated() {
            return totalChangesetsGenerated;
        }

        public int getTotalChangesetsIncluded() {
            return totalChangesetsIncluded;
        }

        public int getTotalEmptyChangests() {
            return totalEmptyChangests;
        }

        public Duration getTimeTaken() {
            return timeTaken;
        }
    }

    /**
     * An interface for the basics of a JournalChangeset, which provides grouping functionalities for
     * changes to be entered into the {@link IStitchingJournal} together.
     *
     * @param <T> The type of entity whose changes are being recorded in the changeset.
     */
    interface IJournalChangeset<T extends JournalableEntity<T>> {

        /**
         * Get the high-level descriptions that describes the action of the changeset.
         *
         * @return the the high-level descriptions that describes the action of the changeset.
         */
        List<String> getChangesetPreambles();

        /**
         * Get the entities mutated and/or removed in the changeset. Note that these are the original
         * entities, not the generated snapshots so that they can be compared across time even as
         * the entities change.
         *
         * Entities in the stream are returned with mutated entities first, and removed entities after.
         *
         * @return The entities changed in the changeset.
         */
        Stream<T> changedEntities();

        /**
         * Get the number of entities modified by this changest.
         *
         * @return the number of entities modified by this changest.
         */
        int changedEntityCount();

        /**
         * Get the map of all mutated entities to their original snapshots in the changeset.
         *
         * @return The map of all mutated entities to their original snapshots in the changeset.
         */
        Map<T, T> getMutatedEntities();

        /**
         * Get the collection of all entities in the changeset that were removed.
         *
         * @return The collection of all entities in the changeset that were removed.
         */
        Collection<T> getRemovedEntities();

        /**
         * Get the collection of all new entities in the changeset that were added.
         *
         * @return The collection of all new entities in the changeset that were added.
         */
        Collection<T> getAddedEntities();

        /**
         * Get the index of the changeset. Changesets can be sorted by their index.
         *
         * @return The index of a changeset. A lower number indicates a changeset was created before a changeset
         *         with a higher number.
         */
        int getChangesetIndex();

        /**
         * Generate a description of the semantic differences for all the entries added to this
         * {@link JournalChangeset}.
         *
         * @param semanticDiffer The differ to use in generating the semantic differences.
         * @param format The format to use in generating differences.
         *
         * @return A list of details of the semantic differences for the entries in this changeset.
         */
        default List<String> semanticDifferences(@Nonnull final SemanticDiffer<T> semanticDiffer,
                                           @Nonnull final FormatRecommendation format) {
            final Stream<String> changeDifferences = getMutatedEntities().entrySet().stream()
                .map(entry -> semanticDiffer.semanticDiff(entry.getValue(), entry.getKey(), format))
                .filter(difference -> !difference.isEmpty());
            final Stream<String> removalDescriptions = getRemovedEntities().stream()
                .map(JournalableEntity::removalDescription)
                .filter(description -> !description.isEmpty());

            return Stream.concat(changeDifferences, removalDescriptions)
                .collect(Collectors.toList());
        }

        /**
         * Get the number of internal changesets involved in this changeset. By default, a changeset
         * has a size of 1. Merged changesets may have a larger size.
         *
         * @return The size of the changeset.
         */
        default int getChangesetSize() {
            return 1;
        }
    }

    /**
     * A {@link JournalChangeset} provides a natural grouping for a set of related changes in the journal.
     * For example, a merge change in stitching might involve merging data from EntityA onto EntityB.
     * This results in changes to both EntityA, EntityB, and providers and consumers to both those entities.
     * This group of changes is naturally related and when inspecting journal results after stitching is
     * complete, journal readers would benefit from seeing those changes grouped together in a single
     * {@link JournalChangeset}.
     *
     * Changesets can also record the removal of entities from the topology.
     *
     * @param <T> The type of the {@link JournalableEntity} whose changes will be recorded in the journal.
     */
    class JournalChangeset<T extends JournalableEntity<T>> implements IJournalChangeset<T> {
        private final JournalFilter filter;

        /**
         * A string to introduce the changeset in the journal.
         */
        private final String changesetPreamble;

        /**
         * The index number of a changeset. A lower number indicates a changeset was create prior to
         * a changeset with a higher number.
         */
        private final int changesetIndex;

        /**
         * A map of Journalable -> Snapshot of that journalable.
         *
         * Entries in this map are not overwritten, requests to add a change to the changeset
         * when that Journalable was already added to the changeset results in keeping an original
         * snapshot, not an overwritten snapshot.
         *
         * Use an IdentityHashMap to avoid expensive equality checks and because reference equality
         * is correct for the purpose of recording entries to the journal changeset.
         */
        private final Map<T, T> mutated = new IdentityHashMap<>();

        /**
         * The list of entities removed as part of this changeset.
         *
         * The list is ordered. Note that it makes no sense to add the same entity multiple times
         * to this list.
         *
         * Removed entities for a changeset are entered into the journal AFTER changes in the changeset
         * are entered into the journal.
         */
        private final List<T> removed = new ArrayList<>();


        /**
         * The list of entities added as part of this changeset.
         *
         * The list is ordered. Note that it makes no sense to add the same entity multiple times
         * to this list.
         *
         * Added entities for a changeset are entered into the journal AFTER changes in the changeset
         * are entered into the journal.
         */
        private final List<T> added = new ArrayList<>();

        /**
         * Create a new journal changeset. Marked as private so that it can be created via the journal.
         *
         * @param changesetPreamble A string describing the action of the changeset.
         * @param filter A filter for entries in the journal. Only entries that pass the filter
         *               will be added.
         * @param changesetIndex The changesetIndex. A lower number indicates the changeset was created
         *                       before a changeset with a higher number.
         */
        public JournalChangeset(@Nonnull final String changesetPreamble,
                                @Nonnull final JournalFilter filter,
                                final int changesetIndex) {
            this.changesetPreamble = Objects.requireNonNull(changesetPreamble);
            this.filter = Objects.requireNonNull(filter);
            this.changesetIndex = changesetIndex;
        }

        /**
         * Call before applying changes to an entity in the topology in order to allow that
         * change to be traced in the journal. This works by adding an entry and its snapshot
         * to the changeset. If the entry was already added to the changeset, it will not be
         * re-added. Instead, the earlier snapshot will be maintained and the new request
         * to re-add will be ignored.
         *
         * If added, a snapshot of the entry will be taken and recorded. These entries and their
         * snapshots can be diffed to generate a list of semantic differences via the
         * {@link #semanticDifferences} method.
         *
         * @param entry The entry to add to the changeset along with its snapshot.
         * @return true if the entry was successfully added (ie it was not already in the changeset)
         *         false if the entry could not be added because it was already in the changeset.
         */
        public boolean beforeChange(@Nonnull final T entry) {
            if (!mutated.containsKey(entry)) {
                // TODO: Need to be merge-aware
                if (filter.shouldEnter(entry)) {
                    mutated.put(entry, entry.snapshot());
                    return true;
                }
            }

            return false;
        }

        /**
         * Add a removal to the changeset. Note that removed entities are NOT snapshotted.
         * If the removal of this entity was already added to the changeset, the additional
         * removal is ignored.
         *
         * If the changeset has a filter, the removal will only be added if the entity passes
         * the filter.
         *
         * @param entry The entry to add to the changeset along.
         * @return true if the entry was successfully added (ie it was not already in the changeset)
         *         false if the entry could not be added because it was already added as a removal
         *         in the changeset.
         */
        public boolean observeRemoval(@Nonnull final T entry) {
            if (filter.shouldEnter(entry)) {
                removed.add(entry);
                return true;
            }

            return false;
        }

        /**
         * Add an addition to the changeset. Note that added entities are NOT snapshotted.
         * If the addition of this entity was already added to the changeset, the additional
         * addition is ignored.
         *
         * If the changeset has a filter, the addition will only be added if the entity passes
         * the filter.
         *
         * @param entry The entry to add to the changeset along.
         * @return true if the entry was successfully added (ie it was not already in the changeset)
         *         false if the entry could not be added because the filter doesn't permit the entry
         *         in the changeset.
         */
        public boolean observeAddition(@Nonnull final T entry) {
            if (filter.shouldEnter(entry)) {
                added.add(entry);
                return true;
            }

            return false;
        }

        @Override
        public int changedEntityCount() {
            return mutated.size() + removed.size() + added.size();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Stream<T> changedEntities() {
            return Stream.of(mutated.keySet().stream(), removed.stream(), added.stream())
                .reduce(Stream::concat).orElseGet(Stream::empty);
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
            return changesetIndex;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<String> getChangesetPreambles() {
            return Collections.singletonList(changesetPreamble);
        }
    }
}
