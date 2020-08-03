package com.vmturbo.topology.processor.stitching.journal;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.Stitching.ChangeGrouping;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.journal.JournalableOperation;
import com.vmturbo.stitching.journal.SemanticDiffer;
import com.vmturbo.stitching.journal.SemanticDiffer.EmptySemanticDiffer;

/**
 * A journal to record changes to the topology made during various stitching phases.
 *
 * @param <T> The type of the entity that can be recorded in this journal instance.
 */
@NotThreadSafe
public class StitchingJournal<T extends JournalableEntity<T>> implements IStitchingJournal<T> {

    private static final String TOPOLOGY_TYPE_LABEL = "topology_type";

    private static final DataMetricSummary JOURNAL_CHANGESETS_GENERATED_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_journal_changesets_generated")
        .withHelp("Number of changesets made during a stitching pass. " +
            "This is across all stitching phases (pre/main/post).")
        .withLabelNames(TOPOLOGY_TYPE_LABEL)
        .build()
        .register();

    private static final DataMetricSummary JOURNAL_CHANGESETS_RECORDED_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_journal_changesets_recorded")
        .withHelp("Number of changests recorded to the stitching journal during a stitching pass.")
        .withLabelNames(TOPOLOGY_TYPE_LABEL)
        .build()
        .register();

    private static final DataMetricSummary JOURNAL_TIME_TAKEN_SUMMARY = DataMetricSummary.builder()
        .withName("tp_stitching_journal_duration_seconds")
        .withHelp("Duration to execute all stitching phases and record the changes " +
            "in the journal for a stitching pass.")
        .withLabelNames(TOPOLOGY_TYPE_LABEL)
        .build()
        .register();

    private final Collection<JournalRecorder> recorders;

    private final JournalFilter filter;

    private final JournalOptions journalOptions;

    private int nextChangesetIndex = 0;

    private final ChangesetMerger<T> changesetMerger = new ChangesetMerger<>();

    private final SemanticDiffer<T> semanticDiffer;

    private Optional<OngoingOperation> ongoingOperation = Optional.empty();

    private final Clock clock;

    private final StitchingMetrics stitchingMetrics;

    public StitchingJournal() {
        this(new IncludeAllFilter(),
            JournalOptions.getDefaultInstance(),
            new EmptySemanticDiffer<>(JournalOptions.getDefaultInstance().getVerbosity()),
            new ArrayList<>(),
            Clock.systemUTC());
    }

    public StitchingJournal(@Nonnull final JournalFilter filter,
                            @Nonnull final SemanticDiffer<T> semanticDiffer) {
        this(filter,
            JournalOptions.getDefaultInstance(),
            semanticDiffer,
            new ArrayList<>(),
            Clock.systemUTC());
    }

    public StitchingJournal(@Nonnull final SemanticDiffer<T> semanticDiffer,
                            @Nonnull final JournalRecorder... recorders) {
        this(new IncludeAllFilter(),
            JournalOptions.getDefaultInstance(),
            semanticDiffer,
            Arrays.asList(recorders),
            Clock.systemUTC());
    }

    public StitchingJournal(@Nonnull final JournalFilter filter,
                            final JournalOptions journalOptions,
                            @Nonnull final SemanticDiffer<T> semanticDiffer,
                            @Nonnull final Collection<JournalRecorder> recorders,
                            @Nonnull final Clock clock) {
        this.filter = Objects.requireNonNull(filter);
        this.journalOptions = journalOptions;
        this.recorders = Objects.requireNonNull(recorders);
        this.semanticDiffer = Objects.requireNonNull(semanticDiffer);
        this.clock = Objects.requireNonNull(clock);
        this.stitchingMetrics = new StitchingMetrics();

        // Set the semantic differ's verbosity to match that in the journal options.
        this.semanticDiffer.setVerbosity(this.journalOptions.getVerbosity());
    }

    @Override
    public void addRecorder(@Nonnull final JournalRecorder recorder) {
        recorders.add(Objects.requireNonNull(recorder));
    }

    @Override
    public boolean removeRecorder(@Nonnull final JournalRecorder recorder) {
        return recorders.remove(recorder);
    }

    @Override
    public Collection<JournalRecorder> getRecorders() {
        return Collections.unmodifiableCollection(recorders);
    }

    @Override
    public void markPhase(@Nonnull final StitchingPhase phase) {
        recordToAll(phase, JournalRecorder::recordPhaseStart);
    }

    @Override
    public void recordOperationBeginning(@Nonnull final JournalableOperation operation) {
        recordOperationBeginning(operation, Collections.emptyList());
    }

    @Override
    public void recordOperationBeginning(@Nonnull final JournalableOperation operation,
                                         @Nonnull final Collection<String> details) {
        Preconditions.checkState(!ongoingOperation.isPresent());
        final OngoingOperation oOperation = new OngoingOperation(operation, clock.millis(), details);
        ongoingOperation = Optional.of(oOperation);
        semanticDiffer.setVerbosity(currentOperationVerbosity());

        // Only record the operation start if the operation makes any changes. This prevents
        // having lots of log messages related to operations that do nothing.
        if (journalOptions.getRecordEmptyOperations()) {
            // If recordEmptyOperations is set, no need to wait for the operation to make
            // a change before recording it.
            recorders.forEach(recorder -> recorder.recordOperationStart(
                oOperation.getOperation(), oOperation.getOperationDetails()));
            oOperation.markRecorded();
        }
    }

    @Override
    public void recordOperationEnding() {
        Preconditions.checkState(ongoingOperation.isPresent());
        if (shouldRecord()) {
            recordMergedChangesets();
        }

        final OngoingOperation oOperation = ongoingOperation.get();
        final Duration operationDuration = Duration.ofMillis(clock.millis() - oOperation.getStartTimeMillis());

        if (oOperation.isRecorded()) {
            // Only add the message about the operation ending if the operation start message was recorded
            // (ie the operation made at least one change).
            recorders.forEach(recorder -> recorder.recordOperationEnd(oOperation.getOperation(),
                oOperation.getOperationDetails(),
                oOperation.getChangesetsGenerated(),
                oOperation.getChangesetsIncluded(),
                oOperation.getEmptyChangests(),
                operationDuration));
        }

        stitchingMetrics.add(oOperation.getChangesetsGenerated(),
            oOperation.getChangesetsIncluded(),
            oOperation.getEmptyChangests(),
            operationDuration);

        ongoingOperation = Optional.empty();
        semanticDiffer.setVerbosity(journalOptions.getVerbosity());
    }

    @Override
    public Optional<JournalableOperation> getOngoingOperation() {
        return ongoingOperation.map(OngoingOperation::getOperation);
    }

    @Override
    public void recordOperationException(@Nonnull final String message,
                                         @Nonnull final Exception e) {
        // Lazily record message to avoid work of building it if there's nothing
        // to record.
        final Supplier<String> errorMessageSupplier =
            () -> message + "\n" + ExceptionUtils.getStackTrace(e);
        recordToAll(errorMessageSupplier, JournalRecorder::recordError);
    }

    @Override
    public void recordChangeset(@Nonnull String changesetPreamble,
                                @Nonnull Consumer<JournalChangeset<T>> journalChangesetConsumer) {
        try {
            // Capture a record of all semantic differences captured to the changeset by the consumer.
            final JournalChangeset<T> changeset = new JournalChangeset<>(changesetPreamble,
                filter, nextChangesetIndex);
            nextChangesetIndex++;
            journalChangesetConsumer.accept(changeset);
            ongoingOperation.ifPresent(operation ->
                operation.addChangesetsGenerated(changeset.getChangesetSize()));

            if (shouldRecord()) {
                recordChangeset(changeset);
            }
        } catch (RuntimeException e) {
            // Record and then rethrow the exception. The journal should not interrupt the
            // control flow of its clients.
            if (shouldRecord()) {
                recordOperationException(changesetPreamble, e);
            }

            throw e;
        }
    }

    @Override
    public void recordSemanticDifferences(@Nonnull final IJournalChangeset<T> changeset) {
        if (ongoingOperation.isPresent()) {
            if (ongoingOperation.get().getChangesetsIncluded() > journalOptions.getMaxChangesetsPerOperation()) {
                // Don't record any additional differences for an operation when we've exceeded
                // the limit for that operation. This will prevent us from stalling out the entire
                // topology pipeline due to the journal on a very large topology.
                return;
            }
        }

        if (changeset.changedEntityCount() > 0) {
            final List<String> semanticDifferences = changeset
                .semanticDifferences(semanticDiffer, currentOperationFormat());

            if (!semanticDifferences.isEmpty()) {
                ongoingOperation.ifPresent(operation ->
                    operation.addChangesetsIncluded(changeset.getChangesetSize()));
                recorders.forEach(recorder -> recorder.recordChangeset(changeset.getChangesetPreambles(),
                    semanticDifferences, currentOperationVerbosity(), currentOperationFormat()));
            } else {
                ongoingOperation.ifPresent(operation ->
                    operation.addEmptyChangests(changeset.getChangesetSize()));
            }
        }
    }

    @Override
    public void recordTopologyInfoAndMetrics(@Nonnull final TopologyInfo topologyInfo,
                                             @Nonnull final StitchingMetrics metrics) {
        // Record to the recorders.
        recorders.forEach(recorder -> recorder.recordTopologyInfoAndMetrics(topologyInfo, metrics));

        // Update the related Prometheus metrics.
        JOURNAL_CHANGESETS_GENERATED_SUMMARY.labels(topologyInfo.getTopologyType().name())
            .observe((double)metrics.getTotalChangesetsGenerated());
        JOURNAL_CHANGESETS_RECORDED_SUMMARY.labels(topologyInfo.getTopologyType().name())
            .observe((double)metrics.getTotalChangesetsIncluded());
        JOURNAL_TIME_TAKEN_SUMMARY.labels(topologyInfo.getTopologyType().name())
            .observe(metrics.getTimeTaken().toMillis() / 1000.0);
    }

    @Override
    public void recordTopologySizes(@Nonnull Map<EntityType, Integer> topologySizes) {
        recorders.forEach(recorder -> recorder.recordTopologySizes(topologySizes));
    }

    @Override
    public void recordMessage(@Nonnull String message) {
        recorders.forEach(recorder -> recorder.recordMessage(message));
    }

    @Override
    public void recordTargets(@Nonnull final Supplier<List<TargetEntry>> targetEntrySupplier) {
        if (shouldRecord() && journalOptions.getIncludeTargetList()) {
            final List<TargetEntry> entries = targetEntrySupplier.get();
            recorders.forEach(recorder -> recorder.recordTargets(entries));
        }
    }

    @Override
    public void flushRecorders() {
        // Flush the recorders
        recorders.forEach(JournalRecorder::flush);
    }

    @Override
    public void dumpTopology(@Nonnull Stream<T> entityStream) {
        if (shouldRecord()) {
            recordMessage(JournalRecorder.prettyPrintCenter("DUMP TOPOLOGY: START", '=', 80));
            entityStream
                .filter(filter::shouldEnter)
                .forEach(entity -> {
                    final String serializedEntity = semanticDiffer.dumpEntity(entity);
                    recordMessage(serializedEntity);
                });
            recordMessage(JournalRecorder.prettyPrintCenter("DUMP TOPOLOGY: END", '=', 80) + "\n");
        }
    }

    @Nonnull
    @Override
    public JournalOptions getJournalOptions() {
        return journalOptions;
    }

    @Nonnull
    @Override
    public StitchingMetrics getMetrics() {
        return stitchingMetrics;
    }

    @Override
    public <NEXT_ENTITY extends JournalableEntity<NEXT_ENTITY>>
    IStitchingJournal<NEXT_ENTITY> childJournal(@Nonnull final SemanticDiffer<NEXT_ENTITY> semanticDiffer) {
        final StitchingJournal<NEXT_ENTITY> child = new StitchingJournal<>(filter, journalOptions,
            semanticDiffer, recorders, clock);
        child.getMetrics().mergeWith(stitchingMetrics);

        return child;
    }

    /**
     * Check if the journal state is set up so that added changesets should be merged.
     * A changeset should be merged if the ongoing operation says to recommends that changes
     * be logged in pretty format and the journal options suggest combining related changes.
     *
     * @return Whether changesets should be merged.
     */
    @VisibleForTesting
    boolean shouldMergeChangesets() {
        final boolean isPrettyOperation = getOngoingOperation()
            .map(operation -> operation.getFormatRecommendation() == FormatRecommendation.PRETTY)
            .orElse(false);

        return isPrettyOperation &&
            journalOptions.getChangeGrouping() == ChangeGrouping.COMBINE_RELATED;
    }

    /**
     * Get the format for the current operation.
     *
     * @return The format for the current operation. If there is no current operation, return
     *         {@link com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation#PRETTY}.
     */
    @Nonnull
    @VisibleForTesting
    FormatRecommendation currentOperationFormat() {
        if (journalOptions.getVerbosity() == Verbosity.COMPLETE_VERBOSITY) {
            // Only Pretty format is permitted when COMPLETE_VERBOSITY is enabled.
            return FormatRecommendation.PRETTY;
        }

        return getOngoingOperation()
            .map(JournalableOperation::getFormatRecommendation)
            .orElse(FormatRecommendation.PRETTY);
    }

    @Nonnull
    @VisibleForTesting
    Verbosity currentOperationVerbosity() {
        if (journalOptions.getVerbosity() != Verbosity.LOCAL_CONTEXT_VERBOSITY) {
            return journalOptions.getVerbosity();
        }

        return getOngoingOperation()
            // Compact format operations lowers verbosity when the verbosity is set to LOCAL_CONTEXT.
            .map(operation -> operation.getFormatRecommendation() == FormatRecommendation.COMPACT ?
                Verbosity.CHANGES_ONLY_VERBOSITY : Verbosity.LOCAL_CONTEXT_VERBOSITY)
            .orElse(journalOptions.getVerbosity());
    }

    private void recordChangeset(@Nonnull final JournalChangeset<T> changeset) {
        ongoingOperation.ifPresent(operation -> {
            // If the operation start was not yet recorded to the journal, record it when we first
            // observe a changeset for that operation.
            if (!operation.isRecorded()) {
                recorders.forEach(recorder -> recorder.recordOperationStart(
                    operation.getOperation(), operation.getOperationDetails()));
                operation.markRecorded();
            }
        });

        if (shouldMergeChangesets()) {
            // Merge the changeset. The merged changesets will be grouped by like entities and
            // merged changests will be recorded when the operation completes.
            changesetMerger.add(changeset);
        } else {
            // Record the changeset immediately because it does not need to be merged.
            recordSemanticDifferences(changeset);
        }
    }

    private void recordMergedChangesets() {
        changesetMerger.recordAll(this);
        changesetMerger.clear();
    }

    private boolean shouldRecord() {
        return !recorders.isEmpty();
    }

    private <ENTRY> void recordToAll(@Nonnull final Supplier<ENTRY> entrySupplier,
                                     @Nonnull final BiConsumer<JournalRecorder, ENTRY> recordingMethod) {
        if (shouldRecord()) {
            final ENTRY entry = entrySupplier.get();
            recorders.forEach(recorder -> recordingMethod.accept(recorder, entry));
        }
    }

    private <ENTRY> void recordToAll(@Nonnull final ENTRY entry,
                                     @Nonnull final BiConsumer<JournalRecorder, ENTRY> recordingMethod) {
        recorders.forEach(recorder -> recordingMethod.accept(recorder, entry));
    }

    /**
     * A simple container object for holding the main and post stitching journal.
     */
    public static class StitchingJournalContainer {
        private IStitchingJournal<StitchingEntity> mainStitchingJournal;
        private IStitchingJournal<TopologyEntity> postStitchingJournal;

        public StitchingJournalContainer() {
            mainStitchingJournal = null;
            postStitchingJournal = null;
        }

        public void setMainStitchingJournal(@Nonnull final IStitchingJournal<StitchingEntity> journal) {
            Preconditions.checkState(mainStitchingJournal == null,
                "Attempt to set main stitching journal after it has been previously set.");
            this.mainStitchingJournal = Objects.requireNonNull(journal);
        }

        public void setPostStitchingJournal(@Nonnull final IStitchingJournal<TopologyEntity> journal) {
            Preconditions.checkState(postStitchingJournal == null,
                "Attempt to set main stitching journal after it has been previously set.");
            this.postStitchingJournal = Objects.requireNonNull(journal);
        }

        public Optional<IStitchingJournal<StitchingEntity>> getMainStitchingJournal() {
            return Optional.ofNullable(mainStitchingJournal);
        }

        public Optional<IStitchingJournal<TopologyEntity>> getPostStitchingJournal() {
            return Optional.ofNullable(postStitchingJournal);
        }
    }

    /**
     * A small helper that stores the ongoing operation and some associated data points for that operation.
     */
    private static class OngoingOperation {
        private final JournalableOperation operation;
        private final long startTimeMillis;
        private final Collection<String> operationDetails;
        private int changesetsGenerated;
        private int changesetsIncluded;
        private int emptyChangests;
        private boolean recorded;

        public OngoingOperation(@Nonnull final JournalableOperation operation,
                                @Nonnull final long startTimeMillis,
                                @Nonnull final Collection<String> details) {
            this.operation = Objects.requireNonNull(operation);
            this.operationDetails = Objects.requireNonNull(details);
            this.startTimeMillis = startTimeMillis;
            this.changesetsGenerated = 0;
            this.changesetsIncluded = 0;
            this.emptyChangests = 0;
            this.recorded = false;
        }

        public void addChangesetsGenerated(int additionalChangesetsGenerated) {
            Preconditions.checkArgument(additionalChangesetsGenerated >= 0);
            this.changesetsGenerated += additionalChangesetsGenerated;
        }

        public void addChangesetsIncluded(int additionalChangesetsIncluded) {
            Preconditions.checkArgument(additionalChangesetsIncluded >= 0);
            this.changesetsIncluded += additionalChangesetsIncluded;
        }

        public Collection<String> getOperationDetails() {
            return operationDetails;
        }

        /**
         * Add to the empty changeset count.
         * Note that empty changests are also automatically added to the included count.
         *
         * @param emptyChangests The number of empty changests to add.
         */
        public void addEmptyChangests(int emptyChangests) {
            Preconditions.checkArgument(emptyChangests >= 0);
            this.emptyChangests += emptyChangests;
            addChangesetsIncluded(emptyChangests);
        }

        public int getChangesetsGenerated() {
            return changesetsGenerated;
        }

        public int getChangesetsIncluded() {
            return changesetsIncluded;
        }

        public int getEmptyChangests() {
            return emptyChangests;
        }

        public JournalableOperation getOperation() {
            return operation;
        }

        public long getStartTimeMillis() {
            return startTimeMillis;
        }

        public boolean isRecorded() {
            return recorded;
        }

        public void markRecorded() {
            recorded = true;
        }
    }
}
