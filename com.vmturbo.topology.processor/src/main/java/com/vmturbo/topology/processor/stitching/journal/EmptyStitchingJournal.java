package com.vmturbo.topology.processor.stitching.journal;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.journal.JournalableOperation;
import com.vmturbo.stitching.journal.SemanticDiffer;

/**
 * A very basic implementation of the {@link IStitchingJournal} interface that ignores all requests
 * to enter information into the journal.
 *
 * This enables, in production, when we do not want to enter anything in the journal, to have
 * almost zero overhead on all journal operations.
 *
 * @param <T> The type of the entity that can be recorded in this journal instance.
 */
public class EmptyStitchingJournal<T extends JournalableEntity<T>> implements IStitchingJournal<T> {
    @Override
    public void addRecorder(@Nonnull JournalRecorder recorder) {
        // Do nothing
    }

    @Override
    public boolean removeRecorder(@Nonnull JournalRecorder recorder) {
        return false;
    }

    @Override
    public Collection<JournalRecorder> getRecorders() {
        return Collections.emptyList();
    }

    @Override
    public void markPhase(@Nonnull StitchingPhase phase) {
        // Do nothing
    }

    @Override
    public void recordOperationBeginning(@Nonnull JournalableOperation operation) {
        // Do nothing
    }

    @Override
    public void recordOperationEnding() {
        // Do nothing
    }

    @Override
    public Optional<JournalableOperation> getOngoingOperation() {
        return Optional.empty();
    }

    @Override
    public void recordOperationException(@Nonnull String message, @Nonnull Exception e) {
        // Do nothing
    }

    @Override
    public void recordSemanticDifferences(@Nonnull IJournalChangeset<T> changeset) {
        // Do nothing
    }

    @Override
    public void recordChangeset(@Nonnull String changesetPreamble, @Nonnull Consumer<JournalChangeset<T>> journalChangesetConsumer) {
        // Do nothing
    }

    @Override
    public void recordTopologyInfoAndMetrics(@Nonnull TopologyInfo topologyInfo, @Nonnull StitchingMetrics metrics) {
        // Do nothing
    }

    @Override
    public void recordTopologySizes(@Nonnull Map<EntityType, Integer> topologySizes) {
        // Do nothing
    }

    @Override
    public void recordMessage(@Nonnull String message) {
        // Do nothing
    }

    @Override
    public void recordTargets(@Nonnull Supplier<List<TargetEntry>> targetEntrySupplier) {
        // Do nothing
    }

    @Override
    public void flushRecorders() {
        // Do nothing
    }

    @Override
    public void dumpTopology(@Nonnull Stream<T> entityStream) {
        // Do nothing
    }

    @Override
    public boolean shouldDumpTopologyBeforePreStitching() {
        return false;
    }

    @Override
    public boolean shouldDumpTopologyAfterPostStitching() {
        return false;
    }

    @Nonnull
    @Override
    public JournalOptions getJournalOptions() {
        return JournalOptions.getDefaultInstance();
    }

    @Nonnull
    @Override
    public StitchingMetrics getMetrics() {
        return new StitchingMetrics();
    }

    @Override
    public <NEXT_ENTITY extends JournalableEntity<NEXT_ENTITY>>
    IStitchingJournal<NEXT_ENTITY> childJournal(@Nonnull SemanticDiffer<NEXT_ENTITY> semanticDiffer) {
        return new EmptyStitchingJournal<>();
    }
}
