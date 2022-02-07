package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal.CoverageJournalEntry;

/**
 * The output of {@link CloudCommitmentCoverageAllocator}, containing cloud commitment coverage
 * allocations for topology entities, along with the total coverage (source/input coverage to the allocator
 * + allocator coverage).
 */

@HiddenImmutableTupleImplementation
@Immutable
public interface CloudCommitmentCoverageAllocation {

    /**
     * An empty allocation (both total coverage and allocated coverage are empty).
     */
    CloudCommitmentCoverageAllocation EMPTY_ALLOCATION =
            CloudCommitmentCoverageAllocation.from(
                    ImmutableTable.of(),
                    ImmutableTable.of(),
                    Collections.emptyList());

    /**
     * The immutable total coverage table.
     * @return An immutable {@link Table} of the total coverage allocated after execution of
     * {@link CloudCommitmentCoverageAllocation}. The total coverage is a summation of the input
     * coverage to the allocator and any coverage allocated as part of its execution.
     */
    @Nonnull
    Table<Long, Long, CloudCommitmentAmount> totalCoverageTable();

    /**
     * The immutable coverage table of net-new allocations from the {@link CloudCommitmentCoverageAllocator}.
     * @return An immutable {@link Table} of the coverage added as part of {@link CloudCommitmentCoverageAllocator}
     * analysis. The table is formatted as {@literal <Entity OID, Commitment OID, Coverage Amount>}.
     */
    @Nonnull
    Table<Long, Long, CloudCommitmentAmount> allocatorCoverageTable();

    /**
     * The immutable list of coverage journal entries, representing the individual steps resulting in {@link #allocatorCoverageTable()}.
     * @return The immutable list of coverage journal entries.
     */
    @Nonnull
    List<CoverageJournalEntry> coverageJournalEntries();

    /**
     * Builds an instance of {@link CloudCommitmentCoverageAllocation} from {@code totalCoverage}
     * and {@code allocatorCoverage}. A copy of both coverage tables will be made, such that the
     * {@link CloudCommitmentCoverageAllocation} returned will be immutable and independent of
     * changes to either table.
     *
     * @param totalCoverage A {@link Table} containing {@literal <Entity OID, Commitment OID, Coverage Allocation Amount>}
     *                      of total coverage after {@link CloudCommitmentCoverageAllocator} analysis.
     * @param allocatorCoverage A {@link Table} containing {@literal <Entity OID, Commitment OID, Coverage Allocation Amount>}
     *                          of coverage added by {@link CloudCommitmentCoverageAllocator}.
     * @param journalEntryList The coverage journal entry list.
     * @return A newly created instance of {@link CloudCommitmentCoverageAllocation}.
     */
    @Nonnull
    static CloudCommitmentCoverageAllocation from(
            @Nonnull Table<Long, Long, CloudCommitmentAmount> totalCoverage,
            @Nonnull Table<Long, Long, CloudCommitmentAmount> allocatorCoverage,
            @Nonnull List<CoverageJournalEntry> journalEntryList) {
        return CloudCommitmentCoverageAllocationTuple.of(
                ImmutableTable.copyOf(totalCoverage),
                ImmutableTable.copyOf(allocatorCoverage),
                journalEntryList);
    }
}
