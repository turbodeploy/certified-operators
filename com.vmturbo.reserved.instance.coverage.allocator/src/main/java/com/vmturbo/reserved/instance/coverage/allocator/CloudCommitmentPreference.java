package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;

/**
 * An interface for preferencing of cloud commitments within a single coverage group.
 */
public interface CloudCommitmentPreference {

    /**
     * The default preference, which will preference smaller commitments with more available capacity.
     */
    CloudCommitmentPreference DEFAULT_PREFERENCE = new DefaultCloudCommitmentPreference();

    /**
     * Sorts the provided {@code commitmentOids}, based on internal preferencing logic and the
     * provided {@code coverageJournal}.
     * @param coverageJournal The coverage journal, useful to check both capacity and available
     *                        coverage.
     * @param coverageTypeInfo The coverage type info.
     * @param commitmentOids The commitment OIDs to sort.
     * @return The sorted set of commitment OIDs.
     */
    @Nonnull
    SortedSet<Long> sortCommitments(@Nonnull CloudCommitmentCoverageJournal coverageJournal,
                                    @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                                    @Nonnull Set<Long> commitmentOids);

    /**
     * The default implementation of {@link CloudCommitmentPreference}.
     */
    class DefaultCloudCommitmentPreference implements CloudCommitmentPreference {

        /**
         * {@inheritDoc}.
         */
        @Override
        public SortedSet<Long> sortCommitments(@Nonnull final CloudCommitmentCoverageJournal coverageJournal,
                                               @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                                               @Nonnull final Set<Long> commitmentOids) {

            Preconditions.checkNotNull(coverageJournal);
            Preconditions.checkNotNull(commitmentOids);

            final Comparator<Long> commitmentComparator =
                    Comparator.<Long, Double>comparing((commitmentOid) -> coverageJournal.getCommitmentCapacity(commitmentOid, coverageTypeInfo))
                            .thenComparing((commitmentOid) -> coverageJournal.getUnallocatedCapacity(commitmentOid, coverageTypeInfo))
                            .thenComparing(Function.identity());

            return ImmutableSortedSet.orderedBy(commitmentComparator)
                    .addAll(commitmentOids)
                    .build();
        }
    }
}
