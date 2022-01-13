package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Comparator;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;

/**
 * An interface to preference coverage entities within a coverage group.
 */
public interface CoverageEntityPreference {

    /**
     * The default preferencing of coverage entities. Preferences smaller VMs over larger VMs and
     * between identically sized VMs will preference entities closer to fully covered.
     */
    CoverageEntityPreference DEFAULT_PREFERENCE = new DefaultCoverageEntityPreference();

    /**
     * Sorts the provided {@code entityOids}, based on internal preferencing logic and the
     * provided {@code coverageJournal}.
     * @param coverageJournal The coverage journal, useful to check both capacity and available
     *                        coverage.
     * @param coverageTypeInfo The coverage type info.
     * @param entityOids The entity OIDs to sort.
     * @return The sorted set of entity OIDs.
     */
    @Nonnull
    Iterable<Long> sortEntities(@Nonnull CloudCommitmentCoverageJournal coverageJournal,
                                @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                                @Nonnull Set<Long> entityOids);

    /**
     * The default implementation of {@link CoverageEntityPreference}.
     */
    class DefaultCoverageEntityPreference implements CoverageEntityPreference {

        /**
         * {@inheritDoc}.
         */
        @Override
        public Iterable<Long> sortEntities(@Nonnull final CloudCommitmentCoverageJournal coverageJournal,
                                           @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                                           @Nonnull final Set<Long> entityOids) {

            Preconditions.checkNotNull(coverageJournal);
            Preconditions.checkNotNull(entityOids);

            // Sorts entities by smallest to largest instance type (based on coverage capacity). Similar to
            // the RI comparator, this mirrors AWS coverage application (Azure behavior is unknown and therefore
            // we treat it the same as AWS). It further sorts by uncovered capacity (smallest to largest),
            // attempting to fully cover partially covered entities rather than more evenly distribute
            // coverage across a topology. The final sort is by OID for determinism
            final Comparator<Long> entityComparator =
                    Comparator.<Long, Double>comparing((entityOid) -> coverageJournal.getEntityCapacity(entityOid, coverageTypeInfo))
                            .thenComparing((entityOid) -> coverageJournal.getUnallocatedCapacity(entityOid, coverageTypeInfo))
                            .thenComparing(Function.identity());

            return ImmutableSortedSet.orderedBy(entityComparator)
                    .addAll(entityOids)
                    .build();
        }
    }
}
