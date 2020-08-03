package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

/**
 * The output of {@link ReservedInstanceCoverageAllocator}, containing reserved instance coverage
 * allocations for topology entities, along with the total coverage (source/input coverage to the allocator
 * + allocator coverage)
 */
public class ReservedInstanceCoverageAllocation {

    /**
     * An empty allocation (both total coverage and allocated coverage are empty)
     */
    public static final ReservedInstanceCoverageAllocation EMPTY_ALLOCATION =
            ReservedInstanceCoverageAllocation.from(ImmutableTable.of(), ImmutableTable.of());

    /**
     * Coverage is formatted as {@literal <Entity OID, RI OID, Cover Amount>}
     */
    private final Table<Long, Long, Double> totalCoverage;

    /**
     * Coverage is formatted as {@literal <Entity OID, RI OID, Cover Amount>}
     */
    private final Table<Long, Long, Double> allocatorCoverage;


    private ReservedInstanceCoverageAllocation(@Nonnull Table<Long, Long, Double> totalCoverage,
                                               @Nonnull Table<Long, Long, Double> allocatorCoverage) {

        this.totalCoverage = ImmutableTable.copyOf(Objects.requireNonNull(totalCoverage));
        this.allocatorCoverage = ImmutableTable.copyOf(Objects.requireNonNull(allocatorCoverage));
    }

    /**
     * @return An immutable {@link Table} of the total coverage allocated after execution of
     * {@link ReservedInstanceCoverageAllocator}. The total coverage is a summation of the input
     * coverage to the allocator and any coverage allocated as part of its execution.
     */
    @Nonnull
    public Table<Long, Long, Double> totalCoverageTable() {
        return totalCoverage;
    }

    /**
     * @return An immutable {@link Table} of the coverage added as part of {@link ReservedInstanceCoverageAllocator}
     * analysis. The table is formatted as {@literal <Entity OID, RI OID, Coverage Amount>}
     */
    @Nonnull
    public Table<Long, Long, Double> allocatorCoverageTable() {
        return allocatorCoverage;
    }

    /**
     * Builds an instance of {@link ReservedInstanceCoverageAllocation} from {@code totalCoverage}
     * and {@code allocatorCoverage}. A copy of both coverage tables will be made, such that the
     * {@link ReservedInstanceCoverageAllocation} returned will be immutable and independent of
     * changes to either table.
     *
     * @param totalCoverage A {@link Table} containing {@literal <Entity OID, RI OID, Coverage Allocation Amount>}
     *                      of total coverage after {@link ReservedInstanceCoverageAllocator} analysis
     * @param allocatorCoverage A {@link Table} containing {@literal <Entity OID, RI OID, Coverage Allocation Amount>}
     *                          of coverage added by {@link ReservedInstanceCoverageAllocator}
     * @return A newly created instance of {@link ReservedInstanceCoverageAllocation}
     */
    @Nonnull
    public static ReservedInstanceCoverageAllocation from(
            @Nonnull Table<Long, Long, Double> totalCoverage,
            @Nonnull Table<Long, Long, Double> allocatorCoverage) {
        return new ReservedInstanceCoverageAllocation(totalCoverage, allocatorCoverage);
    }
}
