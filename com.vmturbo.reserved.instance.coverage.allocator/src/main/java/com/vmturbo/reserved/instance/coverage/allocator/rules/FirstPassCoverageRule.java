package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSortedSet;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * The first pass coverage rule fills in any partial coverage assignments present within the initial
 * coverage input. Partial coverages will be prioritized from smallest entity type to largest. Entities
 * with the same type will be prioritized according to the allocated coverage amount (greatest first).
 *
 * <p>
 * For example, take the following allocations of an RI (with a capacity of 16):
 * <ul>
 *     <li> VM_A: 2 allocated (Capacity|4)
 *     <li> VM_B: 1 allocated (Capcity|4)
 *     <li> VM_C: 7 allocated (Capacity|16)
 * </ul>2
 * This filter would return a sorted entity set of VM_A, VM_B, VM_C
 */
public class FirstPassCoverageRule implements ReservedInstanceCoverageRule {

    private static final String RULE_IDENTIFIER = FirstPassCoverageRule.class.getSimpleName();

    private final CoverageTopology coverageTopology;
    private final ReservedInstanceCoverageJournal coverageJournal;

    private final Comparator<Long> reservedInstanceComparator;
    private final Comparator<Long> entityComparator;

    private FirstPassCoverageRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                 @Nonnull ReservedInstanceCoverageJournal coverageJournal) {
        this.coverageTopology = coverageContext.coverageTopology();
        this.coverageJournal = coverageJournal;

        // This comparator sorts the RIs smallest to largest (in regards to capacity and therefore instance
        // type). This mirrors the sorting applied for AWS billing (Azure does not publish their sorting
        // logic). It's further sorts by unallocated capacity (smallest to largest), attempting to fill
        // in those RIs that are close to being fully utilized. The last sort is by OID for determinism
        this.reservedInstanceComparator = Comparator.comparing(coverageJournal::getReservedInstanceCapacity)
                .thenComparing(coverageJournal::getUnallocatedCapacity)
                .thenComparing(Function.identity());
        // Sorts entities by capacity (i.e. instance type) from smallest to largest. Further sorts
        // entities within the same instance type by uncovered capacity (smallest to largest), in
        // order to fill in those entities closest to being fully covered first. The final sort is
        // by entity OID for determinism
        this.entityComparator = Comparator.comparing(coverageJournal::getEntityCapacity)
                .thenComparing(coverageJournal::getUncoveredCapacity)
                .thenComparing(Function.identity());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<ReservedInstanceCoverageGroup> coverageGroups() {
        // Iterate through coverage entries, looking for a coverage entry in which neither the
        // referenced RI nor entity are at capacity. Any entry passing this condition is a candidate to
        // "fill in" the coverage until either the entity or RI are at capacity. We start by iterating
        // over the current coverage entries by <RI OID, Entity OID>
        return coverageJournal.getCoverages().columnMap().entrySet().stream()
                // Filter out RIs already at capacity
                .filter(riEntry -> !coverageJournal.isReservedInstanceAtCapacity(riEntry.getKey()))
                // Sort entries by the RI first
                .sorted((riEntry1, riEntry2) -> reservedInstanceComparator.compare(
                        riEntry1.getKey(),
                        riEntry2.getKey()))
                // Create a group of the RI to any entites its covering, filtering out
                // fully covered entities and sorting them
                .map(riEntry -> ReservedInstanceCoverageGroup.of(
                        "",
                        //No coverage key for this rule
                        null,
                        new TreeSet<>(Collections.singleton(riEntry.getKey())),
                        ImmutableSortedSet
                                .orderedBy(entityComparator)
                                .addAll(riEntry.getValue()
                                        .keySet()
                                        .stream()
                                        // Ignore fully covered entities
                                        .filter(Predicates.not(coverageJournal::isEntityAtCapacity))
                                        .collect(Collectors.toSet()))
                                .build()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createsDisjointGroups() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String ruleIdentifier() {
        return RULE_IDENTIFIER;
    }

    /**
     * Creates a new instance of {@link FirstPassCoverageRule}
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}
     * @param coverageJournal An instance of {@link ReservedInstanceCoverageJournal}
     * @return The newly created instance of {@link FirstPassCoverageRule}
     */
    @Nonnull
    public static ReservedInstanceCoverageRule newInstance(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal) {

        return new FirstPassCoverageRule(coverageContext, coverageJournal);
    }
}
