package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;

/**
 * The first pass coverage rule fills in any partial coverage assignments present within the initial
 * coverage input. Partial coverages will be prioritized from smallest entity type to largest. Entities
 * with the same type will be prioritized according to the allocated coverage amount (greatest first).
 *
 * <p>For example, take the following allocations of an RI (with a capacity of 16):
 * <ul>
 *     <li> VM_A: 2 allocated (Capacity|4)
 *     <li> VM_B: 1 allocated (Capcity|4)
 *     <li> VM_C: 7 allocated (Capacity|16)
 * </ul>2
 * This filter would return a sorted entity set of VM_A, VM_B, VM_C
 */
public class FirstPassCoverageRule implements CoverageRule {

    private static final String RULE_TAG = "First Pass Coverage Rule";

    private final CloudProviderCoverageContext coverageContext;
    private final ReservedInstanceCoverageJournal coverageJournal;

    private FirstPassCoverageRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                 @Nonnull ReservedInstanceCoverageJournal coverageJournal) {
        this.coverageContext = Objects.requireNonNull(coverageContext);
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<CoverageGroup> coverageGroups() {

        final Map<Long, Map<Long, Double>> entityCoverageByRIOid =
                coverageJournal.getCoverages().columnMap();

        // First iterate through RIs within the coverage context, filtering those RIs not at capacity.
        // For each RI, we then create a coverage group of entities which are already covered by it,
        // but are not at capacity (sorting by the entity comparator). The intent is to create groups
        // which contain entities and RIs which are already linked, in which neither is at capacity
        return coverageContext.reservedInstanceOids().stream()
                // Filter out RIs without coverage assignments
                .filter(entityCoverageByRIOid::containsKey)
                // Filter out RIs already at capacity
                .filter(Predicates.not(coverageJournal::isReservedInstanceAtCapacity))
                // Sort RIs by Oid
                .sorted()
                // Create a group of the RI to any entities its covering, filtering out
                // fully covered entities and sorting them
                .map(riOid -> CoverageGroup.builder()
                        .cloudServiceProvider(coverageContext.cloudServiceProvider())
                        .sourceTag(ruleTag())
                        .addCommitmentOids(riOid)
                        .addAllEntityOids(
                                entityCoverageByRIOid.get(riOid)
                                        .keySet()
                                        .stream()
                                        // Ignore fully covered entities
                                        .filter(Predicates.not(coverageJournal::isEntityAtCapacity))
                                        .collect(Collectors.toSet()))
                        .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createsDisjointGroups() {
        return false;
    }

    @Override
    public String ruleTag() {
        return RULE_TAG;
    }


    /**
     * Creates a new instance of {@link FirstPassCoverageRule}.
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}
     * @param coverageJournal An instance of {@link ReservedInstanceCoverageJournal}
     * @return The newly created instance of {@link FirstPassCoverageRule}
     */
    @Nonnull
    public static CoverageRule newInstance(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal) {

        return new FirstPassCoverageRule(coverageContext, coverageJournal);
    }
}
