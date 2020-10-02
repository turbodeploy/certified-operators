package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * Represents a single set of criteria for matching a cloud commitment to a topology entity. For example, AWS coverage
 * application rules dictate that zonal RIs within the same account as a coverable entity receive
 * highest priority during RI allocation. Therefore, we create a single instance of
 * {@link CoverageRule} to match zonal RIs to coverable entities within the same account.
 */
public interface CoverageRule {

    /**
     * Creates and returns a stream of {@link CoverageGroup} instances, representing potential
     * coverage assignments.
     * @return A {@link Stream} of {@link CoverageGroup} instances, representing valid
     * coverage assignments between commitments and entities for this rule.
     */
    @Nonnull
    Stream<CoverageGroup> coverageGroups();

    /**
     * Indicates whether {@link #coverageGroups()} can be concurrently processed.
     * @return True, if each group returned from {@link #coverageGroups()} is disjoint from all other
     * groups. Disjoint groups allow for concurrent processing of potential coverage mappings. A rule
     * creates disjoint groups iff an RI or entity is referenced, at most, from one group. This allows
     * coverage changes to be applied to one group without affecting allocations to another group.
     */
    boolean createsDisjointGroups();


    /**
     * A human-readable identifier for this rule.
     * @return A human-readable identifier for this rule.
     */
    String ruleTag();
}
