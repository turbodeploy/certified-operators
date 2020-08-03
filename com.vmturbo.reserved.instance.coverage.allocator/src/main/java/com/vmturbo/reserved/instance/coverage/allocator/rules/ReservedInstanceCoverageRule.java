package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * Represents a single set of criteria for matching an RI to a topology entity. For example, AWS coverage
 * application rules dictate that zonal RIs within the same account as a coverable entity receive
 * highest priority during RI allocation. Therefore, we create a single instance of
 * {@link ReservedInstanceCoverageRule} to match zonal RIs to coverable entities within the same
 * account.
 */
public interface ReservedInstanceCoverageRule {

    /**
     * @return A {@link Stream} of {@link ReservedInstanceCoverageGroup} instances, representing valid
     * coverage assignments between RIs and entities for this rule.
     */
    @Nonnull
    Stream<ReservedInstanceCoverageGroup> coverageGroups();

    /**
     * @return True, if each group returned from {@link #coverageGroups()} is disjoint from all other
     * groups. Disjoint groups allow for concurrent processing of potential coverage mappings. A rule
     * creates disjoint groups iff an RI or entity is referenced, at most, from one group. This allows
     * coverage changes to be applied to one group without affecting allocations to another group.
     */
    boolean createsDisjointGroups();


    /**
     * @return A human-readable identifier for this rule
     */
    String ruleIdentifier();
}
