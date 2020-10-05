package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;

/**
 * Creates {@link CoverageKey} instances for cloud commitments, which can be compared against
 * {@link CoverageKey} instances for entities (generated from {@link com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher})
 * in order to resolve potential coverage assignments.
 */
public interface CommitmentMatcher {

    /**
     * Generates a set of {@link CoverageKey} instances for the provided {@code commitmentAggregate}.
     * @param commitmentAggregate The target commitment aggregate.
     * @return The set of {@link CoverageKey} instances.
     */
    @Nonnull
    Set<CoverageKey> createKeysForCommitment(@Nonnull CloudCommitmentAggregate commitmentAggregate);
}
