package com.vmturbo.reserved.instance.coverage.allocator.matcher.entity;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A coverage matcher, creating {@link CoverageKey} instances for entities to be matched against
 * {@link CoverageKey} instances for cloud commitments. This matcher is solely responsible for
 * generating keys for entities.
 */
public interface CoverageEntityMatcher {

    /**
     * Creates the set of {@link CoverageKey} instances, representing potential scopes for matching
     * to cloud commitments.
     * @param entityOid The target entity OID.
     * @return The set of {@link CoverageKey} instances, which can be compared to coverage key instances
     * for cloud commitments in order to determine potential coverage assignments.
     */
    @Nonnull
    Set<CoverageKey> createCoverageKeys(long entityOid);

    /**
     * A factory class for creating {@link CoverageEntityMatcher} instances.
     */
    interface CoverageEntityMatcherFactory {

        /**
         * Creates a new entity matcher, based on the provided coverage topology and matcher configs.
         * @param coverageTopology The {@link CoverageTopology}, used to resolve coverage attributes
         *                         of entities (e.g. stop and tier info).
         * @param matcherConfigs The matcher configs, specifying how provided entities should be matched
         *                       to cloud commitments (e.g. whether the billing family or account should
         *                       be used to match entities to cloud commitments).
         * @return The newly created {@link CoverageEntityMatcher} instance.
         */
        @Nonnull
        CoverageEntityMatcher createEntityMatcher(@Nonnull CoverageTopology coverageTopology,
                                                  @Nonnull Set<EntityMatcherConfig> matcherConfigs);
    }

}
