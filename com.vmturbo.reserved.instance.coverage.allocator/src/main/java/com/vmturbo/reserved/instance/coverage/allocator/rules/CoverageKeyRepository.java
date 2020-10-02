package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.SetMultimap;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;

/**
 * A repository for storing mappings between entities/cloud commitments and {@link CoverageKey} instances.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash = true)
public interface CoverageKeyRepository {

    /**
     * A mapping between {@link CoverageKey} and all matching cloud commitment OIDs.
     * @return A set multimap mapping {@link CoverageKey} to all matching commitment OIDs.
     */
    @Nonnull
    SetMultimap<CoverageKey, Long> commitmentsByKey();

    /**
     * A mapping between {@link CoverageKey} and all matching entity OIDs.
     * @return A set multimap mapping {@link CoverageKey} to all matching entity OIDs.
     */
    @Nonnull
    SetMultimap<CoverageKey, Long> entitiesByKey();

    /**
     * Gets the set of commitment OIDs matching the provided {@code key}.
     * @param key The target {@link CoverageKey}.
     * @return The set of matching commitment OIDs. An empty set will be returned if no commitments
     * match.
     */
    @Nonnull
    default Set<Long> getCommitmentsForKey(@Nonnull CoverageKey key) {
        Preconditions.checkNotNull(key);

        return commitmentsByKey().get(key);
    }

    /**
     * Gets the set of entity OIDs matching the provided {@code key}.
     * @param key The target {@link CoverageKey}.
     * @return The set of matching entity OIDs. An empty set will be returned if no entities
     * match.
     */
    @Nonnull
    default Set<Long> getEntitiesForKey(@Nonnull CoverageKey key) {
        Preconditions.checkNotNull(key);

        return entitiesByKey().get(key);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CoverageKeyRepository} instances.
     */
    class Builder extends ImmutableCoverageKeyRepository.Builder {}
}
