package com.vmturbo.reserved.instance.coverage.allocator.key;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Stores mappings of {@link CoverageKey} instances to RI and entity Oids. The mappings are used by
 * consumers to indicate possible coverage assignments between an RI and an entity.
 */
public class CoverageKeyRepository {

    private final Map<CoverageKey, Set<Long>> reservedInstancesByCoverageKey;
    private final Map<CoverageKey, Set<Long>> entitiesByCoverageKey;

    private CoverageKeyRepository(@Nonnull Builder builder) {

        this.reservedInstancesByCoverageKey = builder.reservedInstancesByCoverageKey.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Entry::getKey,
                        e -> ImmutableSet.copyOf(e.getValue())));
        this.entitiesByCoverageKey = builder.entitiesByCoverageKey.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Entry::getKey,
                        e -> ImmutableSet.copyOf(e.getValue())));

    }

    /**
     * @return The mapping of {@link CoverageKey} to a {@link Set} of RI oids
     */
    @Nonnull
    public Map<CoverageKey, Set<Long>> reservedInstancesByCoverageKey() {
        return reservedInstancesByCoverageKey;
    }

    /**
     * @return The mapping of {@link CoverageKey} to a {@link Set} of entity oids
     */
    @Nonnull
    public Map<CoverageKey, Set<Long>> entitiesByCoverageKey() {
        return entitiesByCoverageKey;
    }

    /**
     * @param key An instance of {@link CoverageKey} to lookup. Null values are not supported
     * @return The set of RI oids associated with {@code key} or an empty set if not oids
     * link to {@code key}l
     */
    @Nonnull
    public Set<Long> getReservedInstancesForKey(@Nonnull CoverageKey key) {
        Preconditions.checkNotNull(key);
        return reservedInstancesByCoverageKey.getOrDefault(key, Collections.EMPTY_SET);
    }

    /**
     * @param key An instance of {@link CoverageKey} to lookup. Null values are not supported
     * @return The set of entity oids associated with {@code key} or an empty set if not oids
     * link to {@code key}l
     */
    @Nonnull
    public Set<Long> getEntitiesForKey(@Nonnull CoverageKey key) {
        Preconditions.checkNotNull(key);
        return entitiesByCoverageKey.getOrDefault(key, Collections.EMPTY_SET);
    }

    /**
     * @return A new {@link Builder} instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for {@link CoverageKeyRepository}
     */
    public static class Builder {

        private final Map<CoverageKey, Set<Long>> reservedInstancesByCoverageKey = new HashMap<>();

        private final Map<CoverageKey, Set<Long>> entitiesByCoverageKey = new HashMap<>();

        /**
         * Add an RI oid entry for {@code coverageKey}
         * @param coverageKey An instance of {@link CoverageKey}
         * @param riOid An RI oid
         * @return The instance of {@link CoverageKeyCreationConfig.Builder} for method chaining
         */
        @Nonnull
        public Builder addReservedInstanceByKey(CoverageKey coverageKey, long riOid) {
            reservedInstancesByCoverageKey.computeIfAbsent(coverageKey, (__) -> Sets.newHashSet())
                    .add(riOid);
            return this;
        }

        /**
         * Add an entity oid entry for {@code coverageKey}
         * @param coverageKey An instance of {@link CoverageKey}
         * @param entityOid An entity oid
         * @return The instance of {@link CoverageKeyCreationConfig.Builder} for method chaining
         */
        @Nonnull
        public Builder addEntityByKey(CoverageKey coverageKey, long entityOid) {
            entitiesByCoverageKey.computeIfAbsent(coverageKey, (__) -> Sets.newHashSet())
                    .add(entityOid);
            return this;
        }

        /**
         * @return A newly created instance of {@link CoverageKeyRepository}
         */
        @Nonnull
        public CoverageKeyRepository build() {
            return new CoverageKeyRepository(this);
        }
    }
}
