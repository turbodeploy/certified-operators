package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableSortedSet;

import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKey;

/**
 * Represents potential coverage assignments between a group of RIs and topology entities. The source
 * of potential coverage assignments are instances of {@link ReservedInstanceCoverageRule}. Note: while
 * the coverage assignments represent valid assignments based on attributes of both the RIs and entities,
 * groupings do not indicate available coverage to either cover the entire group of entities or utilize
 * the entire group of RIs
 */
@Immutable
public class ReservedInstanceCoverageGroup {

    private final String sourceName;

    private final CoverageKey sourceKey;

    private final SortedSet<Long> reservedInstanceOids;

    private final SortedSet<Long> entityOids;

    private ReservedInstanceCoverageGroup(@Nonnull String sourceName,
                                         @Nullable CoverageKey sourceKey,
                                         @Nonnull SortedSet<Long> reservedInstanceOids,
                                         @Nonnull SortedSet<Long> entityOids) {
        this.sourceName = Objects.requireNonNull(sourceName);
        this.sourceKey = sourceKey;
        this.reservedInstanceOids = ImmutableSortedSet.copyOfSorted(
                Objects.requireNonNull(reservedInstanceOids));
        this.entityOids = ImmutableSortedSet.copyOfSorted(
                Objects.requireNonNull(entityOids));
    }

    /**
     * @return The identifier of the source of this group
     */
    @Nonnull
    public String sourceName() {
        return sourceName;
    }

    @Nonnull
    public Optional<CoverageKey> sourceKey() {
        return Optional.ofNullable(sourceKey);
    }

    /**
     * @return A collection of RI OIDs within this group. Ordering of OIDs returned is dependent
     * on the source
     */
    @Nonnull
    public Collection<Long> reservedInstanceOids() {
        return reservedInstanceOids;
    }

    /**
     * @return A collection of entity OIDs within this group. Ordering of OIDs returned is dependent
     * on the source
     */
    @Nonnull
    public Collection<Long> entityOids() {
        return entityOids;
    }

    /**
     * Creates a new instance of {@link ReservedInstanceCoverageGroup}
     * @param sourceName The source identifier
     * @param sourceKey The source {@link CoverageKey} of this group
     * @param reservedInstanceOids A collection of RI OIDs
     * @param entityOids A collection of entity OIDs
     * @return The newly created instance of {@link ReservedInstanceCoverageGroup}
     */
    public static ReservedInstanceCoverageGroup of(@Nonnull String sourceName,
                                                   @Nullable CoverageKey sourceKey,
                                                   @Nonnull SortedSet<Long> reservedInstanceOids,
                                                   @Nonnull SortedSet<Long> entityOids) {
        return new ReservedInstanceCoverageGroup(sourceName, sourceKey, reservedInstanceOids, entityOids);
    }
}
