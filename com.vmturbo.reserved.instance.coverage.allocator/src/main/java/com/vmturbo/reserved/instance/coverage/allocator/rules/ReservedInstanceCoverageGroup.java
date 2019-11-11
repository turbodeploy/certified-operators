package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableSortedSet;

import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
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

    private final CloudServiceProvider cloudServiceProvider;

    private final String sourceName;

    private final CoverageKey sourceKey;

    private final SortedSet<Long> reservedInstanceOids;

    private final SortedSet<Long> entityOids;

    private ReservedInstanceCoverageGroup(
            @Nonnull CloudServiceProvider cloudServiceProvider,
            @Nonnull String sourceName,
            @Nullable CoverageKey sourceKey,
            @Nonnull SortedSet<Long> reservedInstanceOids,
            @Nonnull SortedSet<Long> entityOids) {
        this.cloudServiceProvider = Objects.requireNonNull(cloudServiceProvider);
        this.sourceName = Objects.requireNonNull(sourceName);
        this.sourceKey = sourceKey;
        this.reservedInstanceOids = ImmutableSortedSet.copyOfSorted(
                Objects.requireNonNull(reservedInstanceOids));
        this.entityOids = ImmutableSortedSet.copyOfSorted(
                Objects.requireNonNull(entityOids));
    }

    /**
     * @return The cloud service provider of this group. It can be assumed all entities and RIs
     * contained within the group are from this CSP
     */
    @Nonnull
    public CloudServiceProvider cloudServiceProvider() {
        return cloudServiceProvider;
    }

    /**
     * @return The identifier of the source of this group
     */
    @Nonnull
    public String sourceName() {
        return sourceName;
    }

    /**
     * The {@link CoverageKey} used to match the group of RIs to the group of entities represented
     * in this group
     * @return The {@link CoverageKey} used to create the group. It may be empty if a coverage key was
     * not used (e.g. in the case of the first pass coverage rule, which does not used coverage keys)
     */
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
     * @param cloudServiceProvider The CSP of this group
     * @param sourceName The source identifier
     * @param sourceKey The source {@link CoverageKey} of this group
     * @param reservedInstanceOids A collection of RI OIDs
     * @param entityOids A collection of entity OIDs
     * @return The newly created instance of {@link ReservedInstanceCoverageGroup}
     */
    public static ReservedInstanceCoverageGroup of(@Nonnull CloudServiceProvider cloudServiceProvider,
                                                   @Nonnull String sourceName,
                                                   @Nullable CoverageKey sourceKey,
                                                   @Nonnull SortedSet<Long> reservedInstanceOids,
                                                   @Nonnull SortedSet<Long> entityOids) {
        return new ReservedInstanceCoverageGroup(
                cloudServiceProvider,
                sourceName,
                sourceKey,
                reservedInstanceOids,
                entityOids);
    }
}
