package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;

/**
 * Represents potential coverage assignments between a group of cloud commitments and topology entities. The source
 * of potential coverage assignments are instances of {@link CoverageRule}. Note: while
 * the coverage assignments represent valid assignments based on attributes of both the commitments and entities,
 * groupings do not indicate available coverage to either cover the entire group of entities or utilize
 * the entire group of commitments.
 */
@HiddenImmutableImplementation
@Immutable
public interface CoverageGroup {

    /**
     * The cloud service provider of this group. It can be assumed all entities and commitments
     * contained within the group are from this CSP
     * @return The cloud service provider of this group. It can be assumed all entities and commitments
     * contained within the group are from this CSP.
     */
    @Nonnull
    CloudServiceProvider cloudServiceProvider();

    /**
     * The identifier of the source of this group.
     * @return The identifier of the source of this group.
     */
    @Nonnull
    String sourceTag();

    /**
     * The {@link CoverageKey} used to match the group of commitments to the group of entities represented
     * in this group.
     * @return The {@link CoverageKey} used to create the group. It may be empty if a coverage key was
     * not used (e.g. in the case of the first pass coverage rule, which does not used coverage keys)
     */
    @Nonnull
    Optional<CoverageKey> sourceKey();

    /**
     * A collection of commitment OIDs within this group.
     * @return A collection of commitment OIDs within this group.
     */
    @Nonnull
    Set<Long> commitmentOids();

    /**
     * A collection of entity OIDs within this group.
     * @return A collection of entity OIDs within this group.
     */
    @Nonnull
    Set<Long> entityOids();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CoverageGroup} instances.
     */
    class Builder extends ImmutableCoverageGroup.Builder {}
}
