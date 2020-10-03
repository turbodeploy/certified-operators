package com.vmturbo.reserved.instance.coverage.allocator.rules;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CommitmentMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilter.CloudCommitmentFilterConfig;

/**
 * A configuration for a {@link CoverageRule}, specifying which cloud commitments are applicable
 * to the rule and how selected commitments should be matched to coverage entities.
 */
@HiddenImmutableImplementation
@Immutable
public interface CoverageRuleConfig {

    /**
     * A human-readable string representing the rule.
     * @return A human-readable string representing the rule.
     */
    @Nonnull
    String ruleTag();

    /**
     * A configuration for matching cloud commitments to coverage entities.
     * @return A configuration for matching cloud commitments to coverage entities.
     */
    @Nonnull
    CommitmentMatcherConfig commitmentMatcherConfig();

    /**
     * A configuration for filtering cloud commitments. Any commitments passing the filter
     * will be matched to coverage entities through the {@link #commitmentMatcherConfig()}.
     * @return The configuration for filtering cloud commitments.
     */
    @Nonnull
    CloudCommitmentFilterConfig commitmentSelectionConfig();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CoverageRuleConfig} instances.
     */
    class Builder extends ImmutableCoverageRuleConfig.Builder {}
}
