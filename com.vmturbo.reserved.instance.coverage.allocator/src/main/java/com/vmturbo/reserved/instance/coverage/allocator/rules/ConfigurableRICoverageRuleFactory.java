package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyCreator.CoverageKeyCreatorFactory;

/**
 * A factory class for creating instances of {@link ConfigurableRICoverageRule}
 */
public class ConfigurableRICoverageRuleFactory {

    private final CloudProviderCoverageContext coverageContext;

    private final ReservedInstanceCoverageJournal coverageJournal;

    private final CoverageKeyCreatorFactory coverageKeyCreatorFactory;

    private ConfigurableRICoverageRuleFactory(@Nonnull CloudProviderCoverageContext coverageContext,
                                              @Nonnull ReservedInstanceCoverageJournal coverageJournal,
                                              @Nonnull CoverageKeyCreatorFactory coverageKeyCreatorFactory) {

        this.coverageContext = Objects.requireNonNull(coverageContext);
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
        this.coverageKeyCreatorFactory = Objects.requireNonNull(coverageKeyCreatorFactory);
    }

    /**
     * Creates a new rule factory
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}
     * @param coverageJournal An instance of {@link ReservedInstanceCoverageJournal}
     * @param coverageKeyCreatorFactory An instance of {@link CoverageKeyCreatorFactory}
     * @return The newly created rule factory
     */
    public static ConfigurableRICoverageRuleFactory newFactory(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal,
            @Nonnull CoverageKeyCreatorFactory coverageKeyCreatorFactory) {
        return new ConfigurableRICoverageRuleFactory(
                coverageContext,
                coverageJournal,
                coverageKeyCreatorFactory);
    }

    /**
     * Creates a new rule with {@code ruleConfig}
     * @param ruleConfig The instance of {@link RICoverageRuleConfig} to configure the rule
     * @return A newly created {@link ConfigurableRICoverageRule}
     */
    public ConfigurableRICoverageRule newRule(@Nonnull RICoverageRuleConfig ruleConfig) {
        return ConfigurableRICoverageRule.newInstance(
                coverageContext,
                coverageJournal,
                coverageKeyCreatorFactory,
                ruleConfig);
    }
}
