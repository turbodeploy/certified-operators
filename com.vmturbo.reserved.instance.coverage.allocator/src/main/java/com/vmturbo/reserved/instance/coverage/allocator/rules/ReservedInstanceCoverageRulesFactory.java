package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyCreator.CoverageKeyCreatorFactory;

/**
 * A static factory for creating instances of {@link ReservedInstanceCoverageRule}
 */
public class ReservedInstanceCoverageRulesFactory {

    /**
     * An ordered set of RI coverage rule configurations for AWS. These rules are based on
     * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/apply_ri.html
     */
    public static final List<RICoverageRuleConfig> AWS_RI_COVERAGE_RULE_CONFIGS = ImmutableList.of(
            // First AWS rule is zonal RIs within the same account
            RICoverageRuleConfig.builder()
                    .isSharedScope(false)
                    .isZoneScoped(true)
                    .isPlatformFlexible(false)
                    .isSizeFlexible(false)
                    .build(),
            // Second AWS rule is zonal RIs within the same billing family
            RICoverageRuleConfig.builder()
                    .isSharedScope(true)
                    .isZoneScoped(true)
                    .isPlatformFlexible(false)
                    .isSizeFlexible(false)
                    .build(),
            // Third AWS rule is regional RIs within the same account. We do not set
            // isSizeFlexible, given RIs at this scope can be size flexible or non-size
            // flexible. Therefore, VMs should be able to match to either ISF or non-ISF RIs
            RICoverageRuleConfig.builder()
                    .isSharedScope(false)
                    .isZoneScoped(false)
                    .isPlatformFlexible(false)
                    .build(),
            // Fourth/last AWS rule is regional RIs within the billing family. We do not
            // set isSizeFlexible here for the same reason as above
            RICoverageRuleConfig.builder()
                    .isSharedScope(true)
                    .isZoneScoped(false)
                    .isPlatformFlexible(false)
                    .build());

    /**
     * An ordered set of RI coverage rule configurations for Azure.
     */
    public static final List<RICoverageRuleConfig> AZURE_RI_COVERAGE_RULE_CONFIGS = ImmutableList.of(
            // First Azure rule is non-size flexible RIs in the same account
            RICoverageRuleConfig.builder()
                    .isSharedScope(false)
                    .isZoneScoped(false)
                    .isPlatformFlexible(true)
                    .isSizeFlexible(false)
                    .build(),
            // Second Azure rule is non-size flexible RIs in the billing family (EA family)
            RICoverageRuleConfig.builder()
                    .isSharedScope(true)
                    .isZoneScoped(false)
                    .isPlatformFlexible(true)
                    .isSizeFlexible(false)
                    .build(),
            // Third Azure rule is size flexible RIs in the same account
            RICoverageRuleConfig.builder()
                    .isSharedScope(false)
                    .isZoneScoped(false)
                    .isPlatformFlexible(true)
                    .isSizeFlexible(true)
                    .build(),
            // Last Azure rule is size flexible RIs in the billing family (EA family)
            RICoverageRuleConfig.builder()
                    .isSharedScope(true)
                    .isZoneScoped(false)
                    .isPlatformFlexible(true)
                    .isSizeFlexible(true)
                    .build());

    private ReservedInstanceCoverageRulesFactory() {}

    /**
     * Creates an ordered list of {@link ReservedInstanceCoverageRule} instances, based on the CSP
     * of {@code coverageContext}.
     *
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}. The generated rules
     *                        will be specific to the CSP of the context
     * @param coverageJournal An instance of {@link ReservedInstanceCoverageJournal}
     * @param coverageKeyCreatorFactory An instance of {@link CoverageKeyCreatorFactory}
     * @return An ordered {@link List} of {@link ReservedInstanceCoverageRule} instances
     */
    public static List<ReservedInstanceCoverageRule> createRules(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal,
            @Nonnull CoverageKeyCreatorFactory coverageKeyCreatorFactory) {

        final ConfigurableRICoverageRuleFactory ruleFactory =
                ConfigurableRICoverageRuleFactory.newFactory(
                        coverageContext,
                        coverageJournal,
                        coverageKeyCreatorFactory);

        final ImmutableList.Builder<ReservedInstanceCoverageRule> ruleListBuilder =
                ImmutableList.<ReservedInstanceCoverageRule>builder()
                        .add(FirstPassCoverageRule.newInstance(coverageContext, coverageJournal));

        switch (coverageContext.cloudServiceProvider()) {
            case AWS:
                AWS_RI_COVERAGE_RULE_CONFIGS.stream()
                        .map(ruleFactory::newRule)
                        .forEach(ruleListBuilder::add);
                break;
            case AZURE:
                AZURE_RI_COVERAGE_RULE_CONFIGS.stream()
                        .map((ruleFactory::newRule))
                        .forEach(ruleListBuilder::add);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return ruleListBuilder.build();
    }
}
