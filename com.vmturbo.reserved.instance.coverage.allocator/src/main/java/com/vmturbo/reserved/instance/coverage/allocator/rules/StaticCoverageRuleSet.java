package com.vmturbo.reserved.instance.coverage.allocator.rules;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CommitmentMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.EntityMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig.TierMatcher;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.ReservedInstanceFilter.ReservedInstanceFilterConfig;

/**
 * Contains static configurations for {@link ConfigurableCoverageRule} instances.
 */
public interface StaticCoverageRuleSet {

    /**
     * A map of the cloud provider to the list of {@link CoverageRuleConfig}, which will be used to
     * create a list of {@link ConfigurableCoverageRule} instances.
     */
    ListMultimap<CloudServiceProvider, CoverageRuleConfig> RULE_SET_BY_CLOUD_PROVIDER =
            ImmutableListMultimap.<CloudServiceProvider, CoverageRuleConfig>builder()
                    .putAll(CloudServiceProvider.AWS,
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local Zonal RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.ACCOUNT)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.AVAILABILITY_ZONE)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(false)
                                            .isSizeFlexible(false)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Shared Zonal RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.BILLING_FAMILY)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.AVAILABILITY_ZONE)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(false)
                                            .isSizeFlexible(false)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local Regional RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.ACCOUNT)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(false)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Shared Regional RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.BILLING_FAMILY)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(false)
                                            .build())
                                    .build())
                    .putAll(CloudServiceProvider.AZURE,
                            // Azure requires a reserved capacity RI to be single scoped
                            // We rely on that requirement in ordering the rules
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local-scope Size-inflexible RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.ACCOUNT)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.ACCOUNT)
                                            .isPlatformFlexible(true)
                                            .isSizeFlexible(false)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local-scoped Size-flexible RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.ACCOUNT)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.ACCOUNT)
                                            .isPlatformFlexible(true)
                                            .isSizeFlexible(true)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Global-scoped Size-flexible RIs within Account")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.ACCOUNT)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(true)
                                            .isSizeFlexible(true)
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Global-scoped Size-flexible RIs within BF")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.BILLING_FAMILY)
                                            .build())
                                    .commitmentSelectionConfig(ReservedInstanceFilterConfig.builder()
                                            .addLocations(CloudCommitmentLocation.REGION)
                                            .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                            .isPlatformFlexible(true)
                                            .isSizeFlexible(true)
                                            .build())
                                    .build())

            .build();

    /**
     * A map of the cloud provider to a set of {@link EntityMatcherConfig} instances. The set of
     * matcher configs represent all potential configurations for the cloud provider in matching
     * a cloud commitment to a coverage entity. The coverage keys for entities will be generated
     * once, at the rule list creation (within {@link CoverageRulesFactory}, while the coverage keys
     * for cloud commitments will be generated within each coverage rule.
     */
    SetMultimap<CloudServiceProvider, EntityMatcherConfig> ENTITY_MATCHER_CONFIGS_BY_CLOUD_PROVIDER =
            ImmutableSetMultimap.<CloudServiceProvider, EntityMatcherConfig>builder()
                    .putAll(CloudServiceProvider.AWS,
                            // Zonal match (match to tier only)
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.ACCOUNT, CloudCommitmentScope.BILLING_FAMILY)
                                    .addLocations(CloudCommitmentLocation.AVAILABILITY_ZONE)
                                    .addTierMatchers(TierMatcher.TIER)
                                    .includePlatform(true)
                                    .includeTenancy(true)
                                    .build(),
                            // Regional match
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.ACCOUNT, CloudCommitmentScope.BILLING_FAMILY)
                                    .addLocations(CloudCommitmentLocation.REGION)
                                    .addTierMatchers(TierMatcher.TIER, TierMatcher.FAMILY)
                                    .includePlatform(true)
                                    .includeTenancy(true)
                                    .build())
                    .putAll(CloudServiceProvider.AZURE,
                            // Match (Account,Tier), (Account, Family)
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.ACCOUNT)
                                    .addLocations(CloudCommitmentLocation.REGION)
                                    .addTierMatchers(TierMatcher.TIER, TierMatcher.FAMILY)
                                    .includePlatform(false)
                                    .includeTenancy(true)
                                    .build(),
                            // Azure can only match to billing family if RI size-flexible
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.BILLING_FAMILY)
                                    .addLocations(CloudCommitmentLocation.REGION)
                                    .addTierMatchers(TierMatcher.FAMILY)
                                    .includePlatform(false)
                                    .includeTenancy(true)
                                    .build())
                    .build();
}
