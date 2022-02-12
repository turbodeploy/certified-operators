package com.vmturbo.reserved.instance.coverage.allocator.rules;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;

import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilterCriteria;
import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilterCriteria.ComputeScopeFilterCriteria;
import com.vmturbo.cloud.common.commitment.filter.ReservedInstanceFilterCriteria;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CommitmentMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.EntityMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig.TierMatcher;

/**
 * Contains static configurations for {@link ConfigurableCoverageRule} instances.
 */
public interface StaticCoverageRuleSet {

    /**
     * A map of the cloud provider to the list of {@link CoverageRuleConfig}, which will be used to
     * create a list of {@link ConfigurableCoverageRule} instances.
     */
    ListMultimap<String, CoverageRuleConfig> RULE_SET_BY_CLOUD_PROVIDER =
            ImmutableListMultimap.<String, CoverageRuleConfig>builder()
                    .putAll("aws",
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local Zonal RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.AVAILABILITY_ZONE)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(false)
                                                    .isSizeFlexible(false)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Shared Zonal RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.AVAILABILITY_ZONE)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(false)
                                                    .isSizeFlexible(false)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local Regional RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(false)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Shared Regional RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(false)
                                                    .build())
                                            .build())
                                    .build())
                    .putAll("azure",
                            // Azure requires a reserved capacity RI to be single scoped
                            // We rely on that requirement in ordering the rules
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local-scope Size-inflexible RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(false)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Local-scoped Size-flexible RIs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Global-scoped Size-flexible RIs within Account")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Global-scoped Size-flexible RIs within BF")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .build())
                                    .commitmentSelectionCriteria(ReservedInstanceFilterCriteria.builder()
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
                                            .build())
                                    .build())
                    .putAll("gcp",
                            CoverageRuleConfig.builder()
                                    .ruleTag("Account scoped CUDs")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(CloudCommitmentFilterCriteria.builder()
                                            .type(CloudCommitmentType.TOPOLOGY_COMMITMENT)
                                            .coverageType(CloudCommitmentCoverageType.COMMODITY)
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Billing family scoped CUDs within purchasing project")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                            .build())
                                    .commitmentSelectionCriteria(CloudCommitmentFilterCriteria.builder()
                                            .type(CloudCommitmentType.TOPOLOGY_COMMITMENT)
                                            .coverageType(CloudCommitmentCoverageType.COMMODITY)
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
                                            .build())
                                    .build(),
                            CoverageRuleConfig.builder()
                                    .ruleTag("Billing family scoped CUDs shared")
                                    .commitmentMatcherConfig(CommitmentMatcherConfig.builder()
                                            .scope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .build())
                                    .commitmentSelectionCriteria(CloudCommitmentFilterCriteria.builder()
                                            .type(CloudCommitmentType.TOPOLOGY_COMMITMENT)
                                            .coverageType(CloudCommitmentCoverageType.COMMODITY)
                                            .addLocations(CloudCommitmentLocationType.REGION)
                                            .addEntityScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                            .resourceScopeCriteria(ComputeScopeFilterCriteria.builder()
                                                    .isPlatformFlexible(true)
                                                    .isSizeFlexible(true)
                                                    .build())
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
    SetMultimap<String, EntityMatcherConfig> ENTITY_MATCHER_CONFIGS_BY_CLOUD_PROVIDER =
            ImmutableSetMultimap.<String, EntityMatcherConfig>builder()
                    .putAll("aws",
                            // Zonal match (match to tier only)
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT, CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                    .addLocations(CloudCommitmentLocationType.AVAILABILITY_ZONE)
                                    .addTierMatchers(TierMatcher.TIER)
                                    .includePlatform(true)
                                    .includeTenancy(true)
                                    .build(),
                            // Regional match
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT, CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                    .addLocations(CloudCommitmentLocationType.REGION)
                                    .addTierMatchers(TierMatcher.TIER, TierMatcher.FAMILY)
                                    .includePlatform(true)
                                    .includeTenancy(true)
                                    .build())
                    .putAll("azure",
                            // Match (Account,Tier), (Account, Family)
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                    .addLocations(CloudCommitmentLocationType.REGION)
                                    .addTierMatchers(TierMatcher.TIER, TierMatcher.FAMILY)
                                    .includePlatform(false)
                                    .includeTenancy(true)
                                    .build(),
                            // Azure can only match to billing family if RI is size-flexible
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                    .addLocations(CloudCommitmentLocationType.REGION)
                                    .addTierMatchers(TierMatcher.FAMILY)
                                    .includePlatform(false)
                                    .includeTenancy(true)
                                    .build())
                    .putAll("gcp",
                            VirtualMachineMatcherConfig.builder()
                                    .addScopes(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT, CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                                    .addLocations(CloudCommitmentLocationType.REGION)
                                    .addTierMatchers(TierMatcher.FAMILY)
                                    .includePlatform(false)
                                    .includeTenancy(true)
                                    .build())
                    .build();
}
