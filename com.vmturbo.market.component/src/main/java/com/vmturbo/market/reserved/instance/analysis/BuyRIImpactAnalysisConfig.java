package com.vmturbo.market.reserved.instance.analysis;

import javax.annotation.PostConstruct;

import com.google.common.base.MoreObjects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory.DefaultBuyRIImpactAnalysisFactory;
import com.vmturbo.market.topology.TopologyProcessorConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory.DefaultCoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher.CoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.DefaultCoverageEntityMatcher.DefaultCoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ConfigurableCoverageRule.ConfigurableCoverageRuleFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilterFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A spring configuration for {@link BuyRIImpactAnalysis}.
 */
@Configuration
@Import({
        TopologyProcessorConfig.class,
        GroupClientConfig.class
})
public class BuyRIImpactAnalysisConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${buyRIImpactAnalysisValidation:false}")
    private boolean buyRIImpactAnalysisValidation;

    @Value("${concurrentBuyRIImpactAnalysis:true}")
    private boolean concurrentBuyRIImpactAnalysis;

    @Value("${identityGeneratorPrefix:2}")
    private long identityGeneratorPrefix;

    /**
     * Gets the {@link CoverageTopologyFactory}.
     * @return The {@link CoverageTopologyFactory}.
     */
    @Bean
    public CoverageTopologyFactory coverageTopologyFactory() {
        return new CoverageTopologyFactory(topologyProcessorConfig.thinTargetCache());
    }

    /**
     * Gets the {@link CloudCommitmentFilterFactory}.
     * @return The {@link CloudCommitmentFilterFactory}.
     */
    @Bean
    public CloudCommitmentFilterFactory cloudCommitmentFilterFactory() {
        return new CloudCommitmentFilterFactory();
    }

    /**
     * Gets the {@link ComputeCommitmentMatcherFactory}.
     * @return The {@link ComputeCommitmentMatcherFactory}.
     */
    @Bean
    public ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory() {
        return new ComputeCommitmentMatcherFactory();
    }

    /**
     * Gets the {@link ConfigurableCoverageRuleFactory}.
     * @return The {@link ConfigurableCoverageRuleFactory}.
     */
    @Bean
    public ConfigurableCoverageRuleFactory configurableCoverageRuleFactory() {
        return new ConfigurableCoverageRuleFactory(
                cloudCommitmentFilterFactory(),
                computeCommitmentMatcherFactory());
    }

    /**
     * Gets the {@link CoverageEntityMatcherFactory}.
     * @return The {@link CoverageEntityMatcherFactory}.
     */
    @Bean
    public CoverageEntityMatcherFactory coverageEntityMatcherFactory() {
        return new DefaultCoverageEntityMatcherFactory();
    }

    /**
     * Gets the {@link CoverageRulesFactory}.
     * @return The {@link CoverageRulesFactory}.
     */
    @Bean
    public CoverageRulesFactory coverageRulesFactory() {
        return new CoverageRulesFactory(
                configurableCoverageRuleFactory(),
                coverageEntityMatcherFactory());
    }

    /**
     * Gets the {@link CoverageAllocatorFactory}.
     * @return The {@link CoverageAllocatorFactory}.
     */
    @Bean
    public CoverageAllocatorFactory coverageAllocatorFactory() {
        return new DefaultCoverageAllocatorFactory(coverageRulesFactory());
    }

    /**
     * The {@link ComputeTierFamilyResolverFactory}.
     * @return The {@link ComputeTierFamilyResolverFactory}.
     */
    @Bean
    public ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory() {
        return new ComputeTierFamilyResolverFactory();
    }

    /**
     * bean for the billing family retriever.
     *
     * @return An instance of the billing family retriever.
     */
    @Bean
    public BillingFamilyRetrieverFactory billingFamilyRetrieverFactory() {
        return new DefaultBillingFamilyRetrieverFactory(
                new GroupMemberRetriever(
                        GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())));
    }

    /**
     * Gets the {@link IdentityProvider}.
     * @return The {@link IdentityProvider}.
     */
    @Bean
    public IdentityProvider identityProvider() {
        return new DefaultIdentityProvider(identityGeneratorPrefix);
    }

    /**
     * Gets the {@link CloudCommitmentAggregatorFactory}.
     * @return The {@link CloudCommitmentAggregatorFactory}.
     */
    @Bean
    public CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory() {
        return new DefaultCloudCommitmentAggregatorFactory(
                identityProvider(),
                computeTierFamilyResolverFactory(),
                billingFamilyRetrieverFactory());
    }

    /**
     * Gets the {@link BuyRIImpactAnalysisFactory}.
     * @return The {@link BuyRIImpactAnalysisFactory}.
     */
    @Bean
    public BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory() {
        return new DefaultBuyRIImpactAnalysisFactory(
                coverageAllocatorFactory(),
                coverageTopologyFactory(),
                cloudCommitmentAggregatorFactory(),
                concurrentBuyRIImpactAnalysis,
                buyRIImpactAnalysisValidation);
    }

    /**
     * Logs the configuration settings.
     */
    @PostConstruct
    public void logConfiguration() {
        logger.info(MoreObjects.toStringHelper(this)
                .add("buyRIImpactAnalysisValidation", buyRIImpactAnalysisValidation)
                .add("concurrentBuyRIImpactAnalysis", concurrentBuyRIImpactAnalysis)
                .toString());
    }
}
