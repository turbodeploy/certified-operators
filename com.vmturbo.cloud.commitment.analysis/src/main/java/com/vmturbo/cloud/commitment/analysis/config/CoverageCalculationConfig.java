package com.vmturbo.cloud.commitment.analysis.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AggregateDemandPreference.AggregateDemandPreferenceFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AnalysisCoverageTopology.AnalysisCoverageTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationStage.CoverageCalculationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationSummary.CoverageCalculationSummaryFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationTaskFactory;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory.DefaultCoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher.CoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.DefaultCoverageEntityMatcher.DefaultCoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ConfigurableCoverageRule.ConfigurableCoverageRuleFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilterFactory;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * A configuration for creating a {@link CoverageCalculationFactory} instance, including setting up
 * the {@link CoverageAllocatorFactory}.
 */
@Lazy
@Import({
        SharedFactoriesConfig.class
})
public class CoverageCalculationConfig {

    @Autowired
    private IdentityProvider identityProvider;

    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    @Autowired
    private BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

    @Autowired
    private ThinTargetCache thinTargetCache;

    @Value("${cca.coverageCalculationSummaryInterval:PT10S}")
    private String coverageCalculationSummaryInterval;

    /**
     * Creates a new {@link CloudCommitmentAggregatorFactory} bean.
     * @return The {@link CloudCommitmentAggregatorFactory} bean.
     */
    @Bean
    public CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory() {
        return new DefaultCloudCommitmentAggregatorFactory(
                identityProvider,
                computeTierFamilyResolverFactory,
                billingFamilyRetrieverFactory);
    }

    /**
     * Creates a new {@link AnalysisCoverageTopologyFactory} bean.
     * @return The {@link AnalysisCoverageTopologyFactory} bean.
     */
    @Bean
    public AnalysisCoverageTopologyFactory analysisCoverageTopologyFactory() {
        return new AnalysisCoverageTopologyFactory(
                identityProvider,
                thinTargetCache);
    }

    /**
     * Creates a new {@link CloudCommitmentFilterFactory} bean.
     * @return The {@link CloudCommitmentFilterFactory} bean.
     */
    @Bean
    public CloudCommitmentFilterFactory cloudCommitmentFilterFactory() {
        return new CloudCommitmentFilterFactory();
    }

    /**
     * Creates a new {@link ComputeCommitmentMatcherFactory} bean.
     * @return The {@link ComputeCommitmentMatcherFactory} bean.
     */
    @Bean
    public ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory() {
        return new ComputeCommitmentMatcherFactory();
    }

    /**
     * Creates a new {@link ConfigurableCoverageRuleFactory} bean.
     * @return The {@link ConfigurableCoverageRuleFactory} bean.
     */
    @Bean
    public ConfigurableCoverageRuleFactory configurableCoverageRuleFactory() {
        return new ConfigurableCoverageRuleFactory(
                cloudCommitmentFilterFactory(),
                computeCommitmentMatcherFactory());
    }

    /**
     * Creates a new {@link CoverageEntityMatcherFactory} bean.
     * @return The {@link CoverageEntityMatcherFactory} bean.
     */
    @Bean
    public CoverageEntityMatcherFactory coverageEntityMatcherFactory() {
        return new DefaultCoverageEntityMatcherFactory();
    }

    /**
     * Creates a new {@link CoverageRulesFactory} bean.
     * @return The {@link CoverageRulesFactory} bean.
     */
    @Bean
    public CoverageRulesFactory coverageRulesFactory() {
        return new CoverageRulesFactory(
                configurableCoverageRuleFactory(),
                coverageEntityMatcherFactory());
    }

    /**
     * Creates a new {@link CoverageAllocatorFactory} bean.
     * @return The {@link CoverageAllocatorFactory} bean.
     */
    @Bean
    public CoverageAllocatorFactory coverageAllocatorFactory() {
        return new DefaultCoverageAllocatorFactory(coverageRulesFactory());
    }

    /**
     * Creates a new {@link AggregateDemandPreferenceFactory} bean.
     * @return The {@link AggregateDemandPreferenceFactory} bean.
     */
    @Bean
    public AggregateDemandPreferenceFactory aggregateDemandPreferenceFactory() {
        return new AggregateDemandPreferenceFactory();
    }

    /**
     * Creates a new {@link CoverageCalculationTaskFactory} bean.
     * @return The {@link CoverageCalculationTaskFactory} bean.
     */
    @Bean
    public CoverageCalculationTaskFactory coverageCalculationTaskFactory() {
        return new CoverageCalculationTaskFactory(
                cloudCommitmentAggregatorFactory(),
                analysisCoverageTopologyFactory(),
                coverageAllocatorFactory(),
                aggregateDemandPreferenceFactory());
    }

    /**
     * Creates a new {@link CoverageCalculationSummaryFactory} bean.
     * @return The {@link CoverageCalculationSummaryFactory} bean.
     */
    @Bean
    public CoverageCalculationSummaryFactory coverageCalculationSummaryFactory() {
        return new CoverageCalculationSummaryFactory();
    }

    /**
     * Creates a new {@link CoverageCalculationFactory} bean.
     * @return The {@link CoverageCalculationFactory} bean.
     */
    @Bean
    public CoverageCalculationFactory coverageCalculationFactory() {
        return new CoverageCalculationFactory(
                coverageCalculationTaskFactory(),
                coverageCalculationSummaryFactory(),
                Duration.parse(coverageCalculationSummaryInterval));
    }
}
