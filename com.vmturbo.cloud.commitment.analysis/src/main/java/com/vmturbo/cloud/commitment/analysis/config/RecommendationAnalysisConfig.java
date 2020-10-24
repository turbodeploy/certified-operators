package com.vmturbo.cloud.commitment.analysis.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisStage.RecommendationAnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisTask.RecommendationAnalysisTaskFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.SavingsPricingResolver.SavingsPricingResolverFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.CloudCommitmentSavingsCalculatorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.DefaultCloudCommitmentSavingsCalculatorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.ReservedInstanceSavingsCalculator.ReservedInstanceSavingsCalculatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;

/**
 * Spring configuration for creating a {@link RecommendationAnalysisFactory} instance.
 */
@Lazy
@Configuration
public class RecommendationAnalysisConfig {

    @Value("${cca.recommendationTaskSummaryInterval:PT10S}")
    private String recommendationTaskSummaryInterval;

    @Autowired
    private IdentityProvider identityProvider;

    /**
     * Bean to create a {@link ReservedInstanceSavingsCalculatorFactory} instance.
     * @return The {@link ReservedInstanceSavingsCalculatorFactory} instance.
     */
    @Bean
    public ReservedInstanceSavingsCalculatorFactory riSavingsCalculatorFactory() {
        return new ReservedInstanceSavingsCalculatorFactory();
    }

    /**
     * Bean to create a {@link CloudCommitmentSavingsCalculatorFactory} instance.
     * @return The {@link CloudCommitmentSavingsCalculatorFactory} instance.
     */
    @Bean
    public CloudCommitmentSavingsCalculatorFactory cloudCommitmentSavingsCalculatorFactory() {
        return new DefaultCloudCommitmentSavingsCalculatorFactory(riSavingsCalculatorFactory());
    }

    /**
     * Bean to create a {@link SavingsPricingResolverFactory} instance.
     * @return The {@link SavingsPricingResolverFactory} instance.
     */
    @Bean
    public SavingsPricingResolverFactory savingsPricingResolverFactory() {
        return new SavingsPricingResolverFactory();
    }

    /**
     * Bean to create a {@link RecommendationAnalysisTaskFactory} instance.
     * @return The {@link RecommendationAnalysisTaskFactory} instance.
     */
    @Bean
    public RecommendationAnalysisTaskFactory recommendationAnalysisTaskFactory() {
        return new RecommendationAnalysisTaskFactory(
                identityProvider,
                cloudCommitmentSavingsCalculatorFactory(),
                savingsPricingResolverFactory());
    }

    /**
     * Bean to create a {@link RecommendationAnalysisFactory} instance.
     * @return The {@link RecommendationAnalysisFactory} instance.
     */
    @Bean
    public RecommendationAnalysisFactory recommendationAnalysisFactory() {
        return new RecommendationAnalysisFactory(
                recommendationAnalysisTaskFactory(),
                Duration.parse(recommendationTaskSummaryInterval));
    }
}