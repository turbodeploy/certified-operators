package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandCollector.AggregateDemandCollectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AllocatedTransformationPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationPipeline.DemandTransformationPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.FlexibleRIComputeTransformer.FlexibleRIComputeTransformerFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector.AnalysisSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.CloudTierFilter.CloudTierFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector.RecommendationSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ScopedEntityFilter.ScopedEntityFilterFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;

/**
 * A spring configuration for creating a {@link DemandTransformationFactory} bean.
 */
@Lazy
@Configuration
public class DemandTransformationConfig {

    @Value("${cca.includeStandaloneAccounts:false}")
    private boolean includeStandaloneAccounts;

    /**
     * Resolved from {@link SharedFactoriesConfig}.
     */
    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    /**
     * The {@link FlexibleRIComputeTransformerFactory}.
     * @return The {@link FlexibleRIComputeTransformerFactory}.
     */
    @Bean
    public FlexibleRIComputeTransformerFactory flexibleRIComputeTransformerFactory() {
        return new FlexibleRIComputeTransformerFactory();
    }

    /**
     * The {@link AggregateDemandCollectorFactory}.
     * @return The {@link AggregateDemandCollectorFactory}.
     */
    @Bean
    public AggregateDemandCollectorFactory aggregateDemandCollectorFactory() {
        return new AggregateDemandCollectorFactory();
    }

    /**
     * The {@link DemandTransformationPipelineFactory}.
     * @return The {@link DemandTransformationPipelineFactory}.
     */
    @Bean
    public DemandTransformationPipelineFactory demandTransformationPipelineFactory() {
        return new DemandTransformationPipelineFactory();
    }

    /**
     * The {@link ClassificationFilterFactory}.
     * @return The {@link ClassificationFilterFactory}.
     */
    @Bean
    public ClassificationFilterFactory classificationFilterFactory() {
        return new ClassificationFilterFactory();
    }

    /**
     * The {@link EntityStateFilterFactory}.
     * @return The {@link EntityStateFilterFactory}.
     */
    @Bean
    public EntityStateFilterFactory entityStateFilterFactory() {
        return new EntityStateFilterFactory();
    }

    /**
     * The {@link CloudTierFilterFactory}.
     * @return The {@link CloudTierFilterFactory}.
     */
    @Bean
    public CloudTierFilterFactory cloudTierFilterFactory() {
        return new CloudTierFilterFactory();
    }

    /**
     * The {@link ScopedEntityFilterFactory}.
     * @return The {@link ScopedEntityFilterFactory}.
     */
    @Bean
    public ScopedEntityFilterFactory scopedEntityFilterFactory() {
        return new ScopedEntityFilterFactory();
    }

    /**
     * The {@link AnalysisSelectorFactory}.
     * @return The {@link AnalysisSelectorFactory}.
     */
    @Bean
    public AnalysisSelectorFactory analysisSelectorFactory() {
        return new AnalysisSelectorFactory(
                classificationFilterFactory(),
                entityStateFilterFactory());
    }

    /**
     * The {@link RecommendationSelectorFactory}.
     * @return The {@link RecommendationSelectorFactory}.
     */
    @Bean
    public RecommendationSelectorFactory recommendationSelectorFactory() {
        return new RecommendationSelectorFactory(
                cloudTierFilterFactory(),
                scopedEntityFilterFactory(),
                entityStateFilterFactory(),
                classificationFilterFactory(),
                includeStandaloneAccounts);
    }

    /**
     * The {@link AllocatedTransformationPipelineFactory}.
     * @return The {@link AllocatedTransformationPipelineFactory}.
     */
    @Bean
    public AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory() {
        return new AllocatedTransformationPipelineFactory(
                demandTransformationPipelineFactory(),
                analysisSelectorFactory(),
                flexibleRIComputeTransformerFactory(),
                recommendationSelectorFactory(),
                aggregateDemandCollectorFactory());
    }

    /**
     * The {@link DemandTransformationFactory}.
     * @return The {@link DemandTransformationFactory}.
     */
    @Bean
    public DemandTransformationFactory demandTransformationFactory() {
        return new DemandTransformationFactory(
                allocatedTransformationPipelineFactory());
    }
}
