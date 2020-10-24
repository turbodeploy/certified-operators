package com.vmturbo.cloud.commitment.analysis;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.cloud.commitment.analysis.config.CloudCommitmentSpecMatcherConfig;
import com.vmturbo.cloud.commitment.analysis.config.CoverageCalculationConfig;
import com.vmturbo.cloud.commitment.analysis.config.DemandClassificationConfig;
import com.vmturbo.cloud.commitment.analysis.config.DemandTransformationConfig;
import com.vmturbo.cloud.commitment.analysis.config.PricingResolverConfig;
import com.vmturbo.cloud.commitment.analysis.config.RecommendationAnalysisConfig;
import com.vmturbo.cloud.commitment.analysis.config.SharedFactoriesConfig;
import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReaderImpl;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.AnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.DefaultAnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStaticAnalysisConfig;
import com.vmturbo.cloud.commitment.analysis.runtime.StaticAnalysisConfig;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.CloudCommitmentInventoryResolverStage.CloudCommitmentInventoryResolverStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.RecommendationSpecMatcherStage.RecommendationSpecMatcherStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationStage.CoverageCalculationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.pricing.PricingResolverStage.PricingResolverStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisStage.RecommendationAnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.DemandRetrievalStage.DemandRetrievalFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.CloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.util.LogContextExecutorService;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.MinimalEntityCloudTopology.DefaultMinimalEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;

/**
 * Configures all classes required to run a cloud commitment analysis. The underlying demand stores
 * are autowired into this configuration and must be defined by the hosting component.
 */
@Lazy
@Import({
        CloudCommitmentSpecMatcherConfig.class,
        DemandClassificationConfig.class,
        DemandTransformationConfig.class,
        CoverageCalculationConfig.class,
        PricingResolverConfig.class,
        RecommendationAnalysisConfig.class,
        SharedFactoriesConfig.class,
})
@Configuration
public class CloudCommitmentAnalysisConfig {

    @Autowired
    private ComputeTierAllocationStore computeTierAllocationStore;

    @Autowired
    private CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver;

    @Autowired
    private RepositoryServiceBlockingStub repositoryServiceBlockingStub;

    @Autowired
    private IdentityProvider identityProvider;

    @Autowired
    private TopologyEntityCloudTopologyFactory topologyEntityCloudTopologyFactory;

    @Autowired
    private CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory;

    @Autowired
    private DemandClassificationFactory demandClassificationFactory;

    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    @Lazy
    @Autowired
    private DemandTransformationFactory demandTransformationFactory;

    @Lazy
    @Autowired
    private BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

    @Lazy
    @Autowired
    private CoverageCalculationFactory coverageCalculationFactory;

    @Lazy
    @Autowired
    private PricingResolverStageFactory pricingResolverStageFactory;

    @Lazy
    @Autowired
    private RecommendationAnalysisFactory recommendationAnalysisFactory;

    @Value("${cca.analysisStatusLogInterval:PT1M}")
    private String analysisStatusLogInterval;

    @Value("${cca.maxConcurrentAnalyses:3}")
    private int maxConcurrentAnalyses;

    @Value("${cca.analysisBucketAmount:1}")
    private long analysisBucketAmount;

    @Value("${cca.analysisBucketUnit:HOURS}")
    private String analysisBucketUnit;

    // A value less than or equal to zero indicates numCores should be used
    @Value("${cca.workerThreadCount:0}")
    private int workerThreadCount;

    /**
     * Bean for the cloud commitment demand reader.
     *
     * @return An instance of the cloud commitment demand reader.
     */
    @Bean
    public CloudCommitmentDemandReader cloudCommitmentDemandReader() {
        return new CloudCommitmentDemandReaderImpl(computeTierAllocationStore);
    }

    /**
     * Bean for the initialization stage factory.
     *
     * @return An instance of the initialization stage factory.
     */
    @Bean
    public InitializationStageFactory initializationStageFactory() {
        return new InitializationStageFactory();
    }

    /**
     * Bean for the demand retrieval factory.
     *
     * @return The demand retrieval factory.
     */
    @Bean
    public DemandRetrievalFactory demandRetrievalFactory() {
        return new DemandRetrievalFactory(cloudCommitmentDemandReader());
    }

    /**
     * Bean for the analysis pipeline factory.
     *
     * @return An instance of the analysis pipleine factory.
     */
    @Bean
    public AnalysisPipelineFactory analysisPipelineFactory() {
        return new AnalysisPipelineFactory(identityProvider,
                initializationStageFactory(),
                demandRetrievalFactory(),
                demandClassificationFactory,
                demandTransformationFactory,
                cloudCommitmentInventoryResolverStageFactory(),
                coverageCalculationFactory,
                ccaRecommendationSpecMatcherStageFactory(),
                pricingResolverStageFactory,
                recommendationAnalysisFactory);
    }

    /**
     * Bean for the minimal cloud topology factory.
     *
     * @return An instance of the MinimalCloudTopologyFactory.
     */
    @Bean
    public MinimalCloudTopologyFactory minimalCloudTopologyFactory() {
        return new DefaultMinimalEntityCloudTopologyFactory(billingFamilyRetrieverFactory);
    }

    @Bean
    protected ThreadFactory workerThreadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("cloud-commitment-analysis-worker-%d")
                .build();
    }

    @Bean
    protected ExecutorService workerExecutorService() {
        int numThreads = workerThreadCount > 0
                ? workerThreadCount
                : Runtime.getRuntime().availableProcessors();
        return LogContextExecutorService.newExecutorService(
                Executors.newFixedThreadPool(numThreads, workerThreadFactory()));
    }

    /**
     * Creates an instance of the CloudCommitmentInventoryResolverStageFactory.
     *
     * @return The CloudCommitmentInventoryResolverStageFactory.
     */
    @Bean
    public CloudCommitmentInventoryResolverStageFactory cloudCommitmentInventoryResolverStageFactory() {
        return new CloudCommitmentInventoryResolverStageFactory(cloudCommitmentBoughtResolver);
    }

    /**
     * Creates an instance of the CCARecommendationSpecMatcherStageFactory.
     *
     * @return The CCA Recommendation spec matcher stage factory.
     */
    @Bean
    public RecommendationSpecMatcherStageFactory ccaRecommendationSpecMatcherStageFactory() {
        return new RecommendationSpecMatcherStageFactory();
    }



    /**
     * Bean for the analysis context factory.
     *
     * @return An instance of the AnalysisContextFactory.
     */
    @Bean
    public AnalysisContextFactory analysisContextFactory() {
        return new DefaultAnalysisContextFactory(
                repositoryServiceBlockingStub,
                minimalCloudTopologyFactory(),
                topologyEntityCloudTopologyFactory,
                cloudCommitmentSpecMatcherFactory,
                computeTierFamilyResolverFactory,
                workerExecutorService(),
                staticAnalysisConfig());
    }

    /**
     * Bean for the analysis factory.
     *
     * @return An instance of the AnalysisFactory.
     */
    @Bean
    public AnalysisFactory analysisFactory() {
        return new AnalysisFactory(
                identityProvider,
                analysisPipelineFactory(),
                analysisContextFactory());
    }

    @Bean
    TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    @Bean
    ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("cloud-commitment-analysis-%d")
                .build();
    }

    /**
     * Bean for the cloud commitment analysis manager.
     *
     * @return An instance of the CloudCommitmentAnalysisManager.
     */
    @Bean
    public CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager() {
        return new CloudCommitmentAnalysisManager(
                analysisFactory(),
                taskScheduler(),
                Duration.parse(analysisStatusLogInterval),
                maxConcurrentAnalyses);
    }

    /**
     * bean for the static analysis config.
     *
     * @return An instance of the StaticAnalysisConfig.
     */
    @Bean
    public StaticAnalysisConfig staticAnalysisConfig() {
        return ImmutableStaticAnalysisConfig.builder()
                .analysisBucket(BoundedDuration.builder()
                        .amount(analysisBucketAmount)
                        .unit(ChronoUnit.valueOf(analysisBucketUnit))
                        .build())
                .build();
    }
}
