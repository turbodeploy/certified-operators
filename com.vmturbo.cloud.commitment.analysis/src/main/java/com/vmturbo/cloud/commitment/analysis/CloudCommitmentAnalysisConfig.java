package com.vmturbo.cloud.commitment.analysis;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
import com.vmturbo.cloud.commitment.analysis.config.DemandClassificationConfig;
import com.vmturbo.cloud.commitment.analysis.config.DemandTransformationConfig;
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
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.DemandRetrievalStage.DemandRetrievalFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.CloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.MinimalEntityCloudTopology.DefaultMinimalEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;

/**
 * Configures all classes required to run a cloud commitment analysis. The underlying demand stores
 * are autowired into this configuration and must be defined by the hosting component.
 */
@Lazy
@Import({
        CloudCommitmentSpecMatcherConfig.class,
        DemandClassificationConfig.class,
        DemandTransformationConfig.class,
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
    private GroupClientConfig groupClientConfig;

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

    @Autowired
    private DemandTransformationFactory demandTransformationFactory;

    @Value("${ccaAnalysisStatusLogInterval:PT1M}")
    private String analysisStatusLogInterval;

    @Value("${maxConcurrentCloudCommitmentAnalyses:3}")
    private int maxConcurrentAnalyses;

    @Value("${analysisBucketAmount:1}")
    private long analysisBucketAmount;

    @Value("${analysisBucketUnit:HOURS}")
    private String analysisBucketUnit;

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
                cloudCommitmentInventoryResolverStageFactory());
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
     * Bean for the minimal cloud topology factory.
     *
     * @return An instance of the MinimalCloudTopologyFactory.
     */
    @Bean
    public MinimalCloudTopologyFactory minimalCloudTopologyFactory() {
        return new DefaultMinimalEntityCloudTopologyFactory(billingFamilyRetrieverFactory());
    }

    /**
     * Creates an instance of the CloudCommitmentInventoryResolverStage.
     *
     * @return The CloudCommitmentInventoryResolverStage.
     */
    @Bean
    public CloudCommitmentInventoryResolverStageFactory cloudCommitmentInventoryResolverStageFactory() {
        return new CloudCommitmentInventoryResolverStageFactory(cloudCommitmentBoughtResolver);
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
