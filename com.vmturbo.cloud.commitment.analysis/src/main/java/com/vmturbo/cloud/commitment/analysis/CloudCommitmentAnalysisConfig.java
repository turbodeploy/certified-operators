package com.vmturbo.cloud.commitment.analysis;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReaderImpl;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.IdentityProvider;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.AnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.DefaultAnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStaticAnalysisConfig;
import com.vmturbo.cloud.commitment.analysis.runtime.StaticAnalysisConfig;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.DemandSelectionStage.DemandSelectionFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.commitment.analysis.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalEntityCloudTopology.DefaultMinimalEntityCloudTopologyFactory;
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
@Configuration
public class CloudCommitmentAnalysisConfig {

    @Autowired
    private ComputeTierAllocationStore computeTierAllocationStore;

    @Autowired
    private RepositoryServiceBlockingStub repositoryServiceBlockingStub;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private IdentityProvider identityProvider;

    @Autowired
    private TopologyEntityCloudTopologyFactory topologyEntityCloudTopologyFactory;

    @Value("${ccaAnalysisStatusLogInterval:PT1M}")
    private String analysisStatusLogInterval;

    @Value("${maxConcurrentCloudCommitmentAnalyses:3}")
    private int maxConcurrentAnalyses;

    @Value("${analysisSegmentInterval:1}")
    private long analysisSegmentInterval;

    @Value("${analysisSegmentUnit:HOURS}")
    private String analysisSegmentUnit;

    @Bean
    public CloudCommitmentDemandReader cloudCommitmentDemandReader() {
        return new CloudCommitmentDemandReaderImpl(computeTierAllocationStore);
    }

    @Bean
    public InitializationStageFactory initializationStageFactory() {
        return new InitializationStageFactory();
    }

    @Bean
    public DemandSelectionFactory demandSelectionFactory() {
        return new DemandSelectionFactory(cloudCommitmentDemandReader());
    }

    @Bean
    public AnalysisPipelineFactory analysisPipelineFactory() {
        return new AnalysisPipelineFactory(identityProvider,
                initializationStageFactory(),
                demandSelectionFactory());
    }

    @Bean
    public BillingFamilyRetrieverFactory billingFamilyRetrieverFactory() {
        return new DefaultBillingFamilyRetrieverFactory(
                new GroupMemberRetriever(
                        GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())));
    }

    @Bean
    public MinimalCloudTopologyFactory minimalCloudTopologyFactory() {
        return new DefaultMinimalEntityCloudTopologyFactory(billingFamilyRetrieverFactory());
    }

    @Bean
    public AnalysisContextFactory analysisContextFactory() {
        return new DefaultAnalysisContextFactory(
                repositoryServiceBlockingStub,
                minimalCloudTopologyFactory(),
                topologyEntityCloudTopologyFactory,
                staticAnalysisConfig());
    }

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
        return new ThreadFactoryBuilder().
                setNameFormat("cloud-commitment-analysis-%d")
                .build();
    }

    @Bean
    public CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager() {
        return new CloudCommitmentAnalysisManager(
                analysisFactory(),
                taskScheduler(),
                Duration.parse(analysisStatusLogInterval),
                maxConcurrentAnalyses);
    }

    @Bean
    public StaticAnalysisConfig staticAnalysisConfig() {
        return ImmutableStaticAnalysisConfig.builder()
                .analysisSegmentInterval(analysisSegmentInterval)
                .analysisSegmentUnit(ChronoUnit.valueOf(analysisSegmentUnit))
                .build();
    }
}
