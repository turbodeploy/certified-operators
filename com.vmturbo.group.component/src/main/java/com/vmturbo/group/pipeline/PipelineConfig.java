package com.vmturbo.group.pipeline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Spring configuration related to group information pipeline.
 */
@Configuration
@Import({ActionOrchestratorClientConfig.class,
        GroupConfig.class,
        RepositoryClientConfig.class,
        RpcConfig.class})
public class PipelineConfig {

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${groupSupplementaryInfoIngestionMaxThreadPoolSize:0}")
    private int groupSupplementaryInfoIngestionMaxThreadPoolSize;

    @Value("${groupSupplementaryInfoIngestionBatchSize:500}")
    private int groupSupplementaryInfoIngestionBatchSize;

    /**
     * Factory for pipelines that update group information.
     *
     * @return the {@link GroupInfoUpdatePipelineFactory}.
     */
    @Bean
    public GroupInfoUpdatePipelineFactory groupInfoUpdatePipelineFactory() {
        return new GroupInfoUpdatePipelineFactory(rpcConfig.cachingMemberCalculator(),
                repositoryClientConfig.searchServiceClient(),
                groupConfig.groupEnvironmentTypeResolver(),
                groupConfig.groupStore(),
                groupConfig.groupSeverityCalculator(),
                rpcConfig.transactionProvider(),
                groupSupplementaryInfoIngestionThreadPool(),
                groupSupplementaryInfoIngestionBatchSize);
    }

    /**
     * Class responsible for running {@link GroupInfoUpdatePipeline}s and group severity updates.
     *
     * @return the {@link GroupInfoUpdater}.
     */
    @Bean
    public GroupInfoUpdater groupInfoUpdater() {
        GroupInfoUpdater groupInfoUpdater =
                new GroupInfoUpdater(groupInfoUpdatePipelineFactory(),
                        groupSeverityUpdater(),
                        realtimeTopologyContextId);
        repositoryClientConfig.repository().addListener(groupInfoUpdater);
        aoClientConfig.entitySeverityClientCache()
                .addEntitySeverityClientCacheUpdateListener(groupInfoUpdater);
        return groupInfoUpdater;
    }

    /**
     * Thread pool for calculation and ingestion of group supplementary info.
     *
     * @return thread pool
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService groupSupplementaryInfoIngestionThreadPool() {
        if (groupSupplementaryInfoIngestionMaxThreadPoolSize <= 0) {
            this.groupSupplementaryInfoIngestionMaxThreadPoolSize =
                    Runtime.getRuntime().availableProcessors();
        }
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("group-supplementary-info-ingestion-%d").build();
        return new ThreadPoolExecutor(0, groupSupplementaryInfoIngestionMaxThreadPoolSize, 10L,
                TimeUnit.SECONDS, new LinkedBlockingQueue(), threadFactory);
    }

    /**
     * Updates severity for groups.
     *
     * @return the {@link GroupSeverityUpdater}.
     */
    @Bean
    public GroupSeverityUpdater groupSeverityUpdater() {
        return new GroupSeverityUpdater(rpcConfig.cachingMemberCalculator(),
                groupConfig.groupSeverityCalculator(), groupConfig.groupStore());
    }
}
