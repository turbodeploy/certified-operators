package com.vmturbo.group.pipeline;

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
                groupConfig.groupSeverityCalculator());
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
