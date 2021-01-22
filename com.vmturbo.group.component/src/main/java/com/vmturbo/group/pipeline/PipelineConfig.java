package com.vmturbo.group.pipeline;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Spring configuration related to group information pipeline.
 */
@Configuration
@Import({GroupConfig.class,
        RepositoryClientConfig.class,
        RpcConfig.class})
public class PipelineConfig {

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
                groupConfig.groupStore());
    }

    /**
     * Class responsible for running {@link GroupInfoUpdatePipeline}s.
     *
     * @return the {@link GroupInfoUpdatePipelineRunner}.
     */
    @Bean
    public GroupInfoUpdatePipelineRunner groupInfoUpdatePipelineRunner() {
        GroupInfoUpdatePipelineRunner pipelineRunner =
                new GroupInfoUpdatePipelineRunner(groupInfoUpdatePipelineFactory(),
                        Executors.newSingleThreadExecutor(),
                        realtimeTopologyContextId);
        repositoryClientConfig.repository().addListener(pipelineRunner);
        return pipelineRunner;
    }
}
