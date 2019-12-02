package com.vmturbo.topology.processor.actions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc.ActionConstraintsServiceStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionREST.ActionExecutionServiceController;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for action execution.
 */
@Configuration
@Import({ControllableConfig.class,
        EntityConfig.class,
        OperationConfig.class,
        RepositoryConfig.class,
        TargetConfig.class,
        ActionOrchestratorClientConfig.class})
public class ActionsConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public ActionDataManager actionDataManager() {
        return new ActionDataManager(
                SearchServiceGrpc.newBlockingStub(repositoryConfig.repositoryChannel()),
                topologyToSdkEntityConverter());
    }

    @Bean
    public TopologyToSdkEntityConverter topologyToSdkEntityConverter() {
        return new TopologyToSdkEntityConverter(entityConfig.entityStore(),
                targetConfig.targetStore(), targetConfig.groupScopeResolver());
    }

    @Bean
    public EntityRetriever entityRetriever() {
        return new EntityRetriever(topologyToSdkEntityConverter(),
                repositoryConfig.repository(),
                realtimeTopologyContextId);
    }

    @Bean
    public ActionExecutionContextFactory actionExecutionContextFactory() {
        return new ActionExecutionContextFactory(actionDataManager(),
                entityConfig.entityStore(),
                entityRetriever(),
                targetConfig.targetStore(),
                probeConfig.probeStore());
    }

    @Bean
    public ActionExecutionRpcService actionExecutionService() {
        return new ActionExecutionRpcService(
                operationConfig.operationManager(),
                actionExecutionContextFactory());
    }

    @Bean
    public ActionExecutionServiceController actionExecutionServiceController() {
        return new ActionExecutionServiceController(actionExecutionService());
    }

    @Bean
    public ActionConstraintsServiceStub actionConstraintsServiceStub() {
        return ActionConstraintsServiceGrpc.newStub(
            aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public ActionConstraintsUploader actionConstraintsUploader() {
        return new ActionConstraintsUploader(entityConfig.entityStore(),
            actionConstraintsServiceStub());
    }
}
