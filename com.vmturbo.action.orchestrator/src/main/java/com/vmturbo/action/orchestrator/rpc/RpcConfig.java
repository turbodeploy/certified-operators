package com.vmturbo.action.orchestrator.rpc;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.DefaultActionPaginatorFactory;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.approval.ExternalActionApprovalManager;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.common.protobuf.action.ActionConstraintDTOREST.ActionConstraintsServiceController;
import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionsServiceController;
import com.vmturbo.common.protobuf.action.ActionsDebugREST.ActionsDebugServiceController;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOREST.EntitySeverityServiceController;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

@Configuration
@Import({ActionOrchestratorGlobalConfig.class,
    ActionStoreConfig.class,
    ActionExecutionConfig.class,
    ActionStatsConfig.class,
    UserSessionConfig.class,
    TopologyProcessorClientConfig.class,
    TopologyProcessorConfig.class})
public class RpcConfig {

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired
    private ActionExecutor actionExecutor;

    @Autowired
    private ActionTranslator actionTranslator;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private ActionStatsConfig actionStatsConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    @Value("${actionPaginationDefaultLimit}")
    private int actionPaginationDefaultLimit;

    @Value("${actionPaginationMaxLimit}")
    private int actionPaginationMaxLimit;

    @Value("${maxAmountOfEntitiesPerGrpcMessage:5000}")
    private int maxAmountOfEntitiesPerGrpcMessage;

    @Bean
    public ActionsRpcService actionRpcService() {
        return new ActionsRpcService(
            actionOrchestratorGlobalConfig.actionOrchestratorClock(),
            actionStoreConfig.actionStorehouse(),
            actionApprovalManager(),
            actionTranslator,
            actionPaginatorFactory(),
            actionStatsConfig.historicalActionStatReader(),
            actionStatsConfig.currentActionStatReader(),
            userSessionConfig.userSessionContext(),
            actionStoreConfig.acceptedActionsStore());
    }

    /**
     * Action approval manager - used to approve and execute actions requiring approval.
     *
     * @return the bean created
     */
    @Bean
    public ActionApprovalManager actionApprovalManager() {
        return new ActionApprovalManager(actionExecutor,
                actionExecutionConfig.actionTargetSelector(),
                actionStoreConfig.entitySettingsCache(), actionTranslator,
                workflowConfig.workflowStore(), actionStoreConfig.acceptedActionsStore());
    }

    /**
     * External approval manager bean.
     *
     * @return the bean created
     */
    @Bean
    public ExternalActionApprovalManager externalActionApprovalManager() {
        return new ExternalActionApprovalManager(actionApprovalManager(),
                actionStoreConfig.actionStorehouse(),
                topologyProcessorClientConfig.createActionStateReceiver(),
                topologyProcessorConfig.realtimeTopologyContextId());
    }

    @Bean
    public ActionPaginatorFactory actionPaginatorFactory() {
        return new DefaultActionPaginatorFactory(
                actionPaginationDefaultLimit,
                actionPaginationMaxLimit);
    }

    @Bean
    public Optional<ActionsDebugRpcService> actionsDebugRpcService() {
        // The ActionsDebugRpcService should only be instantiated if the system property
        // for the debug service has been set to true at startup time.
        return grpcDebugServicesEnabled() ?
            Optional.of(new ActionsDebugRpcService(actionStoreConfig.actionStorehouse())) :
            Optional.empty();
    }

    @Bean
    public ActionsServiceController actionsServiceController() {
        return new ActionsServiceController(actionRpcService());
    }

    @Bean
    public EntitySeverityRpcService entitySeverityRpcService() {
        return new EntitySeverityRpcService(actionStoreConfig.actionStorehouse(),
                actionPaginationDefaultLimit, actionPaginationMaxLimit, maxAmountOfEntitiesPerGrpcMessage);
    }

    @Bean
    public ActionConstraintsRpcService actionConstraintsRpcService() {
        return new ActionConstraintsRpcService(actionExecutionConfig.actionConstraintStoreFactory());
    }

    @Bean
    public ActionConstraintsServiceController actionConstraintsServiceController() {
        return new ActionConstraintsServiceController(actionConstraintsRpcService());
    }

    @Bean
    public EntitySeverityServiceController entitySeverityServiceController() {
        return new EntitySeverityServiceController(entitySeverityRpcService());
    }

    @Bean
    public ActionsDebugServiceController actionsDebugServiceController() {
        return actionsDebugRpcService()
            .map(ActionsDebugServiceController::new)
            .orElse(null);
    }

    private boolean grpcDebugServicesEnabled() {
        // the default is 'false' if this environment variable is not defined
        return Boolean.getBoolean("grpc.debug.services.enabled");
    }
}
