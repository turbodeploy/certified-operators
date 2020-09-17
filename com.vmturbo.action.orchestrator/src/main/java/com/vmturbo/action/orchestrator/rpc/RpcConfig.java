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
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTOREST.AtomicActionSpecsUploadServiceController;
import com.vmturbo.common.protobuf.action.ActionsDebugREST.ActionsDebugServiceController;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOREST.EntitySeverityServiceController;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring configuration that sets up action related protobuf service implementations as well as
 * their HTTP rest end points.
 */
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

    @Value("${actionPaginationDefaultLimit:100}")
    private int actionPaginationDefaultLimit;

    @Value("${actionPaginationMaxLimit:500}")
    private int actionPaginationMaxLimit;

    @Value("${maxAmountOfEntitiesPerGrpcMessage:5000}")
    private int maxAmountOfEntitiesPerGrpcMessage;

    @Value("${grpc.debug.services.enabled:false}")
    private boolean grpcDebugServicesEnabled;

    /**
     * Returns the the object that implements protobuf ActionsService.
     *
     * @return the the object that implements protobuf ActionsService.
     */
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
            actionStoreConfig.acceptedActionsStore(),
            actionStoreConfig.rejectedActionsStore(),
            actionPaginationMaxLimit);
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
                topologyProcessorClientConfig.createActionApprovalResponseReceiver(),
                topologyProcessorConfig.realtimeTopologyContextId(), actionStoreConfig.rejectedActionsStore());
    }

    /**
     * Returns the paginator using the environment properties actionPaginationDefaultLimit
     * and actionPaginationMaxLimit.
     *
     * @return the default paginator.
     */
    @Bean
    public ActionPaginatorFactory actionPaginatorFactory() {
        return new DefaultActionPaginatorFactory(
                actionPaginationDefaultLimit,
                actionPaginationMaxLimit);
    }

    /**
     * Returns the the object that implements protobuf ActionsDebugService.
     *
     * @return the the object that implements protobuf ActionsDebugService.
     */
    @Bean
    public Optional<ActionsDebugRpcService> actionsDebugRpcService() {
        // The ActionsDebugRpcService should only be instantiated if the system property
        // for the debug service has been set to true at startup time.
        return grpcDebugServicesEnabled
            ? Optional.of(new ActionsDebugRpcService(actionStoreConfig.actionStorehouse()))
            : Optional.empty();
    }

    /**
     * Code generated rest endpoint for /ActionsService that uses ActionsRpcService
     * as the implementation.
     *
     * @return code generated rest endpoint for /ActionsService.
     */
    @Bean
    public ActionsServiceController actionsServiceController() {
        return new ActionsServiceController(actionRpcService());
    }

    /**
     * Returns the the object that implements protobuf EntitySeverityService.
     *
     * @return the the object that implements protobuf EntitySeverityService.
     */
    @Bean
    public EntitySeverityRpcService entitySeverityRpcService() {
        return new EntitySeverityRpcService(actionStoreConfig.actionStorehouse(),
                actionPaginationDefaultLimit, actionPaginationMaxLimit, maxAmountOfEntitiesPerGrpcMessage);
    }

    /**
     * Returns the the object that implements protobuf ActionConstraintsService.
     *
     * @return the the object that implements protobuf ActionConstraintsService.
     */
    @Bean
    public ActionConstraintsRpcService actionConstraintsRpcService() {
        return new ActionConstraintsRpcService(actionExecutionConfig.actionConstraintStoreFactory());
    }

    /**
     * Code generated rest endpoint for /ActionConstraintsService that uses
     * ActionConstraintsRpcService as the implementation.
     *
     * @return code generated rest endpoint for /ActionConstraintsService.
     */
    @Bean //
    public ActionConstraintsServiceController actionConstraintsServiceController() {
        return new ActionConstraintsServiceController(actionConstraintsRpcService());
    }

    /**
     * Create the {@link AtomicActionSpecsRpcService} to receive the {@link AtomicActionSpec}'s.
     *
     * @return the bean created
     */
    @Bean
    public AtomicActionSpecsRpcService atomicActionSpecsRpcService() {
        return new AtomicActionSpecsRpcService(actionStoreConfig.actionMergeSpecsCache());
    }

    /**
     * Code generated rest endpoint for /AtomicActionSpecsUploadService that uses
     * AtomicActionSpecsRpcService as the implementation.
     *
     * @return code generated rest endpoint for /AtomicActionSpecsUploadService.
     */
    @Bean
    public AtomicActionSpecsUploadServiceController actionMergeSpecsServiceController() {
        return new AtomicActionSpecsUploadServiceController(atomicActionSpecsRpcService());
    }

    /**
     * Code generated rest endpoint for /EntitySeverityService that uses EntitySeverityRpcService
     * as the implementation.
     *
     * @return code generated rest endpoint for /EntitySeverityService.
     */
    @Bean
    public EntitySeverityServiceController entitySeverityServiceController() {
        return new EntitySeverityServiceController(entitySeverityRpcService());
    }

    /**
     * Code generated rest endpoint for /ActionsDebugService that uses ActionsDebugRpcService
     * as the implementation.
     *
     * @return code generated rest endpoint for /ActionsDebugService.
     */
    @Bean
    public ActionsDebugServiceController actionsDebugServiceController() {
        return actionsDebugRpcService()
            .map(ActionsDebugServiceController::new)
            .orElse(null);
    }
}
