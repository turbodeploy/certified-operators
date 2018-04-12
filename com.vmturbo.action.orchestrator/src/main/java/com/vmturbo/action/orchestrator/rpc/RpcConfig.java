package com.vmturbo.action.orchestrator.rpc;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.DefaultActionPaginatorFactory;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionsServiceController;
import com.vmturbo.common.protobuf.action.ActionsDebug;
import com.vmturbo.common.protobuf.action.ActionsDebugREST.ActionsDebugServiceController;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOREST.EntitySeverityServiceController;

@Configuration
@Import({ActionStoreConfig.class, ActionExecutionConfig.class})
public class RpcConfig {

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired
    private ActionExecutor actionExecutor;

    @Autowired
    private ActionTranslator actionTranslator;

    @Value("${actionPaginationDefaultLimit}")
    private int actionPaginationDefaultLimit;

    @Value("${actionPaginationMaxLimit}")
    private int actionPaginationMaxLimit;

    @Bean
    public ActionsRpcService actionRpcService() {
        return new ActionsRpcService(
            actionStoreConfig.actionStorehouse(),
            actionExecutor,
            actionTranslator,
            actionPaginatorFactory()
        );
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
        return new EntitySeverityRpcService(actionStoreConfig.actionStorehouse());
    }

    @Bean
    public EntitySeverityServiceController entitySeverityServiceController() {
        return new EntitySeverityServiceController(entitySeverityRpcService());
    }

    @ConditionalOnProperty(value="grpc.debug.services.enabled")
    @Bean
    public ActionsDebugServiceController actionsDebugServiceController() {
        return actionsDebugRpcService()
            .map(ActionsDebugServiceController::new)
            .orElse(null);
    }

    private boolean grpcDebugServicesEnabled() {
        return Optional.ofNullable(Boolean.getBoolean("grpc.debug.services.enabled"))
            .orElse(false);
    }
}
