package com.vmturbo.topology.processor.actions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.ActionExecutionREST.ActionExecutionServiceController;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;

/**
 * Configuration for action execution.
 */
@Configuration
@Import({EntityConfig.class, OperationConfig.class, ControllableConfig.class})
public class ActionsConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private ControllableConfig controllableConfig;

    @Bean
    public ActionExecutionRpcService actionExecutionService() {
        return new ActionExecutionRpcService(entityConfig.entityStore(),
                operationConfig.operationManager(),
                controllableConfig.entityActionDaoImp());
    }

    @Bean
    public ActionExecutionServiceController actionExecutionServiceController() {
        return new ActionExecutionServiceController(actionExecutionService());
    }
}
