package com.vmturbo.action.orchestrator.workflow.config;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.workflow.rpc.DiscoveredWorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowIdentityStore;
import com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOREST;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.PersistentIdentityStore;
/**
 * Spring configuration for Workflow processing - rpc, store.
 **/
@Configuration
@Import(ActionOrchestratorDBConfig.class)
public class WorkflowConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Bean
    public DiscoveredWorkflowRpcService discoveredWorkflowRpcService() {
        return new DiscoveredWorkflowRpcService(workflowStore());
    }

    @Bean
    public WorkflowRpcService fetchWorkflowRpcService() {
        return new WorkflowRpcService(workflowStore());
    }

    @Bean
    public WorkflowStore workflowStore() {
        return new PersistentWorkflowStore(databaseConfig.dsl(), identityStore(), clock());
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public CachingIdentityStore identityStore() {
        return new CachingIdentityStore(workflowAttributeExtractor(),
                workflowPersistentIdentityStore(), identityInitializer());
    }

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    @Bean
    public WorkflowAttributeExtractor workflowAttributeExtractor() {
        return new WorkflowAttributeExtractor();
    }

    @Bean
    public PersistentIdentityStore workflowPersistentIdentityStore() {
        return new PersistentWorkflowIdentityStore(databaseConfig.dsl());
    }

    @Bean
    public WorkflowDTOREST.DiscoveredWorkflowServiceController discoveredWorkflowRpcServiceController() {
        return new WorkflowDTOREST.DiscoveredWorkflowServiceController(discoveredWorkflowRpcService());
    }

}
