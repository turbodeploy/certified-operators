package com.vmturbo.action.orchestrator.workflow.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.workflow.WorkflowDiagnostics;
import com.vmturbo.action.orchestrator.workflow.rpc.DiscoveredWorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.store.InMemoryWorkflowStore;
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
@Import({ActionOrchestratorDBConfig.class, ActionOrchestratorGlobalConfig.class})
public class WorkflowConfig {

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    /**
     * If true then used in-memory workflow cache (synced up with DB workflow store). If false
     * then all discovered workflows are persisted only in DB and workflow fetch requests
     * required DB interaction.
     */
    @Value("${useInMemoryWorkflowCache:true}")
    private boolean useInMemoryWorkflowCache;

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Bean
    public DiscoveredWorkflowRpcService discoveredWorkflowRpcService() {
        return new DiscoveredWorkflowRpcService(workflowStore());
    }

    @Bean
    public WorkflowRpcService workflowRpcService() {
        return new WorkflowRpcService(
            workflowStore(),
            tpConfig.thinTargetCache());
    }

    @Bean
    public WorkflowStore workflowStore() {
        if (useInMemoryWorkflowCache) {
            return new InMemoryWorkflowStore(databaseConfig.dsl(), identityStore(),
                    actionOrchestratorGlobalConfig.actionOrchestratorClock());
        } else {
            return new PersistentWorkflowStore(databaseConfig.dsl(), identityStore(),
                    actionOrchestratorGlobalConfig.actionOrchestratorClock());
        }
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

    /**
     * Bean to handle diagnostics import/export for worfklows.
     *
     * @return the bean created
     */
    @Bean
    public WorkflowDiagnostics workflowDiagnostics() {
        return new WorkflowDiagnostics(workflowStore());
    }

}
