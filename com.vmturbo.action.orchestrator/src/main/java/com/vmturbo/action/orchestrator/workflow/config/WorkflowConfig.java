package com.vmturbo.action.orchestrator.workflow.config;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import io.grpc.Channel;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.DbAccessConfig;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.workflow.WorkflowDiagnostics;
import com.vmturbo.action.orchestrator.workflow.rpc.DiscoveredWorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowRpcService;
import com.vmturbo.action.orchestrator.workflow.store.InMemoryWorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowIdentityStore;
import com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc.ApiMessageServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOREST;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring configuration for Workflow processing - rpc, store.
 **/
@Configuration
@Import({DbAccessConfig.class, ActionOrchestratorGlobalConfig.class})
public class WorkflowConfig {

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${apiHost}")
    private String apiHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    /**
     * If true then used in-memory workflow cache (synced up with DB workflow store). If false
     * then all discovered workflows are persisted only in DB and workflow fetch requests
     * required DB interaction.
     */
    @Value("${useInMemoryWorkflowCache:true}")
    private boolean useInMemoryWorkflowCache;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    /**
     * Service responsible for storing discovered workflows.
     *
     * @return instance of {@link DiscoveredWorkflowRpcService}
     */
    @Bean
    public DiscoveredWorkflowRpcService discoveredWorkflowRpcService() {
        return new DiscoveredWorkflowRpcService(workflowStore());
    }

    /**
     * Service responsible for CRUD operations with workflows.
     *
     * @return instance of {@link WorkflowRpcService}
     */
    @Bean
    public WorkflowRpcService workflowRpcService() {
        return new WorkflowRpcService(workflowStore(), tpConfig.thinTargetCache(),
                tpConfig.topologyProcessorChannel(), actionTemplateApplicator());
    }

    /**
     * Responsible for application of a template on an action.
     *
     * @return instance of {@link ActionTemplateApplicator}
     */
    @Bean
    public ActionTemplateApplicator actionTemplateApplicator() {
        return new ActionTemplateApplicator(apiMessageService());
    }

    /**
     * A service responsible for converting to API message.
     *
     * @return api conversion grpc service
     */
    @Bean
    public ApiMessageServiceBlockingStub apiMessageService() {
        return ApiMessageServiceGrpc.newBlockingStub(apiMessageChannel());
    }

    /**
     * The gRPC channel to the API component.
     *
     * @return The gRPC channel
     */
    @Bean
    public Channel apiMessageChannel() {
        return ComponentGrpcServer.newChannelBuilder(apiHost, grpcPort).keepAliveTime(
                grpcPingIntervalSeconds, TimeUnit.SECONDS).build();
    }

    /**
     * Store for discovered and user-created workflows.
     *
     * @return instance of {@link WorkflowStore}.
     */
    @Bean
    public WorkflowStore workflowStore() {
        try {
            if (useInMemoryWorkflowCache) {
                return new InMemoryWorkflowStore(dbAccessConfig.dsl(), identityStore(),
                        actionOrchestratorGlobalConfig.actionOrchestratorClock());
            } else {
                return new PersistentWorkflowStore(dbAccessConfig.dsl(), identityStore(),
                        actionOrchestratorGlobalConfig.actionOrchestratorClock());
            }
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create workflowStore", e);
        }
    }

    /**
     * The identity data store.
     *
     * @return instance of {@link CachingIdentityStore}.
     */
    @Bean
    public CachingIdentityStore identityStore() {
        return new CachingIdentityStore(workflowAttributeExtractor(),
                workflowPersistentIdentityStore(), identityInitializer());
    }

    /**
     * Identity initializer.
     *
     * @return instance of {@link IdentityInitializer}
     */
    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    /**
     * Responsible for extracting specific workflow attributes.
     *
     * @return instance of {@link WorkflowAttributeExtractor}
     */
    @Bean
    public WorkflowAttributeExtractor workflowAttributeExtractor() {
        return new WorkflowAttributeExtractor();
    }

    /**
     * Persistent workflow identity store.
     *
     * @return instance of {@link PersistentWorkflowIdentityStore}
     */
    @Bean
    public PersistentIdentityStore workflowPersistentIdentityStore() {
        try {
            return new PersistentWorkflowIdentityStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create workflowPersistentIdentityStore", e);
        }
    }

    /**
     * Gets the discovered workflow service controller.
     *
     * @return instance of {@link WorkflowDTOREST.DiscoveredWorkflowServiceController}
     */
    @Bean
    public WorkflowDTOREST.DiscoveredWorkflowServiceController discoveredWorkflowRpcServiceController() {
        return new WorkflowDTOREST.DiscoveredWorkflowServiceController(
                discoveredWorkflowRpcService());
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
