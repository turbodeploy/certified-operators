package com.vmturbo.plan.orchestrator.plan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTOREST.PlanServiceController;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.history.component.api.HistoryComponent;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring configuration for plan instance manipulations.
 */
@Configuration
@Import({SQLDatabaseConfig.class, RepositoryClientConfig.class,
        ActionOrchestratorClientConfig.class, HistoryClientConfig.class,
        RepositoryClientConfig.class, TopologyProcessorClientConfig.class})
public class PlanConfig {

    @Value("${topologyProcessorHost}")
    private String serverAddress;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Bean
    public PlanDao planDao() {
        return new PlanDaoImpl(dbConfig.dsl(), planNotificationSender(),
            repositoryClientConfig.repositoryClient(),
            actionsRpcService(),
            statsRpcService());
    }

    @Bean
    public PlanRpcService planService() {
        return new PlanRpcService(planDao(),
                analysisService(),
                planNotificationSender(),
                startAnalysisThreadPool());
    }

    @Bean
    public AnalysisServiceBlockingStub analysisService() {
        final ManagedChannelBuilder<?> analysisChannel =
                ManagedChannelBuilder.forAddress(serverAddress, grpcPort)
                        .usePlaintext(true);
        final Channel channel = analysisChannel.build();
        return AnalysisServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public PlanServiceController planServiceController() {
        return new PlanServiceController(planService());
    }


    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService actOrchestrThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orch-api-%d")
                        .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ActionOrchestrator actionOrchestrator() {
        final ActionOrchestrator actionOrchestrator = aoClientConfig.actionOrchestratorClient();
        actionOrchestrator.addActionsListener(planProgressListener());
        return actionOrchestrator;
    }

    @Bean
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public PlanProgressListener planProgressListener() {
        return new PlanProgressListener(planDao(), realtimeTopologyContextId);
    }

    @Bean
    public HistoryComponent historyComponent() {
       final HistoryComponent historyComponent = historyClientConfig.historyComponent();
        historyComponent.addStatsListener(planProgressListener());
        return historyComponent;
    }

    /**
     * Stats/history terms used interchangeably.
     */

    @Bean
    public StatsHistoryServiceBlockingStub statsRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public Repository repository() {
        final Repository repositoryClient = repositoryClientConfig.repository();
        repositoryClient.addListener(planProgressListener());
        return repositoryClient;
    }

    @Bean
    public PlanNotificationSender planNotificationSender() {
        return new PlanNotificationSender(planThreadPool());
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration planApiEndpointRegistration() {
        return new ServerEndpointRegistration(PlanOrchestratorClientImpl.WEBSOCKET_PATH,
                planNotificationSender().getWebsocketEndpoint());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService planThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("plan-api-%d")
                .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService startAnalysisThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("plan-analysis-starter-%d")
                .build();
        // Using a single thread executor right now to avoid overwhelming
        // the topology processor, since the construction and broadcast
        // of topologies is expensive and we already have issues with OOM crashes.
        return Executors.newSingleThreadExecutor(threadFactory);
    }

}
