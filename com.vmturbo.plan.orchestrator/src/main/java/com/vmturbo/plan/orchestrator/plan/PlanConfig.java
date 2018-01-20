package com.vmturbo.plan.orchestrator.plan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTOREST.PlanServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring configuration for plan instance manipulations.
 */
@Configuration
@Import({SQLDatabaseConfig.class, RepositoryClientConfig.class,
        ActionOrchestratorClientConfig.class, HistoryClientConfig.class,
        RepositoryClientConfig.class, TopologyProcessorClientConfig.class,
        BaseKafkaProducerConfig.class, ReservationConfig.class})
public class PlanConfig {

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

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Bean
    public PlanDao planDao() {
        return new PlanDaoImpl(dbConfig.dsl(),
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
        final Channel channel = tpClientConfig.topologyProcessorChannel();
        return AnalysisServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public PlanServiceController planServiceController() {
        return new PlanServiceController(planService());
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
    public RepositoryServiceBlockingStub repositoryServiceBlockingStub() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public PlanProgressListener planProgressListener() {
        final PlanProgressListener listener =  new PlanProgressListener(planDao(),
                reservationConfig.reservationPlacementHandler(), realtimeTopologyContextId);
        repositoryClientConfig.repository().addListener(listener);
        historyClientConfig.historyComponent().addStatsListener(listener);
        return listener;
    }

    /**
     * Stats/history terms used interchangeably.
     */

    @Bean
    public StatsHistoryServiceBlockingStub statsRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public IMessageSender<PlanInstance> notificationSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(PlanOrchestratorClientImpl.STATUS_CHANGED_TOPIC);
    }

    @Bean
    public PlanNotificationSender planNotificationSender() {
        final PlanNotificationSender notificationSender = new PlanNotificationSender(notificationSender());
        planDao().addStatusListener(notificationSender);
        return notificationSender;
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaHealthMonitor() {
        return new KafkaProducerHealthMonitor(kafkaProducerConfig.kafkaMessageSender());
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
