package com.vmturbo.plan.orchestrator.plan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification;
import com.vmturbo.common.protobuf.plan.PlanDTOREST.PlanServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring configuration for plan instance manipulations.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class, RepositoryClientConfig.class,
        ActionOrchestratorClientConfig.class, HistoryClientConfig.class,
        RepositoryClientConfig.class, TopologyProcessorClientConfig.class,
        BaseKafkaProducerConfig.class, ReservationConfig.class,
        GroupClientConfig.class, CostClientConfig.class, GlobalConfig.class})
public class PlanConfig {

    @Value("${planTimeoutMins:60}")
    private int planTimeoutMins;

    @Value("${planCleanupIntervalMin:5}")
    private int planCleanupIntervalMin;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${startAnalysisRetryTimeoutMin:30}")
    private long startAnalysisRetryTimeoutMin;

    @Value("${numPlanAnalysisThreads:5}")
    private int numPlanAnalysisThreads;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Bean
    public PlanDao planDao() {
        return new PlanDaoImpl(dbConfig.dsl(),
                groupClientConfig.groupChannel(),
                userSessionConfig.userSessionContext(),
                repositoryClientConfig.searchServiceClient(),
                globalConfig.clock(),
                planCleanupExecutor(),
                planTimeoutMins,
                TimeUnit.MINUTES,
                planCleanupIntervalMin,
                TimeUnit.MINUTES);
    }

    /**
     * Scheduled executor used by {@link PlanDao} to asynchronously clean up (i.e. mark as failed)
     * plans with no activity for a certain time period.
     *
     * @return The {@link ScheduledExecutorService}.
     */
    @Bean
    public ScheduledExecutorService planCleanupExecutor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("plan-cleanup-%d").build();
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    @Bean
    public PlanRpcService planService() {
        return new PlanRpcService(planDao(),
            analysisService(),
            planNotificationSender(),
            startAnalysisThreadPool(),
            userSessionConfig.userSessionContext(),
            buyRIService(),
            groupServiceBlockingStub(),
            repositoryServiceBlockingStub(),
            planReservedInstanceService(),
            boughtRIService(),
            startAnalysisRetryTimeoutMin,
            TimeUnit.MINUTES,
            realtimeTopologyContextId);
    }

    @Bean
    public BuyRIAnalysisServiceBlockingStub buyRIService() {
        return BuyRIAnalysisServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Bean for Reserved Instance Bought Rpc Service.
     *
     * @return a ReservedInstanceBoughtServiceBlockingStub.
     */
    @Bean
    public ReservedInstanceBoughtServiceBlockingStub boughtRIService() {
        return ReservedInstanceBoughtServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Grpc stub for the plan reserved instance service.
     *
     * @return The {@link PlanReservedInstanceServiceBlockingStub}.
     */
    @Bean
    public PlanReservedInstanceServiceBlockingStub planReservedInstanceService() {
        return PlanReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Grpc stub for the cost service.
     *
     * @return The {@link CostServiceBlockingStub}.
     */
    @Bean
    public CostServiceBlockingStub costService() {
        return CostServiceGrpc.newBlockingStub(costClientConfig.costChannel());
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
    public RepositoryServiceBlockingStub repositoryServiceBlockingStub() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    /**
     * Blocking Group Service client creator.
     *
     * @return Blocking Group Service client.
     */
    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public PlanProgressListener planProgressListener() {
        final PlanProgressListener listener =  new PlanProgressListener(planDao(), planService(),
                reservationConfig.reservationPlacementHandler(), realtimeTopologyContextId);
        aoClientConfig.actionOrchestratorClient().addListener(listener);
        repositoryClientConfig.repository().addListener(listener);
        historyClientConfig.historyComponent().addListener(listener);
        globalConfig.costNotificationClient().addCostNotificationListener(listener);
        globalConfig.marketNotificationClient().addAnalysisStatusListener(listener);
        globalConfig.tpNotificationClient().addTopologySummaryListener(listener);
        return listener;
    }

    @Bean
    public IMessageSender<PlanStatusNotification> notificationSender() {
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
        return Executors.newFixedThreadPool(Math.max(1, numPlanAnalysisThreads), threadFactory);
    }

    @Bean
    public PlanInstanceQueue planInstanceQueue() {
        return new PlanInstanceQueue(planDao(), planService());
    }

    @Bean
    public PlanInstanceCompletionListener planInstanceCompletionListener() {
        final PlanInstanceCompletionListener listener =
                new PlanInstanceCompletionListener(planInstanceQueue());
        planDao().addStatusListener(listener);
        return listener;
    }
}
