package com.vmturbo.plan.orchestrator.plan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
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
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.impl.CostSubscription;
import com.vmturbo.cost.api.impl.CostSubscription.Topic;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
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
        GroupClientConfig.class, CostClientConfig.class, MarketClientConfig.class})
public class PlanConfig {

    /**
     * This parameter is used to control the time out hours for running plans
     */
    @Value("${planTimeOutHours}")
    private int planTimeOutHours;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${startAnalysisRetryTimeoutMin:30}")
    private long startAnalysisRetryTimeoutMin;

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
    private MarketClientConfig marketClientConfig;

    @Bean
    public PlanDao planDao() {
        return new PlanDaoImpl(dbConfig.dsl(),
                repositoryClientConfig.repositoryClient(),
                actionsRpcService(),
                statsRpcService(),
                groupClientConfig.groupChannel(),
                userSessionConfig.userSessionContext(),
                repositoryClientConfig.searchServiceClient(),
                riBuyContextService(),
                planTimeOutHours);
    }

    /**
     * Java Bean for Market component.
     *
     * @return The Market component.
     */
    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
            MarketSubscription.forTopic(MarketSubscription.Topic.AnalysisStatusNotification));
        return market;
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
            startAnalysisRetryTimeoutMin,
            TimeUnit.MINUTES);
    }

    @Bean
    public RIBuyContextFetchServiceBlockingStub riBuyContextService() {
        return RIBuyContextFetchServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public BuyRIAnalysisServiceBlockingStub buyRIService() {
        return BuyRIAnalysisServiceGrpc.newBlockingStub(costClientConfig.costChannel());
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
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
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
        costComponent().addCostNotificationListener(listener);
        marketComponent().addAnalysisStatusListener(listener);
        return listener;
    }

    @Bean
    public CostComponent costComponent() {
        return costClientConfig.costComponent(
                CostSubscription.forTopic(Topic.COST_STATUS_NOTIFICATION));
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
        return Executors.newFixedThreadPool(5, threadFactory);
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
