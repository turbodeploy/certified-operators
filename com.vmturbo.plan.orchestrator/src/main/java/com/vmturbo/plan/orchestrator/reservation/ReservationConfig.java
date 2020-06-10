package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTOREST.ReservationServiceController;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.market.PlanOrchestratorMarketConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.repository.RepositoryConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class,
    RepositoryConfig.class,
    TemplatesConfig.class,
    BaseKafkaProducerConfig.class,
    PlanOrchestratorMarketConfig.class,
        MarketClientConfig.class
})
public class ReservationConfig {
    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private PlanOrchestratorMarketConfig planOrchestratorMarketConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(planConfig.planDao(), templatesConfig.templatesDao(),
                dbConfig.reservationDao(), planConfig.planService(), reservationManager());
    }

    /**
     * Create a {@link ReservationNotificationSender}.
     *
     * @return the reservation notification sender.
     */
    @Bean
    public ReservationNotificationSender reservationNotificationSender() {
        return new ReservationNotificationSender(kafkaProducerConfig.kafkaMessageSender()
            .messageSender(PlanOrchestratorClientImpl.RESERVATION_NOTIFICATION_TOPIC));
    }

    /**
     * Create a {@link InitialPlacementServiceBlockingStub}.
     *
     * @return the InitialPlacementServiceBlockingStub.
     */
    @Bean
    public InitialPlacementServiceBlockingStub initialPlacementService() {
        return InitialPlacementServiceGrpc.newBlockingStub(marketClientConfig.marketChannel());
    }

    /**
     * create a ReservationManager.
     * @return a new ReservationManager
     */
    @Bean
    public ReservationManager reservationManager() {
        ReservationManager reservationManager = new ReservationManager(planConfig.planDao(),
                dbConfig.reservationDao(), planConfig.planService(), reservationNotificationSender(), initialPlacementService(), templatesConfig.templatesDao());
        planConfig.planDao().addStatusListener(reservationManager);
        return reservationManager;
    }

    @Bean
    public ReservationServiceController reservationServiceController() {
        return new ReservationServiceController(reservationRpcService());
    }

    @Bean
    public ReservationPlacementHandler reservationPlacementHandler() {
        ReservationPlacementHandler placementHandler = new ReservationPlacementHandler(reservationManager(),
                repositoryConfig.repositoryServiceBlockingStub());
        // Register to handle projected topology of reservation plans.
        planOrchestratorMarketConfig.planProjectedTopologyListener().addProjectedTopologyProcessor(placementHandler);
        return placementHandler;
    }
}