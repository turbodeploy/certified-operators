package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class, TemplatesConfig.class, BaseKafkaProducerConfig.class,
        MarketClientConfig.class})
public class ReservationConfig {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Value("${prepareReservationCache:true}")
    private boolean prepareReservationCache;

    @Value("${delayedDeletionTimeInMillis:172800000}")
    private long delayedDeletionTimeInMillis;

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(templatesConfig.templatesDao(),
                dbConfig.reservationDao(), reservationManager(),
                delayedDeletionTimeInMillis);
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
     *
     * @return a new ReservationManager
     */
    @Bean
    public ReservationManager reservationManager() {
        return new ReservationManager(dbConfig.reservationDao(), reservationNotificationSender(),
                initialPlacementService(), templatesConfig.templatesDao(), planConfig.planDao(),
                planConfig.planService(), prepareReservationCache,
                planConfig.groupServiceBlockingStub());
    }
}