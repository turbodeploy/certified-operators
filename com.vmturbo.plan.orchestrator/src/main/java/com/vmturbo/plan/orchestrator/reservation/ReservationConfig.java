package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class,
        TemplatesConfig.class,
        BaseKafkaProducerConfig.class,
        MarketClientConfig.class
})
public class ReservationConfig {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(templatesConfig.templatesDao(),
                dbConfig.reservationDao(), reservationManager());
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
        ReservationManager reservationManager = new ReservationManager(dbConfig.reservationDao(),
                reservationNotificationSender(), initialPlacementService(),
                templatesConfig.templatesDao());
        return reservationManager;
    }
}