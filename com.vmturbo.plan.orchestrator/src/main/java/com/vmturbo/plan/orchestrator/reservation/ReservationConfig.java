package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.ReservationDTOREST.ReservationServiceController;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.repository.RepositoryConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class, RepositoryConfig.class, TemplatesConfig.class, BaseKafkaProducerConfig.class})
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
     * create a ReservationManager.
     * @return a new ReservationManager
     */
    @Bean
    public ReservationManager reservationManager() {
        ReservationManager reservationManager = new ReservationManager(planConfig.planDao(),
                dbConfig.reservationDao(), planConfig.planService(), reservationNotificationSender());
        planConfig.planDao().addStatusListener(reservationManager);
        return reservationManager;
    }

    @Bean
    public ReservationServiceController reservationServiceController() {
        return new ReservationServiceController(reservationRpcService());
    }

    @Bean
    public ReservationPlacementHandler reservationPlacementHandler() {
        return new ReservationPlacementHandler(reservationManager(),
                repositoryConfig.repositoryServiceBlockingStub());
    }
}