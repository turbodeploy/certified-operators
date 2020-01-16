package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.ReservationDTOREST.ReservationServiceController;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.repository.RepositoryConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class, RepositoryConfig.class, TemplatesConfig.class})
public class ReservationConfig {
    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;


    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(planConfig.planDao(), templatesConfig.templatesDao(),
                dbConfig.reservationDao(), planConfig.planService());
    }

    @Bean
    public ReservationServiceController reservationServiceController() {
        return new ReservationServiceController(reservationRpcService());
    }

    @Bean
    public ReservationPlacementHandler reservationPlacementHandler() {
        return new ReservationPlacementHandler(dbConfig.reservationDao(),
                repositoryConfig.repositoryServiceBlockingStub());
    }
}