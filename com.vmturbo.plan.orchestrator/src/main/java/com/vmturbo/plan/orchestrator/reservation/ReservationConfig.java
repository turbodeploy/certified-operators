package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.ReservationDTOREST.ReservationServiceController;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.repository.RepositoryConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({SQLDatabaseConfig.class, RepositoryConfig.class})
public class ReservationConfig {
    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Bean
    public ReservationDao reservationDao() {
        return new ReservationDaoImpl(dbConfig.dsl());
    }

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(planConfig.planDao(), reservationDao(),
                planConfig.planService());
    }

    @Bean
    public ReservationServiceController reservationServiceController() {
        return new ReservationServiceController(reservationRpcService());
    }

    @Bean
    public ReservationPlacementHandler reservationPlacementHandler() {
        return new ReservationPlacementHandler(reservationDao(),
                repositoryConfig.repositoryServiceBlockingStub());
    }
}