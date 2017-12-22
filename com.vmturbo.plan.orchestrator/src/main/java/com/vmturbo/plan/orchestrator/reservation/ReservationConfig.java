package com.vmturbo.plan.orchestrator.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import com.vmturbo.common.protobuf.plan.PlanDTOREST.ReservationServiceController;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;

/**
 * Spring Configuration for Reservation services.
 */
public class ReservationConfig {
    @Autowired
    private PlanConfig planConfig;

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(planConfig.planDao(), planConfig.planService());
    }

    @Bean
    public ReservationServiceController reservationServiceController() {
        return new ReservationServiceController(reservationRpcService());
    }
}