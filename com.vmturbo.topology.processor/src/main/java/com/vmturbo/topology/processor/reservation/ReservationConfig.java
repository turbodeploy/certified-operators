package com.vmturbo.topology.processor.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;

@Configuration
@Import({TemplateConfig.class,
    PlanOrchestratorClientConfig.class})
public class ReservationConfig {

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Bean
    public ReservationManager reservationManager() {
        return new ReservationManager(ReservationServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel()),
                templateConfig.templateConverterFactory());
    }
}
