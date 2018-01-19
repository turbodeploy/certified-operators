package com.vmturbo.topology.processor.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;

@Configuration
@Import({TemplateConfig.class, PlanConfig.class})
public class ReservationConfig {

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private PlanConfig planConfig;


    @Bean
    public ReservationManager reservationManager() {
        return new ReservationManager(planConfig.reservationServiceBlockingStub(),
                templateConfig.templateConverterFactory());
    }
}
