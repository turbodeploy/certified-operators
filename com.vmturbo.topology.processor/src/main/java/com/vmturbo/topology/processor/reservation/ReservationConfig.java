package com.vmturbo.topology.processor.reservation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;

@Configuration
@Import({TemplateConfig.class,
    PlanOrchestratorClientConfig.class,
    GroupClientConfig.class})
public class ReservationConfig {

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TargetConfig targetConfig;

    /**
     * Validates reservations before creating entities in the topology.
     *
     * @return The {@link ReservationValidator}.
     */
    @Bean
    public ReservationValidator reservationValidator() {
        return new ReservationValidator(GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()),
            PolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    /**
     * Responsible for inserting reserved entities into the topology.
     *
     * @return The {@link ReservationManager}.
     */
    @Bean
    public ReservationManager reservationManager() {
        return new ReservationManager(ReservationServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel()),
            templateConfig.templateConverterFactory(),
            reservationValidator(),
            targetConfig.targetStore());
    }
}
