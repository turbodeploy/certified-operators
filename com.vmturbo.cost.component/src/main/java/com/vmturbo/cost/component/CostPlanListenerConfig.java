package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;

/**
 * Configuration for the cost componen't plan orchestrator listener.
 */
@Configuration
@Import({PlanOrchestratorClientConfig.class,
    ReservedInstanceConfig.class,
    EntityCostConfig.class})
public class CostPlanListenerConfig {

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    /**
     * Listener for plan status updates which deletes data when a plan is deleted.
     *
     * @return The {@link CostPlanGarbageCollector}.
     */
    @Bean
    public PlanGarbageDetector costPlanGarbageDetector() {
        final CostPlanGarbageCollector collector = new CostPlanGarbageCollector(
            reservedInstanceConfig.actionContextRIBuyStore(),
            entityCostConfig.planProjectedEntityCostStore(),
            reservedInstanceConfig.planReservedInstanceStore(),
            entityCostConfig.entityCostStore(),
            reservedInstanceConfig.buyReservedInstanceStore());
        return planOrchestratorClientConfig.newPlanGarbageDetector(collector);
    }
}
