package com.vmturbo.topology.processor.planexport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc.PlanExportServiceStub;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;

/**
 * Spring configuration for services related to Plan Destination upload to the Plan Orchestrator.
 **/
@Configuration
@Import({PlanOrchestratorClientConfig.class})
public class PlanDestinationConfig {
    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    /**
     * Returns the component that manages plan destination uploads to the Plan Orchestrator.
     *
     * @return the plan destination uploader.
     */
    @Bean
    public DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader() {
        return new DiscoveredPlanDestinationUploader(planExportServiceStub());
    }

    /**
     * Returns the async plan export service client.
     *
     * @return the async client
     */
    @Bean
    public PlanExportServiceStub planExportServiceStub() {
        return PlanExportServiceGrpc.newStub(planOrchestratorClientConfig.planOrchestratorChannel());
    }
}
