package com.vmturbo.topology.processor.cpucapacity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
 * Spring configuration for the CpuCapacityStore
 **/
@Configuration
@Import({PlanOrchestratorClientConfig.class})
public class CpuCapacityConfig {

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Value("${scaleFactorCacheTimeoutHr:8}")
    private int scaleFactorCacheTimeoutHr;

    @Bean
    public CpuCapacityStore cpucCapacityStore() {
        return new RemoteCpuCapacityStore(cpuCapacityServiceBlockingStub(), scaleFactorCacheTimeoutHr);
    }

    @Bean
    public CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub() {
        return CpuCapacityServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

}
