package com.vmturbo.topology.processor.cpucapacity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.topology.processor.plan.PlanConfig;

/**
 * Spring configuration for the CpuCapacityStore
 **/
@Configuration
public class CpuCapacityConfig {

    @Autowired
    private PlanConfig planClientConfig;

    @Value("${scaleFactorCacheTimeoutHr}")
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
