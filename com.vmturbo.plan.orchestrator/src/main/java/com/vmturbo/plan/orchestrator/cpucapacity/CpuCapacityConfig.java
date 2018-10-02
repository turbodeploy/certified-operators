package com.vmturbo.plan.orchestrator.cpucapacity;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.turbonomic.cpucapacity.CPUCapacityEstimator;
import com.turbonomic.cpucapacity.CPUCapacityEstimatorException;
import com.turbonomic.cpucapacity.CPUCatalog;
import com.turbonomic.cpucapacity.CPUCatalogFactory;

/**
 * Spring Configuration for the CpuCapacityRpcService.
 **/
@Configuration
public class CpuCapacityConfig {

    @Value("${cpuInfoCacheLifetimeHours}")
    private int cpuCatalogLifeHours;

    @Bean
    public CpuCapacityRpcService cpuCapacityService() {
        return new CpuCapacityRpcService(cpuCatalog(), cpuCapacityEstimator(), cpuCatalogLifeHours);
    }

    @Bean
    public CPUCatalog cpuCatalog() {
        try {
            return new CPUCatalogFactory().makeCatalog();
        } catch (CPUCapacityEstimatorException e) {
            throw new BeanCreationException("Error creating CPUCatalog bean:", e);
        }
    }

    @Bean
    CPUCapacityEstimator cpuCapacityEstimator() {
        try {
            return new CPUCatalogFactory().makeEstimator();
        } catch (CPUCapacityEstimatorException e) {
            throw new BeanCreationException("Error creating CPUCatalog bean:", e);
        }
    }
}
