package com.vmturbo.plan.orchestrator.cpucapacity;

import com.turbonomic.cpucapacity.CPUCapacityEstimator;
import com.turbonomic.cpucapacity.CPUCapacityEstimatorException;
import com.turbonomic.cpucapacity.CPUCatalog;
import com.turbonomic.cpucapacity.CPUCatalogFactory;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Configuration for the CpuCapacityRpcService.
 **/
@Configuration
public class CpuCapacityConfig {

    @Value("${cpuInfoCacheLifetimeHours}")
    private int cpuCatalogLifeHours;

    /**
     * Returns the service that exposed CPUCatalog and CPUCapacityEstimator.
     *
     * @return the service that exposed CPUCatalog and CPUCapacityEstimator.
     */
    @Bean
    public CpuCapacityRpcService cpuCapacityService() {
        return new CpuCapacityRpcService(cpuCatalog(), cpuCapacityEstimator(), cpuCatalogLifeHours);
    }

    /**
     * Returns the catalog that provides all the cpu models Turbonomic knows about.
     *
     * @return the catalog that provides all the cpu models Turbonomic knows about.
     */
    @Bean
    public CPUCatalog cpuCatalog() {
        try {
            return new CPUCatalogFactory().makeCatalog();
        } catch (CPUCapacityEstimatorException e) {
            throw new BeanCreationException("Error creating CPUCatalog bean:", e);
        }
    }

    /**
     * Returns the cpu capacity estimation that can provide scaling factors for cpu models.
     *
     * @return the cpu capacity estimation that can provide scaling factors for cpu models.
     */
    @Bean
    CPUCapacityEstimator cpuCapacityEstimator() {
        try {
            return new CPUCatalogFactory().makeEstimator();
        } catch (CPUCapacityEstimatorException e) {
            throw new BeanCreationException("Error creating CPUCatalog bean:", e);
        }
    }
}
