package com.vmturbo.clustermgr;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Test configuration for {@link ClusterMgrServiceTest}.
 */
@Configuration
public class ClusterMgrServiceTestConfiguration {

    @Bean
    public ClusterMgrService clusterMgrService() {
        return new ClusterMgrService(consulService(), factoryInstalledComponentsService());
    }

    @Bean
    public ConsulService consulService() {
        return Mockito.mock(ConsulService.class);
    }

    @Bean
    public FactoryInstalledComponentsService factoryInstalledComponentsService() {
        return new FactoryInstalledComponentsService();
    }
}
