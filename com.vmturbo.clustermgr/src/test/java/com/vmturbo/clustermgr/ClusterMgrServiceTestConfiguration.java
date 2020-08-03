package com.vmturbo.clustermgr;

import static org.mockito.Mockito.mock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.clustermgr.management.ComponentRegistry;
import com.vmturbo.components.common.OsCommandProcessRunner;

/**
 * Test configuration for {@link ClusterMgrServiceTest}.
 */
@Configuration
public class ClusterMgrServiceTestConfiguration {

    @Bean
    public ClusterMgrService clusterMgrService() {
        return new ClusterMgrService(consulService(),
            new OsCommandProcessRunner(), mock(DiagEnvironmentSummary.class), serviceRegistry());
    }

    /**
     * Mock {@link ComponentRegistry}.
     *
     * @return The mock.
     */
    @Bean
    public ComponentRegistry serviceRegistry() {
        return mock(ComponentRegistry.class);
    }

    @Bean
    public ConsulService consulService() {
        return mock(ConsulService.class);
    }
}
