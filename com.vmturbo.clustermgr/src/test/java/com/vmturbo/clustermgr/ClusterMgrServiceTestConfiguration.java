package com.vmturbo.clustermgr;

import static org.mockito.Mockito.mock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.clustermgr.management.ComponentRegistry;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.OsProcessFactory;

/**
 * Test configuration for {@link ClusterMgrServiceTest}.
 */
@Configuration
public class ClusterMgrServiceTestConfiguration {

    @Bean
    public ClusterMgrService clusterMgrService() {
        return new ClusterMgrService(consulService(), osCommandProcessRunner(),
                mock(DiagEnvironmentSummary.class), serviceRegistry());
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

    /**
     * Mock {@link OsCommandProcessRunner}.
     * @return The mock.
     */
    @Bean
    public OsCommandProcessRunner osCommandProcessRunner() {
        return mock(OsCommandProcessRunner.class);
    }

    /**
     * Mock the processFactory in {@link OsCommandProcessRunner}.
     * @return The mock.
     */
    @Bean
    public OsProcessFactory osProcessFactory() {
        return mock(OsProcessFactory.class);
    }
}
