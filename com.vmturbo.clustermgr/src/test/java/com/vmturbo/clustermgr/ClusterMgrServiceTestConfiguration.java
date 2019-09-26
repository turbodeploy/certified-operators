package com.vmturbo.clustermgr;

import static org.mockito.Mockito.mock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.OsCommandProcessRunner;

/**
 * Test configuration for {@link ClusterMgrServiceTest}.
 */
@Configuration
public class ClusterMgrServiceTestConfiguration {

    @Bean
    public ClusterMgrService clusterMgrService() {
        return new ClusterMgrService(consulService(),
            new OsCommandProcessRunner(), mock(DiagEnvironmentSummary.class));
    }

    @Bean
    public ConsulService consulService() {
        return mock(ConsulService.class);
    }
}
