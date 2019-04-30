package com.vmturbo.clustermgr;

import org.mockito.Mockito;
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
        return new ClusterMgrService(consulService(), new OsCommandProcessRunner());
    }

    @Bean
    public ConsulService consulService() {
        return Mockito.mock(ConsulService.class);
    }
}
