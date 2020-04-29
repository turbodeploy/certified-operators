package com.vmturbo.topology.processor.discoverydumper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.clustermgr.api.ClusterMgrClientConfig;

/**
 * Capture Spring configuration values for the {@link ComponentBasedTargetDumpingSettings}.
 */
@Configuration
@Import({ClusterMgrClientConfig.class})
public class ComponentBasedTargetDumpingSettingsConfig {

    @Autowired
    private ClusterMgrClientConfig clusterMgrClientConfig;

    @Value("${component_type}")
    private String componentType;

    @Value("${instance_id}")
    private String componentInstanceId;

    @Bean
    public ComponentBasedTargetDumpingSettings componentBasedTargetDumpingSettings() {
        return new ComponentBasedTargetDumpingSettings(componentType, componentInstanceId,
                clusterMgrClientConfig.restClient());
    }
}
