package com.vmturbo.topology.processor.identity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.IdentityREST.IdentityServiceController;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.probes.ProbeConfig;

/**
 * Configuration for the identity provider the Topology Processor
 * uses to assign OID's to objects.
 *
 * <p>>Includes configuration for the Identity Service which does most
 * of the heavy lifting of OID assignment.
 */
@Configuration
@Import({KVConfig.class, TopologyProcessorDBConfig.class})
public class IdentityProviderConfig {

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Value("${identityGeneratorPrefix:1}")
    private long identityGeneratorPrefix;

    @Value("${assignedIdReloadReattemptIntervalSeconds:10}")
    private long assignedIdReloadReattemptIntervalSeconds;

    @Value("${identityStoreinitializationTimeoutMin:20}")
    private int identityStoreinitializationTimeoutMin;

    @Value("${useIdentityRecordsCache:false}")
    private boolean useIdentityRecordsCache;


    @Bean
    public IdentityDatabaseStore identityDatabaseStore() {
        return new IdentityDatabaseStore(topologyProcessorDBConfig.dsl());
    }

    @Bean
    public IdentityProvider identityProvider() {
        return new IdentityProviderImpl(
            kvConfig.keyValueStore(),
            probeConfig.compatibilityChecker(),
            identityGeneratorPrefix,
            identityDatabaseStore(),
            identityStoreinitializationTimeoutMin,
            assignedIdReloadReattemptIntervalSeconds,
            useIdentityRecordsCache);
    }

    @Bean
    public IdentityRpcService identityRpcService() {
        return new IdentityRpcService(probeConfig.probeStore());
    }

    @Bean
    public IdentityServiceController identityServiceController() {
        return new IdentityServiceController(identityRpcService());
    }
}
