package com.vmturbo.topology.processor.identity;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.IdentityREST.IdentityServiceController;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
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

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${assignedIdReloadReattemptIntervalSeconds}")
    private long assignedIdReloadReattemptIntervalSeconds;


    @Bean
    public IdentityServiceUnderlyingStore underlyingStore() {
        return new IdentityServiceInMemoryUnderlyingStore(identityDatabaseStore(),
                assignedIdReloadReattemptIntervalSeconds, TimeUnit.SECONDS);
    }

    @Bean
    public IdentityDatabaseStore identityDatabaseStore() {
        return new IdentityDatabaseStore(topologyProcessorDBConfig.dsl());
    }

    @Bean
    public HeuristicsMatcher heuristicsMatcher() {
        return new HeuristicsMatcher();
    }

    @Bean
    public IdentityService identityService() {
        return new IdentityService(underlyingStore(), heuristicsMatcher());
    }

    @Bean
    public IdentityProvider identityProvider() {
        return new IdentityProviderImpl(
            identityService(),
            kvConfig.keyValueStore(),
            probeConfig.compatibilityChecker(),
            identityGeneratorPrefix);
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
