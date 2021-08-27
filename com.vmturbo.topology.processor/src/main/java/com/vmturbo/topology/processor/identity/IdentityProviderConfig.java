package com.vmturbo.topology.processor.identity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.topology.IdentityREST.IdentityServiceController;
import com.vmturbo.topology.processor.ClockConfig;
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

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Value("${identityGeneratorPrefix:1}")
    private long identityGeneratorPrefix;

    @Value("${assignedIdReloadReattemptIntervalSeconds:10}")
    private long assignedIdReloadReattemptIntervalSeconds;

    @Value("${identityStoreinitializationTimeoutMin:20}")
    private int identityStoreinitializationTimeoutMin;

    @Value("${useIdentityRecordsCache:false}")
    private boolean useIdentityRecordsCache;

    @Value("${entityExpirationTimeDays:30}")
    private int entityExpirationTimeDays;

    @Value("${entityValidationFrequencyHours:6}")
    private int entityValidationFrequencyHours;

    @Value("${expireOids:false}")
    private boolean expireOids;

    @Value("${expirationDaysPerEntity:}")
    private String serializedExpirationDaysPerEntity;

    @Value("${initialExpirationDelayMins:720}")
    private int initialExpirationDelayMin;

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
            useIdentityRecordsCache,
            staleOidManager());
    }

    @Bean
    public IdentityRpcService identityRpcService() {
        return new IdentityRpcService(probeConfig.probeStore());
    }

    @Bean
    public IdentityServiceController identityServiceController() {
        return new IdentityServiceController(identityRpcService());
    }

    /**
     * Setup and return a ScheduledExecutorService for the running of recurrent tasks.
     *
     * @return a new single threaded scheduled executor service with the thread factory configured.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService oidsExpirationScheduledExecutor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("oids-expiration-task")
            .build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    /**
     * Get a {@link StaleOidManager}.
     *
     * @return a {@link StaleOidManager}.
     */
    @Bean
    public StaleOidManager staleOidManager() {
        YAMLMapper mapper = new YAMLMapper();
        Map<String, String> expirationDaysMap = new HashMap<>();
        if (!serializedExpirationDaysPerEntity.isEmpty()) {
            try {
                expirationDaysMap =
                    mapper.convertValue(mapper.readTree(serializedExpirationDaysPerEntity),
                        HashMap.class);
            } catch (JsonProcessingException e) {
                logger.error("Could not convert the expirationDaysPerEntity into a map, those "
                        + "settings will be ignored");
            }
        }
        return new StaleOidManagerImpl(
                Math.max(TimeUnit.DAYS.toMillis(1), TimeUnit.DAYS.toMillis(entityExpirationTimeDays)),
                Math.max(TimeUnit.HOURS.toMillis(1), TimeUnit.HOURS.toMillis(entityValidationFrequencyHours)),
                Math.max(0, TimeUnit.MINUTES.toMillis(initialExpirationDelayMin)), topologyProcessorDBConfig.dsl(),
                clockConfig.clock(), expireOids, oidsExpirationScheduledExecutor(), expirationDaysMap);
    }
}
