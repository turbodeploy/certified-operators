package com.vmturbo.topology.processor.identity;

import static com.vmturbo.components.common.PropertiesHelpers.parseDuration;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.IdentityREST.IdentityServiceController;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.DbAccessConfig;
import com.vmturbo.topology.processor.KVConfig;
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
@Import({KVConfig.class, DbAccessConfig.class})
public class IdentityProviderConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    @Value("${useDescriptorsBasedCache:false}")
    private boolean useDescriptorsBasedCache;

    @Value("${entityExpirationTimeDays:30}")
    private String entityExpirationTimeDays;
    private Duration entityExpirationTimeDuration;

    @Value("${entityValidationFrequencyHours:6}")
    private int entityValidationFrequencyHours;

    @Value("${expireOids:true}")
    private boolean expireOids;

    @Value("${expirationDaysPerEntity:}")
    private String serializedExpirationDaysPerEntity;

    @Value("${initialExpirationDelayMins:720}")
    private int initialExpirationDelayMin;

    @Value("${shouldDeleteExpiredOids:true}")
    private boolean shouldDeleteExpiredOids;

    @Value("${expiredRecordsRetentionDays:60}")
    private int expiredRecordsRetentionDays;

    /**
     * After the Spring Environment is constructed convert duration properties to {@link Duration}s.
     */
    @PostConstruct
    private void fixDurations() {
        entityExpirationTimeDuration =
                parseDuration("entityExpirationTimeDays", entityExpirationTimeDays,
                        ChronoUnit.DAYS);
    }

    @Bean
    public IdentityDatabaseStore identityDatabaseStore() {
        try {
            return new IdentityDatabaseStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create IdentityDatabaseStore", e);
        }
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
            staleOidManager(), useDescriptorsBasedCache);
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
        try {
            final long entityExpirationTimeMs;
            if (entityExpirationTimeDuration.isZero()) {
                entityExpirationTimeMs = TimeUnit.DAYS.toMillis(1);
            } else {
                entityExpirationTimeMs = entityExpirationTimeDuration.toMillis();
            }
            final long validationFrequencyMs = Math.max(TimeUnit.HOURS.toMillis(1), TimeUnit.HOURS.toMillis(entityValidationFrequencyHours));
            final long initialExpirationDelayMs = Math.max(0, TimeUnit.MINUTES.toMillis(initialExpirationDelayMin));

            final StaleOidManagerImpl.OidManagementParameters.Builder oidManagementParameters =
                    new StaleOidManagerImpl.OidManagementParameters.Builder(entityExpirationTimeMs, dbAccessConfig.dsl(), expireOids,
                            clockConfig.clock(), validationFrequencyMs, shouldDeleteExpiredOids, expiredRecordsRetentionDays);

            if (!expirationDaysMap.isEmpty()) {
                oidManagementParameters.setExpirationDaysPerEntity(expirationDaysMap);
            }

            return new StaleOidManagerImpl(initialExpirationDelayMs, oidsExpirationScheduledExecutor(), oidManagementParameters.build());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create staleOidManager", e);
        }
    }
}
