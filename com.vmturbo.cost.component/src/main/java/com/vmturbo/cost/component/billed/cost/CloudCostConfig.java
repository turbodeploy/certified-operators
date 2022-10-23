package com.vmturbo.cost.component.billed.cost;

import java.sql.SQLException;
import java.time.Duration;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.persistence.DataQueueFactory.DefaultDataQueueFactory;
import com.vmturbo.cloud.common.scope.CachedAggregateScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.common.protobuf.cost.BilledCostServicesREST.BilledCostServiceController;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.scope.SqlCloudScopeIdentityStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.partition.IPartitioningManager;

/**
 * Spring configuration for cloud cost beans, including the {@link CloudCostStore} and {@link BilledCostRpcService}.
 * In the short-term, this config is named "CloudCost" to avoid conflicts with the BilledCostConfig. These two configs
 * will be merged as we transition to partitioned billed cost data.
 */
@Configuration
public class CloudCostConfig {

    // Should be defined in CostDBConfig
    @Autowired
    private DbAccessConfig dbAccessConfig;

    // Should be auto-wired from ReservedInstanceConfig
    @Autowired
    private TimeFrameCalculator timeFrameCalculator;

    // Autowired from BilledCostConfig
    @Autowired
    private TagGroupIdentityService tagGroupIdentityService;

    // Autowired from IdentityProviderConfig
    @Autowired
    private IdentityProvider identityProvider;

    // Autowired from CostPartitioningConfig
    @Autowired
    private IPartitioningManager partitioningManager;

    /**
     * Batch size for inserting into the cloud scope identity table.
     */
    @Value("${cloud.scope.batchStoreSize:1000}")
    private int cloudScopeBatchStoreSize;

    @Value("${cloud.scope.persistenceCacheEnabled:true}")
    private boolean scopePersistenceCacheEnabled;

    @Value("${cloud.scope.cacheInitializationDuration:PT10M}")
    private String scopeCacheInitializationDuration;

    @Value("${cloud.scope.persistenceMaxRetries:3}")
    private int scopePersistenceMaxRetries;

    @Value("${cloud.scope.persistenceMinRetryDelay:PT10S}")
    private String scopePersistenceMinRetryDelay;

    @Value("${cloud.scope.persistenceMaxRetryDelay:PT30S}")
    private String scopePersistenceMaxRetryDelay;

    @Value("${cloud.cost.asyncPersistenceConcurrency:0}")
    private int costPersistenceConcurrency;

    @Value("${cloud.cost.asyncPersistenceTimeoutDuration:PT5M}")
    private String costPersistenceTimeoutDuration;

    /**
     * Creates a new {@link CloudScopeIdentityStore} instance.
     * @return The newly created {@link CloudScopeIdentityStore} instance.
     */
    @Bean
    public CloudScopeIdentityStore cloudScopeIdentityStore() {
        try {

            final CloudScopeIdentityStore.PersistenceRetryPolicy retryPolicy = CloudScopeIdentityStore.PersistenceRetryPolicy.builder()
                    .maxRetries(scopePersistenceMaxRetries)
                    .minRetryDelay(Duration.parse(scopePersistenceMinRetryDelay))
                    .maxRetryDelay(Duration.parse(scopePersistenceMaxRetryDelay))
                    .build();

            return new SqlCloudScopeIdentityStore(
                    dbAccessConfig.dsl(),
                    retryPolicy,
                    scopePersistenceCacheEnabled,
                    cloudScopeBatchStoreSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            throw new BeanCreationException("Failed to create CloudScopeIdentityStore", e);
        }
    }

    /**
     * Creates a new {@link CloudScopeIdentityProvider} instance.
     * @return The newly created {@link CloudScopeIdentityProvider} instance.
     */
    @Bean
    public CloudScopeIdentityProvider cloudScopeIdentityProvider() {
        return new CachedAggregateScopeIdentityProvider(
                cloudScopeIdentityStore(),
                identityProvider,
                Duration.parse(scopeCacheInitializationDuration));
    }

    /**
     * Creates a new {@link DataQueueFactory} instance.
     * @return The newly created {@link DataQueueFactory} instance.
     */
    @Bean
    public DataQueueFactory dataQueueFactory() {
        return new DefaultDataQueueFactory();
    }

    /**
     * Creates a new {@link CloudCostStore} instance.
     * @return The newly created {@link CloudCostStore} instance.
     */
    @Bean
    public CloudCostStore cloudCostStore() {

        try {

            final int persistenceConcurrency = costPersistenceConcurrency > 0
                    ? costPersistenceConcurrency
                    : Runtime.getRuntime().availableProcessors();
            final BilledCostPersistenceConfig persistenceConfig = BilledCostPersistenceConfig.builder()
                    .concurrency(persistenceConcurrency)
                    .persistenceTimeout(Duration.parse(costPersistenceTimeoutDuration))
                    .build();

            return new SqlCloudCostStore(
                    partitioningManager,
                    tagGroupIdentityService,
                    cloudScopeIdentityProvider(),
                    dataQueueFactory(),
                    timeFrameCalculator,
                    dbAccessConfig.dsl(),
                    persistenceConfig);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            throw new BeanCreationException("Failed to create CloudCostStore", e);
        }
    }

    /**
     * Creates a new {@link BilledCostRpcService} instance.
     * @return The newly created {@link BilledCostRpcService} instance.
     */
    @Bean
    public BilledCostRpcService billedCostRpcService() {
        return new BilledCostRpcService(cloudCostStore());
    }

    /**
     * Creates a new {@link BilledCostServiceController} instance. The controller is required
     * for swagger functionality.
     * @return The newly created {@link BilledCostServiceController} instance.
     */
    @Bean
    public BilledCostServiceController billedCostServiceController() {
        return new BilledCostServiceController(billedCostRpcService());
    }
}
