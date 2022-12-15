package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.time.Duration;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.scope.CachedAggregateScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.scope.SqlCloudScopeIdentityStore;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Config for {@link CloudScopeIdentityStore} and related beans.
 */
@Configuration
public class CloudScopeConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    // Autowired from IdentityProviderConfig
    @Autowired
    private IdentityProvider identityProvider;

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
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
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
}