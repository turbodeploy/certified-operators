package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.persistence.DataQueueConfiguration;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.scope.SqlCloudCostScopeIdReplacementLog;
import com.vmturbo.cost.component.scope.SqlOidMappingStore;
import com.vmturbo.cost.component.scope.UploadAliasedOidsRpcService;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Config for Scope id replacement log and related beans.
 */
@Configuration
@Import({DbAccessConfig.class, CloudScopeConfig.class})
public class ScopeIdReplacementLogConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private CloudScopeConfig cloudScopeConfig;

    @Value("${cost.scopeIdReplacementConcurrency:5}")
    private int scopeIdReplacementConcurrency;

    @Value("${cost.scopeIdReplacementBatchSize:500}")
    private int scopeIdReplacementBatchSize;

    @Value("${cost.scopeIdReplacementPersistenceTimeout:PT15M}")
    private String scopeIdReplacementPersistenceTimeout;

    /**
     * Returns an instance of {@link UploadAliasedOidsRpcService}.
     *
     * @return an instance of {@link UploadAliasedOidsRpcService}.
     */
    @Bean
    public UploadAliasedOidsRpcService uploadAliasedOidsRpcService() {
        return new UploadAliasedOidsRpcService(sqlOidMappingStore(),
            Collections.singletonList(sqlCloudCostScopeIdReplacementLog()));
    }

    /**
     * Returns an instance of {@link SqlOidMappingStore}.
     *
     * @return an instance of {@link SqlOidMappingStore}.
     */
    @Bean
    public SqlOidMappingStore sqlOidMappingStore() {
        try {
            return new SqlOidMappingStore(dbAccessConfig.dsl());
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SqlOidMappingStore.", e);
        }
    }

    /**
     * Returns an instance of {@link SqlCloudCostScopeIdReplacementLog}.
     *
     * @return an instance of {@link SqlCloudCostScopeIdReplacementLog}.
     */
    @Bean
    public SqlCloudCostScopeIdReplacementLog sqlCloudCostScopeIdReplacementLog() {
        try {
            final DataQueueConfiguration dataQueueConfiguration = DataQueueConfiguration.builder()
                .queueName("cloud-cost-scope-id-replacement-log")
                .concurrency(scopeIdReplacementConcurrency)
                .build();
            return new SqlCloudCostScopeIdReplacementLog(dbAccessConfig.dsl(), sqlOidMappingStore(),
                cloudScopeConfig.cloudScopeIdentityStore(), dataQueueConfiguration, scopeIdReplacementBatchSize,
                Duration.parse(scopeIdReplacementPersistenceTimeout));
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SqlCloudCostScopeIdReplacementLog.", e);
        }
    }
}