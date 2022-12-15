package com.vmturbo.cost.component.billed.cost;

import java.sql.SQLException;
import java.time.Duration;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.persistence.DataQueueFactory.DefaultDataQueueFactory;
import com.vmturbo.common.protobuf.cost.BilledCostServicesREST.BilledCostServiceController;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.CloudScopeConfig;
import com.vmturbo.cost.component.ScopeIdReplacementLogConfig;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.partition.IPartitioningManager;

/**
 * Spring configuration for cloud cost beans, including the {@link CloudCostStore} and {@link BilledCostRpcService}.
 * In the short-term, this config is named "CloudCost" to avoid conflicts with the BilledCostConfig. These two configs
 * will be merged as we transition to partitioned billed cost data.
 */
@Configuration
@Import({CloudScopeConfig.class, ScopeIdReplacementLogConfig.class})
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

    // Autowired from CostPartitioningConfig
    @Autowired
    private IPartitioningManager partitioningManager;

    @Autowired
    private CloudScopeConfig cloudScopeConfig;

    @Autowired
    private ScopeIdReplacementLogConfig scopeIdReplacementLogConfig;

    @Value("${cloud.cost.asyncPersistenceConcurrency:0}")
    private int costPersistenceConcurrency;

    @Value("${cloud.cost.asyncPersistenceTimeoutDuration:PT5M}")
    private String costPersistenceTimeoutDuration;

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
                    cloudScopeConfig.cloudScopeIdentityProvider(),
                    scopeIdReplacementLogConfig.sqlCloudCostScopeIdReplacementLog(),
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