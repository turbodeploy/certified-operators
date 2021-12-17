package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cost.component.billedcosts.BatchInserter;
import com.vmturbo.cost.component.billedcosts.BilledCostStore;
import com.vmturbo.cost.component.billedcosts.BilledCostUploadRpcService;
import com.vmturbo.cost.component.billedcosts.SqlBilledCostStore;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagIdentityService;
import com.vmturbo.cost.component.billedcosts.TagStore;
import com.vmturbo.cost.component.cleanup.CostCleanupConfig;

/**
 * Configuration for BilledCostUploadRpcService.
 */
@Configuration
@Import({IdentityProviderConfig.class, CostDBConfig.class})
public class BilledCostConfig {
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private CostCleanupConfig costCleanupConfig;

    @Value("${billedCostDataBatchSize:500}")
    private int billedCostDataBatchSize;

    @Value("${parallelBatchInserts:15}")
    private int parallelBatchInserts;

    /**
     * Returns an instance of BilledCostUploadRpcService.
     *
     * @return an instance of BilledCostUploadRpcService.
     */
    @Bean
    public BilledCostUploadRpcService billedCostUploadRpcService() {
        return new BilledCostUploadRpcService(tagGroupIdentityService(), billedCostStore());
    }

    /**
     * Returns an instance of BilledCostStoreFactory.
     *
     * @return an instance of BilledCostStoreFactory.
     */
    @Bean
    public BilledCostStore billedCostStore() {
        return new SqlBilledCostStore(dbConfig.dsl(), batchInserter(),
                costCleanupConfig.timeFrameCalculator());
    }

    /**
     * Returns an instance of TagGroupIdentityResolver.
     *
     * @return an instance of TagGroupIdentityResolver.
     */
    @Bean
    public TagGroupIdentityService tagGroupIdentityService() {
        return new TagGroupIdentityService(tagGroupStore(), tagIdentityService(), identityProvider());
    }

    /**
     * Returns an instance of TagIdentityResolver.
     *
     * @return an instance of TagIdentityResolver.
     */
    @Bean
    public TagIdentityService tagIdentityService() {
        return new TagIdentityService(tagStore(), identityProvider());
    }

    /**
     * Returns an instance of TagGroupStore.
     *
     * @return an instance of TagGroupStore.
     */
    @Bean
    public TagGroupStore tagGroupStore() {
        return new TagGroupStore(dbConfig.dsl(), batchInserter());
    }

    /**
     * Returns an instance of TagStore.
     *
     * @return an instance of TagStore.
     */
    @Bean
    public TagStore tagStore() {
        return new TagStore(dbConfig.dsl(), batchInserter());
    }

    /**
     * Returns an instance of BatchInserter.
     *
     * @return an instance of BatchInserter.
     */
    @Bean
    public BatchInserter batchInserter() {
        return new BatchInserter(billedCostDataBatchSize, parallelBatchInserts);
    }

    /**
     * Returns an instance of IdentityProvider.
     *
     * @return an instance of IdentityProvider.
     */
    public IdentityProvider identityProvider() {
        return identityProviderConfig.identityProvider();
    }
}
