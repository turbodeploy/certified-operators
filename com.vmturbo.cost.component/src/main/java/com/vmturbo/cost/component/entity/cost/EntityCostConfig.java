package com.vmturbo.cost.component.entity.cost;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.util.LogContextExecutorService;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Configuration
@Import({MarketClientConfig.class,
        MarketListenerConfig.class,
        CostDBConfig.class,
        CostNotificationConfig.class,
        RepositoryClientConfig.class,
        SupplyChainServiceConfig.class})
public class EntityCostConfig {

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    @Autowired
    private MarketComponent marketComponent;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private SupplyChainServiceConfig supplyChainServiceConfig;

    @Value("${entityCost.concurrentPersistenceThreads:5}")
    private int concurrentEntityCostThreads;

    @Bean
    protected ThreadFactory workerThreadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("entity-cost-batch-worker-%d")
                .build();
    }

    @Bean
    protected ExecutorService bulkEntityCostExecutorService() {
        int numThreads = concurrentEntityCostThreads > 0
                ? concurrentEntityCostThreads
                : Runtime.getRuntime().availableProcessors();
        return LogContextExecutorService.newExecutorService(
                Executors.newFixedThreadPool(numThreads, workerThreadFactory()));
    }

    @Bean
    public EntityCostStore entityCostStore() {
        return new SqlEntityCostStore(
                databaseConfig.dsl(),
                Clock.systemUTC(),
                bulkEntityCostExecutorService(),
                persistEntityCostChunkSize,
                latestEntityCostStore());
    }

    @Bean
    public InMemoryEntityCostStore projectedEntityCostStore() {
        return new InMemoryEntityCostStore(repositoryClientConfig.repositoryClient(),
                supplyChainServiceConfig.supplyChainRpcService(), realtimeTopologyContextId);
    }

    @Bean
    public InMemoryEntityCostStore latestEntityCostStore() {
        return new InMemoryEntityCostStore(repositoryClientConfig.repositoryClient(),
                supplyChainServiceConfig.supplyChainRpcService(), realtimeTopologyContextId);
    }


    @Bean
    public PlanProjectedEntityCostStore planProjectedEntityCostStore() {
        return new PlanProjectedEntityCostStore(databaseConfig.dsl(),
                persistEntityCostChunkSize);
    }

    @Bean
    public CostComponentProjectedEntityCostListener projectedEntityCostListener() {
        final CostComponentProjectedEntityCostListener projectedEntityCostListener =
                new CostComponentProjectedEntityCostListener(projectedEntityCostStore(),
                        planProjectedEntityCostStore(),
                        costNotificationConfig.costNotificationSender());
        marketComponent.addProjectedEntityCostsListener(projectedEntityCostListener);
        return projectedEntityCostListener;
    }
}
