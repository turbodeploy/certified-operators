package com.vmturbo.cost.component.entity.cost;

import java.sql.SQLException;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.util.LogContextExecutorService;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.persistence.DataIngestionBouncer;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({MarketClientConfig.class,
        MarketListenerConfig.class,
        DbAccessConfig.class,
        CostNotificationConfig.class,
        RepositoryClientConfig.class,
        SupplyChainServiceConfig.class})
public class EntityCostConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    @Autowired
    private DataIngestionBouncer ingestionBouncer;

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
        try {
            return new SqlEntityCostStore(dbAccessConfig.dsl(), Clock.systemUTC(),
                    bulkEntityCostExecutorService(), persistEntityCostChunkSize,
                    latestEntityCostStore(), ingestionBouncer);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntityCostStore bean", e);
        }
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
        try {
            return new PlanProjectedEntityCostStore(dbAccessConfig.dsl(), persistEntityCostChunkSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PlanProjectedEntityCostStore bean", e);
        }
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
