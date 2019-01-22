package com.vmturbo.action.orchestrator.stats;


import java.time.Clock;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.stats.aggregator.ClusterActionAggregator.ClusterActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatsRollupConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({GroupClientConfig.class,
        RepositoryClientConfig.class,
        SQLDatabaseConfig.class,
        ActionTranslationConfig.class,
        ActionStatsRollupConfig.class})
public class ActionStatsConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private SQLDatabaseConfig sqlDatabaseConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private ActionStatsRollupConfig rollupConfig;

    @Value("${actionStatsWriteBatchSize}")
    private int actionStatsWriteBatchSize;

    @Bean
    public ClusterActionAggregatorFactory clusterAggregatorFactory() {
        return new ClusterActionAggregatorFactory(groupClientConfig.groupChannel(), repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public GlobalAggregatorFactory globalAggregatorFactory() {
        return new GlobalAggregatorFactory();
    }

    @Bean
    public SingleActionSnapshotFactory snapshotFactory() {
        return new SingleActionSnapshotFactory();
    }

    @Bean
    public ActionGroupStore actionGroupStore() {
        return new ActionGroupStore(sqlDatabaseConfig.dsl());
    }

    @Bean
    public MgmtUnitSubgroupStore mgmtUnitSubgroupStore() {
        return new MgmtUnitSubgroupStore(sqlDatabaseConfig.dsl());
    }

    @Bean
    public LiveActionsStatistician actionsStatistician() {
        return new LiveActionsStatistician(sqlDatabaseConfig.dsl(),
                actionStatsWriteBatchSize,
                actionGroupStore(),
                mgmtUnitSubgroupStore(),
                snapshotFactory(),
                Arrays.asList(globalAggregatorFactory(), clusterAggregatorFactory()),
                Clock.systemUTC(),
                actionTranslationConfig.actionTranslator(),
                rollupConfig.rollupScheduler());
    }
}
