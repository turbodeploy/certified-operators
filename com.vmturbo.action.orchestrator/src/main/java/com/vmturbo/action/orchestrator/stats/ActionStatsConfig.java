package com.vmturbo.action.orchestrator.stats;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader.CombinedStatsBucketsFactory.DefaultBucketsFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ClusterActionAggregator.ClusterActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatsRollupConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({GroupClientConfig.class,
        RepositoryClientConfig.class,
        SQLDatabaseConfig.class,
        ActionTranslationConfig.class,
        ActionStatsRollupConfig.class,
        ActionOrchestratorGlobalConfig.class})
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

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    /**
     * Auto-wiring the action store config without an @Import
     * because of circular dependency.
     */
    @Autowired
    private ActionStoreConfig actionStoreConfig;

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
    public StatsActionViewFactory snapshotFactory() {
        return new StatsActionViewFactory();
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
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(globalConfig.actionOrchestratorClock(),
            rollupConfig.retentionPeriodFetcher());
    }

    @Bean
    public HistoricalActionStatReader historicalActionStatReader() {
        final Map<TimeFrame, ActionStatTable.Reader> statReadersForTimeFrame = new HashMap<>();
        statReadersForTimeFrame.put(TimeFrame.LATEST, rollupConfig.latestTable().reader());
        statReadersForTimeFrame.put(TimeFrame.HOUR, rollupConfig.hourlyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.DAY, rollupConfig.dailyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.MONTH, rollupConfig.monthlyTable().reader());

        return new HistoricalActionStatReader(actionGroupStore(),
            mgmtUnitSubgroupStore(),
            timeFrameCalculator(),
            statReadersForTimeFrame,
            new DefaultBucketsFactory());
    }

    @Bean
    public CurrentActionStatReader currentActionStatReader() {
        return new CurrentActionStatReader(globalConfig.realtimeTopologyContextId(),
            actionStoreConfig.actionStorehouse());
    }

    @Bean
    public LiveActionsStatistician actionsStatistician() {
        return new LiveActionsStatistician(sqlDatabaseConfig.dsl(),
                actionStatsWriteBatchSize,
                actionGroupStore(),
                mgmtUnitSubgroupStore(),
                snapshotFactory(),
                // TODO (roman, Mar 8 2019): Re-enable Cluster Aggregator after
                // OM-43498 is solved.
                Arrays.asList(globalAggregatorFactory()),
                globalConfig.actionOrchestratorClock(),
                rollupConfig.rollupScheduler(),
                rollupConfig.cleanupScheduler());
    }
}
