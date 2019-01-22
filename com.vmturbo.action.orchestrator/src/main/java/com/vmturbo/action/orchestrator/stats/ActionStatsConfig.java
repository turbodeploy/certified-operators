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
import com.vmturbo.action.orchestrator.stats.LiveActionStatReader.CombinedStatsBucketsFactory.DefaultBucketsFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ClusterActionAggregator.ClusterActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatsRollupConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.components.api.TimeFrameCalculator;
import com.vmturbo.components.api.TimeFrameCalculator.TimeFrame;
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

    @Value("${actionStatsWriteBatchSize}")
    private int actionStatsWriteBatchSize;

    @Value("${numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${numRetainedHours}")
    private int numRetainedHours;

    @Value("${numRetainedDays}")
    private int numRetainedDays;

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
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(globalConfig.actionOrchestratorClock(),
            numRetainedMinutes, numRetainedHours, numRetainedDays);
    }
    @Bean
    public LiveActionStatReader liveActionStatReader() {
        final Map<TimeFrame, ActionStatTable.Reader> statReadersForTimeFrame = new HashMap<>();
        statReadersForTimeFrame.put(TimeFrame.LATEST, rollupConfig.latestTable().reader());
        statReadersForTimeFrame.put(TimeFrame.HOUR, rollupConfig.hourlyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.DAY, rollupConfig.dailyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.MONTH, rollupConfig.monthlyTable().reader());

        return new LiveActionStatReader(actionGroupStore(),
            mgmtUnitSubgroupStore(),
            timeFrameCalculator(),
            statReadersForTimeFrame,
            new DefaultBucketsFactory());
    }

    @Bean
    public LiveActionsStatistician actionsStatistician() {
        return new LiveActionsStatistician(sqlDatabaseConfig.dsl(),
                actionStatsWriteBatchSize,
                actionGroupStore(),
                mgmtUnitSubgroupStore(),
                snapshotFactory(),
                Arrays.asList(globalAggregatorFactory(), clusterAggregatorFactory()),
                globalConfig.actionOrchestratorClock(),
                actionTranslationConfig.actionTranslator(),
                rollupConfig.rollupScheduler());
    }
}
