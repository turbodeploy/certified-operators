package com.vmturbo.action.orchestrator.stats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.DbAccessConfig;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader.CombinedStatsBucketsFactory.DefaultBucketsFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.BusinessAccountActionAggregator.BusinessAccountActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ClusterActionAggregator.ClusterActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.PropagatedActionAggregator.PropagatedActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ResourceGroupActionAggregator.ResourceGroupActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.RollupDirection;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.DayActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.HourActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.IActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRollupDirection;
import com.vmturbo.action.orchestrator.stats.rollup.LatestActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.MonthActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;
import com.vmturbo.action.orchestrator.stats.rollup.v2.ActionStatRollupSchedulerV2;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

@Configuration
@Import({GroupClientConfig.class,
        RepositoryClientConfig.class,
        DbAccessConfig.class,
        ActionTranslationConfig.class,
        ActionOrchestratorApiConfig.class,
        ActionOrchestratorGlobalConfig.class,
        TopologyProcessorConfig.class,
        UserSessionConfig.class})
public class ActionStatsConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private ActionOrchestratorApiConfig apiConfig;

    /**
     * Auto-wiring the action store config without an @Import
     * because of circular dependency.
     */
    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Value("${actionStatsWriteBatchSize:500}")
    private int actionStatsWriteBatchSize;

    @Value("${actionStatRollup.corePoolSize:1}")
    private int rollupCorePoolSize;

    @Value("${actionStatRollup.maxPoolSize:4}")
    private int rollupMaxPoolSize;

    @Value("${actionStatRollup.threadKeepAliveMins:1}")
    private int rollupThreadKeepAliveMins;

    @Value("${actionStatRollup.executorQueueSize:10}")
    private int rollupExecutorQueueSize;

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${actionStatCleanup.minTimeBetweenCleanupsMinutes:60}")
    private int minTimeBetweenCleanupsMinutes;

    @Value("${actionStatRollup.corePoolSize:1}")
    private int cleanupCorePoolSize;

    @Value("${actionStatRollup.maxPoolSize:10}")
    private int cleanupMaxPoolSize;

    @Value("${actionStatRollup.threadKeepAliveMins:1}")
    private int cleanupThreadKeepAliveMins;

    @Value("${actionStatRollup.executorQueueSize:100}")
    private int cleanupExecutorQueueSize;

    /**
     * If true, reporting is enabled in the system.
     * Note - these are shared with the extractor, and used to control whether or not we export
     * rollups for the extractor's consumption.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    /**
     * If true, reporting action ingestion is enabled in the system.
     * Note - these are shared with the extractor, and used to control whether or not we export
     * rollups for the extractor's consumption.
     */
    @Value("${enableActionIngestion:true}")
    private boolean enableActionIngestion;

    @Value("${enableNewActionStatRollups:false}")
    private boolean enableNewActionStatRollups;

    /**
     * If true, rollup exports are enabled in the system. The action orchestrator will broadcast
     * hourly rollups onto a Kafka topic.
     *
     * <p/>This is true by default (rollup export is controlled by the reporting feature flags),
     * but can be set to false in case of emergency to disable the feature.
     */
    @Value("${enableRollupExport:true}")
    private boolean enableRollupExport;

    /**
     * The number of entities to process at a time. This limits the amount of memory consumed by
     * the telemetry gauge update logic. Lower values decrease memory usage at the cost of increased
     * gRPC calls.
     */
    @Value("${telemetry.chunkSize:50000}")
    private int telemetryChunkSize;

    @Bean
    public ClusterActionAggregatorFactory clusterAggregatorFactory() {
        return new ClusterActionAggregatorFactory(groupClientConfig.groupChannel(),
                tpConfig.actionTopologyStore(), new SupplyChainCalculator());
    }

    /**
     * Factory for business account aggregators.
     *
     * @return The {@link BusinessAccountActionAggregatorFactory}.
     */
    @Bean
    public BusinessAccountActionAggregatorFactory businessAccountActionAggregatorFactory() {
        return new BusinessAccountActionAggregatorFactory();
    }

    /**
     * Factory for resource group aggregators.
     *
     * @return The {@link ResourceGroupActionAggregatorFactory}.
     */
    @Bean
    public ResourceGroupActionAggregatorFactory resourceGroupActionAggregatorFactory() {
        return new ResourceGroupActionAggregatorFactory();
    }

    /**
     * Factory for global aggregators.
     *
     * @return The {@link GlobalAggregatorFactory}.
     */
    @Bean
    public GlobalAggregatorFactory globalAggregatorFactory() {
        return new GlobalAggregatorFactory();
    }

    /**
     * Factory for propagated aggregators.
     *
     * @return The {@link PropagatedActionAggregatorFactory}.
     */
    @Bean
    public PropagatedActionAggregatorFactory propagatedActionsAggregatorFactory() {
        return new PropagatedActionAggregatorFactory(involvedEntitiesExpander());
    }

    @Bean
    public StatsActionViewFactory snapshotFactory() {
        return new StatsActionViewFactory();
    }

    @Bean
    public ActionGroupStore actionGroupStore() {
        try {
            return new ActionGroupStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create actionGroupStore", e);
        }
    }

    @Bean
    public MgmtUnitSubgroupStore mgmtUnitSubgroupStore() {
        try {
            return new MgmtUnitSubgroupStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create mgmtUnitSubgroupStore", e);
        }
    }

    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(globalConfig.actionOrchestratorClock(),
            retentionPeriodFetcher());
    }

    @Bean
    public HistoricalActionStatReader historicalActionStatReader() {
        final Map<TimeFrame, ActionStatTable.Reader> statReadersForTimeFrame = new HashMap<>();
        statReadersForTimeFrame.put(TimeFrame.LATEST, latestTable().reader());
        statReadersForTimeFrame.put(TimeFrame.HOUR, hourlyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.DAY, dailyTable().reader());
        statReadersForTimeFrame.put(TimeFrame.MONTH, monthlyTable().reader());

        return new HistoricalActionStatReader(actionGroupStore(),
            mgmtUnitSubgroupStore(),
            timeFrameCalculator(),
            statReadersForTimeFrame,
            new DefaultBucketsFactory());
    }

    /**
     * Bean for {@link CurrentActionStatReader}.
     * @return The {@link CurrentActionStatReader}.
     */
    @Bean
    public CurrentActionStatReader currentActionStatReader() {
        return new CurrentActionStatReader(tpConfig.realtimeTopologyContextId(),
            actionStoreConfig.actionStorehouse(), userSessionConfig.userSessionContext(),
            involvedEntitiesExpander());
    }

    @Bean
    public LiveActionsStatistician actionsStatistician() {
        try {
            return new LiveActionsStatistician(dbAccessConfig.dsl(),
                    actionStatsWriteBatchSize,
                    actionGroupStore(),
                    mgmtUnitSubgroupStore(),
                    snapshotFactory(),
                    Arrays.asList(globalAggregatorFactory(), clusterAggregatorFactory(),
                            businessAccountActionAggregatorFactory(), resourceGroupActionAggregatorFactory(),
                            propagatedActionsAggregatorFactory()),
                    globalConfig.actionOrchestratorClock(),
                    rollupScheduler(),
                    cleanupScheduler());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create actionsStatistician", e);
        }
    }

    /**
     * Returns the {@link InvolvedEntitiesExpander} configured with the remote supply chain
     * and repository services.
     *
     * @return the {@link InvolvedEntitiesExpander} configured with the remote supply chain
     *         and repository services.
     */
    @Bean
    public InvolvedEntitiesExpander involvedEntitiesExpander() {
        return new InvolvedEntitiesExpander(tpConfig.actionTopologyStore(), new SupplyChainCalculator());
    }

    /**
     * Captures completed action statistic rollups and sends them to Kafka for export.
     *
     * @return The {@link RollupExporter}.
     */
    @Bean
    public RollupExporter rollupExporter() {
        return new RollupExporter(apiConfig.rollupNotificationSender(),
                mgmtUnitSubgroupStore(),
                actionGroupStore(),
                globalConfig.actionOrchestratorClock(),
                enableRollupExport && enableActionIngestion && enableReporting);
    }

    @Bean
    public IActionStatRollupScheduler rollupScheduler() {
        try {
            if (enableNewActionStatRollups) {
                return new ActionStatRollupSchedulerV2(dbAccessConfig.dsl(),
                        globalConfig.actionOrchestratorClock(),
                        rollupExporter(),
                        rolledUpStatCalculator());
            } else {
                final List<RollupDirection> rollupDependencies = new ArrayList<>();
                rollupDependencies.add(ImmutableRollupDirection.builder().fromTableReader(latestTable().reader()).toTableWriter(hourlyTable().writer()).exporter(
                        rollupExporter()).description("latest to hourly").build());
                rollupDependencies.add(ImmutableRollupDirection.builder()
                                                               .fromTableReader(hourlyTable().reader())
                                                               .toTableWriter(dailyTable().writer())
                                                               .description("hourly to daily")
                                                               .build());
                rollupDependencies.add(ImmutableRollupDirection.builder()
                                                               .fromTableReader(dailyTable().reader())
                                                               .toTableWriter(monthlyTable().writer())
                                                               .description("daily to monthly")
                                                               .build());
                return new ActionStatRollupScheduler(rollupDependencies, dbAccessConfig.dsl(),
                        globalConfig.actionOrchestratorClock(), rollupExecutorService());
            }
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create rollupScheduler", e);
        }
    }

    @Bean
    public ActionStatCleanupScheduler cleanupScheduler() {
        return new ActionStatCleanupScheduler(globalConfig.actionOrchestratorClock(),
                Arrays.asList(latestTable(), hourlyTable(), dailyTable(), monthlyTable()),
                retentionPeriodFetcher(),
                cleanupExecutorService(),
                minTimeBetweenCleanupsMinutes, TimeUnit.MINUTES);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService cleanupExecutorService() {
        return new ThreadPoolExecutor(cleanupCorePoolSize,
                cleanupMaxPoolSize,
                cleanupThreadKeepAliveMins,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(cleanupExecutorQueueSize),
                new ThreadFactoryBuilder()
                        .setNameFormat("action-cleanup-thread-%d")
                        .setDaemon(true)
                        .build(),
                new CallerRunsPolicy());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService rollupExecutorService() {
        return new ThreadPoolExecutor(rollupCorePoolSize,
                rollupMaxPoolSize,
                rollupThreadKeepAliveMins,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(rollupExecutorQueueSize),
                new ThreadFactoryBuilder()
                        .setNameFormat("action-rollup-thread-%d")
                        .setDaemon(true)
                        .build(),
                new CallerRunsPolicy());
    }

    @Bean
    public LatestActionStatTable latestTable() {
        try {
            return new LatestActionStatTable(dbAccessConfig.dsl(),
                    globalConfig.actionOrchestratorClock(),
                    rolledUpStatCalculator(), HourActionStatTable.HOUR_TABLE_INFO);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create latestTable", e);
        }
    }

    @Bean
    public HourActionStatTable hourlyTable() {
        try {
            return new HourActionStatTable(dbAccessConfig.dsl(),
                    globalConfig.actionOrchestratorClock(),
                    rolledUpStatCalculator(), DayActionStatTable.DAY_TABLE_INFO);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create hourlyTable", e);
        }
    }

    @Bean
    public DayActionStatTable dailyTable() {
        try {
            return new DayActionStatTable(dbAccessConfig.dsl(),
                    globalConfig.actionOrchestratorClock(),
                    rolledUpStatCalculator(), MonthActionStatTable.MONTH_TABLE_INFO);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create dailyTable", e);
        }
    }

    @Bean
    public MonthActionStatTable monthlyTable() {
        try {
            return new MonthActionStatTable(dbAccessConfig.dsl(),
                    globalConfig.actionOrchestratorClock());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create monthlyTable", e);
        }
    }

    @Bean
    public RolledUpStatCalculator rolledUpStatCalculator() {
        return new RolledUpStatCalculator();
    }

    /**
     * This may not be the best place for this bean, since it's not strictly rollup-specific.
     * But leaving it here for now, because it's needed by the cleanup scheduler.
     */
    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(globalConfig.actionOrchestratorClock(),
                updateRetentionIntervalSeconds, TimeUnit.SECONDS,
                numRetainedMinutes, SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    /**
     * Return the number of entities to process at a time when updating telemetry gauges.
     *
     * @return number of entities to process per chunk.
     */
    public int telemetryChunkSize() {
        return telemetryChunkSize;
    }
}
