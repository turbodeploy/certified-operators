package com.vmturbo.cost.component.savings;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.rollup.RollupConfig;
import com.vmturbo.cost.component.savings.bottomup.ActionListener;
import com.vmturbo.cost.component.savings.bottomup.AuditLogWriter;
import com.vmturbo.cost.component.savings.bottomup.EntityEventsJournal;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsProcessor;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStore;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsTracker;
import com.vmturbo.cost.component.savings.bottomup.EntityStateStore;
import com.vmturbo.cost.component.savings.bottomup.EventInjector;
import com.vmturbo.cost.component.savings.bottomup.InMemoryEntityEventsJournal;
import com.vmturbo.cost.component.savings.bottomup.SqlAuditLogWriter;
import com.vmturbo.cost.component.savings.bottomup.SqlEntityEventsJournal;
import com.vmturbo.cost.component.savings.bottomup.SqlEntitySavingsStore;
import com.vmturbo.cost.component.savings.bottomup.SqlEntityStateStore;
import com.vmturbo.cost.component.savings.bottomup.TopologyEventsPoller;
import com.vmturbo.cost.component.savings.temold.TopologyEventsMonitor;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.event.library.TopologyEventProvider;

/**
 * Configuration for cloud savings/investment tracking.  Interesting event types are: entity
 * powered on, entity powered off, entity provider change, and entity deleted.
 */
@Configuration
@Import({ActionOrchestratorClientConfig.class,
        CostComponentGlobalConfig.class,
        TopologyProcessorListenerConfig.class,
        RepositoryClientConfig.class,
        GroupClientConfig.class,
        CloudCommitmentAnalysisStoreConfig.class,
        EntityCostConfig.class,
        CostNotificationConfig.class,
        RollupConfig.class})
public class EntitySavingsConfig {

    private final Logger logger = LogManager.getLogger();

    /**
     * DB config for DSL access.
     */
    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Autowired
    private RollupConfig rollupConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private RepositoryClient repositoryClient;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    /**
     * Chunk size configuration.
     */
    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    /**
     * Enable cloud savings tracking.
     */
    @Value("${enableEntitySavings:true}")
    private boolean enableEntitySavings;

    /**
     * How long to wait in hours for a missing entity before declaring it deleted.
     * Default is 1/12 of a year.
     */
    @Value("${entityDeletionPeriodHours:730}")
    private Long entityDeletionPeriodHours;

    /**
     * How long to retain events in audit events DB table - default 1/2 month max.
     */
    @Value("${entitySavingsAuditLogRetentionHours:365}")
    private Long entitySavingsAuditLogRetentionHours;

    /**
     * Whether audit events data needs to be written to DB or not.
     */
    @Value("${entitySavingsAuditLogEnabled:true}")
    private boolean entitySavingsAuditLogEnabled;

    /**
     * Real-Time Context Id.
     */
    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    /**
     * How often to run the retention processor, default 6 hours.
     */
    @Value("${entitySavingsRetentionProcessorFrequencyHours:6}")
    private Long retentionProcessorFrequencyHours;

    /**
     * How often to run the bill based savings processor, default 24 hours.
     */
    @Value("${billSavingsProcessorFrequencyHours:24}")
    private Integer billSavingsProcessorFrequencyHours;

    /**
     * How often to refresh internal savings action store cache, default 24 hours.
     */
    @Value("${savingsActionStoreCacheRefreshHours:24}")
    private Long savingsActionStoreCacheRefreshHours;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private TopologyEventProvider topologyEventProvider;

    @Autowired
    private TopologyInfoTracker liveTopologyInfoTracker;

    @Autowired
    private SearchServiceBlockingStub searchServiceBlockingStub;

    /**
     * Chunk size for savings data processing. This many entities are processed at a time.
     */
    @Value("${savingsDataProcessingChunkSize:100}")
    private int savingsDataProcessingChunkSize;

    /**
     * Default 45 day (1080 hours) retention for savings events in DB.
     */
    private static final long EVENT_RETENTION_DEFAULT_HOURS = 1080;

    /**
     * How long to retain savings events in DB - default 45 days.
     */
    @Value("${entitySavingsEventsRetentionHours:1080}")
    private Long entitySavingsEventsRetentionHours;

    /**
     * How long (minutes) after the hour/day mark to run the periodic hourly processor task.
     * Made configurable for testing only, would not need to be configurable otherwise.
     */
    @Value("${entitySavingsStartMinuteMark:15}")
    private int entitySavingsStartMinuteMark;

    /**
     * How long to wait after cost component startup before triggering the action cache
     * initializer thread run for the first time. We wait a few mins after startup for things to
     * stabilize before starting the sync with AO.
     */
    @Value("${savingsActionCacheInitDelayMinutes:3}")
    private int savingsActionCacheInitDelayMinutes;

    /**
     * How often to check for action change window cache initialization. Every this many minutes,
     * the initializer thread will check if the cache is ready (initialized successfully), if not,
     * it will query the AO and get the cache in sync with AO.
     */
    @Value("${savingsActionCacheInitPeriodMinutes:5}")
    private int savingsActionCacheInitPeriodMinutes;

    /**
     * Whether dump file option for savings action cache is enabled, default no.
     */
    @Value("${savingsActionCacheDumpEnabled:false}")
    private boolean savingsActionCacheDumpEnabled;

    /**
     * Entity types (cloud only) for which Savings feature is currently supported.
     */
    private static final Set<EntityType> supportedEntityTypes = Stream.concat(
            TopologyDTOUtil.WORKLOAD_TYPES.stream(),
            Stream.of(EntityType.VIRTUAL_VOLUME)).collect(Collectors.toSet());

    /**
     * Provider types we care about for bill based savings.
     */
    @VisibleForTesting
    static final Set<Integer> supportedProviderTypes = ImmutableSet.of(
            EntityType.COMPUTE_TIER.getNumber(),
            EntityType.STORAGE_TIER.getNumber(),
            EntityType.DATABASE_TIER.getNumber(),
            EntityType.DATABASE_SERVER_TIER.getNumber());

    /**
     * Types of actions we currently support Savings feature for.
     */
    private static final Set<ActionType> supportedActionTypes =
            ImmutableSet.of(ActionType.SCALE, ActionType.DELETE, ActionType.ALLOCATE);

    /**
     * Return whether entity savings tracking is enabled.
     * @return True if entity savings tracking is enabled.
     */
    public boolean isEnabled() {
        return this.enableEntitySavings && !isBillSavingsEnabled();
    }

    /**
     * Whether bill based (newer) or bottom-up based (older) savings type is enabled.
     * @return True if bill based is enabled.
     */
    public boolean isBillSavingsEnabled() {
        return FeatureFlags.ENABLE_BILLING_BASED_SAVINGS.isEnabled()
                && FeatureFlags.EXECUTED_ACTIONS_CHANGE_WINDOW.isEnabled();
    }

    /**
     * Gets Settings Service Client.
     *
     * @return Settings Service Client.
     */
    @Bean
    public SettingServiceGrpc.SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Get savings retention configuration.
     *
     * @return Action executed lifetime configuration information.
     */
    @Bean
    public EntitySavingsRetentionConfig getEntitySavingsRetentionConfig() {
        if (entitySavingsEventsRetentionHours < 0) {
            logger.warn("Invalid entitySavingsEventsRetentionHours value {}, defaulting to {} hours.",
                    entitySavingsEventsRetentionHours, EVENT_RETENTION_DEFAULT_HOURS);
            entitySavingsEventsRetentionHours = EVENT_RETENTION_DEFAULT_HOURS;
        }
        return new EntitySavingsRetentionConfig(settingServiceClient(),
                entitySavingsAuditLogRetentionHours, entitySavingsEventsRetentionHours);
    }

    /**
     * Gets a reference to entity state cache.
     *
     * @return EntityState cache to keep track of per entity state in memory.
     */
    @Bean
    public EntityStateStore<DSLContext> entityStateStore() {
        try {
            return new SqlEntityStateStore(dbAccessConfig.dsl(), persistEntityCostChunkSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntityStateStore bean", e);
        }
    }

    /**
     * Get the realtime topology contextId.
     *
     * @return the realtime topology contextId
     */
    @Bean
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    /**
     * Action listener: Listen for action events and insert records in event journal.
     *
     * @return singleton instance of action listener
     */
    @Bean
    public ActionListener actionListener() {
        ActionListener actionListener = new ActionListener(entityEventsJournal(), actionsService(),
                entityCostConfig.entityCostStore(),
                entityCostConfig.projectedEntityCostStore(),
                realtimeTopologyContextId,
                supportedEntityTypes, supportedActionTypes,
                getEntitySavingsRetentionConfig(),
                entitySavingsStore(),
                entityStateStore(),
                rollupConfig.entitySavingsRollupTimesStore(),
                getClock());
        if (isEnabled()) {
            logger.info("Registering action listener with AO to receive action events.");
            // Register listener with the action orchestrator to receive action events.
            aoClientConfig.actionOrchestratorClient().addListener(actionListener);
        } else {
            logger.info("Action listener is disabled because Entity Savings feature is disabled.");
        }
        return actionListener;
    }

    /**
     * Topology Events Poller: gets topology events.
     *
     * @return singleton instance of TopologyEventsPoller
     */
    @Bean
    public TopologyEventsPoller topologyEventsPoller() {
        return new TopologyEventsPoller(topologyEventProvider, liveTopologyInfoTracker,
                entityEventsJournal());
    }

    /**
     * Entity Savings Tracker: object responsible for coordinating the generation of entity savings
     * stats.
     *
     * @return singleton instance of EntitySavingsTracker
     */
    @Bean
    public EntitySavingsTracker entitySavingsTracker() {
        try {
            return new EntitySavingsTracker(entitySavingsStore(), entityEventsJournal(),
                    entityStateStore(), getClock(), cloudTopologyFactory(), repositoryClient,
                    dbAccessConfig.dsl(), realtimeTopologyContextId, persistEntityCostChunkSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntitySavingsTracker bean", e);
        }
    }

    /**
     * Task that executes once an hour to process entity events.
     *
     * @return singleton instance of EntitySavingsProcessor
     */
    @Bean
    public EntitySavingsProcessor entitySavingsProcessor() {
        EntitySavingsProcessor entitySavingsProcessor =
                new EntitySavingsProcessor(entitySavingsTracker(), topologyEventsPoller(),
                        rollupSavingsProcessor(), rollupConfig.entitySavingsRollupTimesStore(),
                        entitySavingsStore(), entityEventsJournal(), getClock(),
                        dataRetentionProcessor(), costNotificationConfig.costNotificationSender());

        if (isEnabled()) {
            int initialDelayMinutes = getInitialStartDelayMinutes();
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    entitySavingsProcessor::execute, initialDelayMinutes, 60, TimeUnit.MINUTES);
            String temConfigInfo = "disabled";
            if (FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
                temConfigInfo = String.format("enabled. entityDeletionPeriodHours = %d",
                        entityDeletionPeriodHours);
            }
            logger.info("EntitySavingsProcessor is enabled, will run at hour+{} min, after {} mins. TEM is {}.",
                    entitySavingsStartMinuteMark, initialDelayMinutes, temConfigInfo);
            if (FeatureFlags.ENABLE_SAVINGS_TEST_INPUT.isEnabled()) {
                startEventInjector();
            }
        } else {
            logger.info("EntitySavingsProcessor is disabled.");
        }

        return entitySavingsProcessor;
    }

    /**
     * Gets the processor that cleans up old stats/audit data.
     *
     * @return DataRetentionProcessor.
     */
    @Bean
    public DataRetentionProcessor dataRetentionProcessor() {
        final EntityEventsJournal eventsJournal = entityEventsJournal();
        return new DataRetentionProcessor(entitySavingsStore(),
                eventsJournal.persistEvents() ? null : auditLogWriter(),
                getEntitySavingsRetentionConfig(), getClock(), retentionProcessorFrequencyHours,
                eventsJournal.persistEvents() ? eventsJournal : null);
    }

    /**
     * Gets how many minutes to wait from now before triggering off the entity savings processor
     * task the first time. E.g if current time is 10:48, then we wait 17 mins, so that the
     * savings processor kicks off at 11:05, and every hour after that.
     *
     * @return Minutes to wait.
     */
    int getInitialStartDelayMinutes() {
        final LocalTime now = LocalTime.now();
        int currentMinute = now.getMinute();
        return currentMinute <= entitySavingsStartMinuteMark
                ? (entitySavingsStartMinuteMark - currentMinute)
                : (60 - currentMinute + entitySavingsStartMinuteMark);
    }

    /**
     * Get access to savings DB store.
     *
     * @return Savings store.
     */
    @Bean
    public EntitySavingsStore<DSLContext> entitySavingsStore() {
        try {
            return new SqlEntitySavingsStore(dbAccessConfig.dsl(), getClock(),
                    persistEntityCostChunkSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntitySavingsStore bean", e);
        }
    }

    /**
     * Gets Actions information.
     *
     * @return ActionsServiceBlockingStub.
     */
    @Bean
    public ActionsServiceBlockingStub actionsService() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
    }

    /**
     * Gets access to events store.
     *
     * @return Events store.
     */
    @Bean
    public EntityEventsJournal entityEventsJournal() {
        try {
            return FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()
                    ? new SqlEntityEventsJournal(dbAccessConfig.dsl(), persistEntityCostChunkSize)
                    : new InMemoryEntityEventsJournal(auditLogWriter());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntityEventsJournal bean", e);
        }
    }

    /**
     * Starts the bottom-up event injector.
     */
    private void startEventInjector() {
        new DataInjectionMonitor("injected-events.json",
                new EventInjector(entityEventsJournal(), getEntitySavingsRetentionConfig()),
                entitySavingsTracker(), searchServiceBlockingStub).start();
    }

    /**
     * Starts the bill based data injector.
     */
    public void startBillDataInjector() {
        new DataInjectionMonitor("injected-data.json",
                new BillingDataInjector(), savingsTracker(), searchServiceBlockingStub).start();
    }

    /**
     * Get instance of rollup processor.
     *
     * @return Rollup processor.
     */
    @Bean
    public RollupSavingsProcessor rollupSavingsProcessor() {
        return new RollupSavingsProcessor(entitySavingsStore(),
                rollupConfig.entitySavingsRollupTimesStore(), getClock());
    }

    /**
     * Gets the audit log writer.
     *
     * @return Audit log writer.
     */
    @Bean
    public AuditLogWriter auditLogWriter() {
        try {
            return new SqlAuditLogWriter(dbAccessConfig.dsl(), getClock(),
                    persistEntityCostChunkSize, isEnabled() && entitySavingsAuditLogEnabled);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create AuditLogWriter bean", e);
        }
    }

    /**
     * Gets UTC clock to use.
     *
     * @return Clock.
     */
    Clock getClock() {
        return costComponentGlobalConfig.clock();
    }

    /**
     * Supported entity types.
     *
     * @return Set of supported types.
     */
    @Nonnull
    public static Set<EntityType> getSupportedEntityTypes() {
        return supportedEntityTypes;
    }

    /**
     * Supported action types - SCALE, and DELETE volumes later.
     * @return Set of supported actions.
     */
    @Nonnull
    public static Set<ActionType> getSupportedActionTypes() {
        return supportedActionTypes;
    }

    /**
     * Gets Cloud Topology Factory.
     *
     * @return Cloud Topology Factory.
     */
    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                groupClientConfig.groupMemberRetriever());
    }

    /**
     * Creates the topology events monitor.
     *
     * @return topology events monitor.
     */
    @Bean
    public TopologyEventsMonitor topologyEventsMonitor() {
        return new TopologyEventsMonitor();
    }

    /**
     * Creates tracker for bill based savings.
     *
     * @return Bill based savings tracker.
     */
    @Bean
    public SavingsTracker savingsTracker() {
        try {
            return new SavingsTracker(new SqlBillingRecordStore(dbAccessConfig.dsl()),
                    new GrpcActionChainStore(actionsService()),
                    (SavingsStore)entitySavingsStore(), supportedProviderTypes,
                    getEntitySavingsRetentionConfig().getVolumeDeleteRetentionMs(), getClock());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create billing SavingsTracker bean", e);
        }
    }

    /**
     * Get instance of savings action store. Calls refresh first time if bill based savings is
     * enabled and we are doing action change window tracking.
     *
     * @return Instance of savings action store.
     */
    @Bean
    public SavingsActionStore savingsActionStore() {
        final CachedSavingsActionStore store = new CachedSavingsActionStore(actionsService(), getClock(),
                savingsActionStoreCacheRefreshHours, savingsDataProcessingChunkSize,
                savingsActionCacheInitDelayMinutes, savingsActionCacheInitPeriodMinutes,
                savingsActionCacheDumpEnabled);
        if (isBillSavingsEnabled()) {
            store.initialize(true);
        }
        return store;
    }

    /**
     * Creates bill based savings processor.
     *
     * @return Bill based savings processor.
     */
    @Bean
    public SavingsProcessor savingsProcessor() {
        SavingsProcessor savingsProcessor =
                new SavingsProcessor(getClock(), savingsDataProcessingChunkSize,
                        rollupConfig.entitySavingsRollupTimesStore(), rollupSavingsProcessor(),
                        (StateStore)entityStateStore(), savingsTracker(),
                        dataRetentionProcessor());
        if (isBillSavingsEnabled()) {
            int durationMinutes = 60 * billSavingsProcessorFrequencyHours;
            final LocalDateTime now = LocalDateTime.now();
            int totalMinutes = now.getHour() * 60 + now.getMinute();
            int waitMinutes = durationMinutes - (totalMinutes % durationMinutes)
                    + entitySavingsStartMinuteMark;
            if (waitMinutes > durationMinutes) {
                waitMinutes -= durationMinutes;
            }
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    savingsProcessor::execute, waitMinutes, durationMinutes, TimeUnit.MINUTES);
            logger.info("BillSavingsProcessor will run every {}h (+ {}m), next after {}m.",
                    billSavingsProcessorFrequencyHours, entitySavingsStartMinuteMark, waitMinutes);
            if (FeatureFlags.ENABLE_SAVINGS_TEST_INPUT.isEnabled()) {
                startBillDataInjector();
            }
        } else {
            logger.info("BillSavingsProcessor is disabled.");
        }
        return savingsProcessor;
    }

    /**
     * Creates executed actions listener.
     *
     * @return Executed actions listener.
     */
    @Bean
    public ExecutedActionsListener executedActionsListener() {
        final ExecutedActionsListener listener = new ExecutedActionsListener(
                (StateStore)entityStateStore(), supportedEntityTypes, savingsActionStore());
        if (isBillSavingsEnabled()) {
            logger.info("Registered BillExecutedActionsListener for executed actions.");
            aoClientConfig.actionOrchestratorClient().addListener(listener);
        } else {
            logger.info("BillExecutedActionsListener is disabled.");
        }
        return listener;
    }
}
