package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalTime;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.event.library.TopologyEventProvider;

/**
 * Configuration for cloud savings/investment tracking.  Interesting event types are: entity
 * powered on, entity powered off, entity provider change, and entity deleted.
 */
@Configuration
@Import({ActionOrchestratorClientConfig.class,
        CostComponentGlobalConfig.class,
        CostClientConfig.class,
        TopologyProcessorListenerConfig.class,
        RepositoryClientConfig.class,
        GroupClientConfig.class,
        CloudCommitmentAnalysisStoreConfig.class})
public class EntitySavingsConfig {

    private final Logger logger = LogManager.getLogger();

    /**
     * DB config for DSL access.
     */
    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private RepositoryClient repositoryClient;

    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * Chunk size configuration.
     */
    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    /**
     * Enable cloud savings tracking.
     */
    @Value("${enableEntitySavings:false}")
    private boolean enableEntitySavings;

    /**
     * The amount of time to retain state in the internal savings event log.
     */
    @Value("${entitySavingsEventLogRetentionHours:2400}")
    private Long entitySavingsEventLogRetentionHours;

    /**
     * Action expiration durations in hours.  These must be integral values.
     */
    @Value("${deleteVolumeActionLifetimeHours:8760}") // Default is 365 days (approximately 1 year)
    private Long deleteVolumeActionLifetimeHours;

    @Value("${defaultActionLifetimeHours:17520}")     // Default is 730 days (approximately 2 years)
    private Long actionLifetimeHours;

    /**
     * Real-Time Context Id.
     */
    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private TopologyEventProvider topologyEventProvider;

    @Autowired
    private TopologyInfoTracker liveTopologyInfoTracker;

    /**
     * How long (minutes) after the hour mark to run the periodic hourly processor task.
     */
    private static final int startMinuteMark = 15;

    /**
     * Entity types (cloud only) for which Savings feature is currently supported.
     */
    private static final Set<EntityType> supportedEntityTypes = Stream.concat(
            TopologyDTOUtil.WORKLOAD_TYPES.stream(),
            Stream.of(EntityType.VIRTUAL_VOLUME)).collect(Collectors.toSet());

    /**
     * Types of actions we currently support Savings feature for.
     */
    private static final Set<ActionType> supportedActionTypes =
            ImmutableSet.of(ActionType.SCALE, ActionType.DELETE);

    /**
     * Return whether entity savings tracking is enabled.
     * @return True if entity savings tracking is enabled.
     */
    public boolean isEnabled() {
        return this.enableEntitySavings;
    }

    /**
     * Return how long to retain events in the internal event log.  As external action and topology
     * events are handled, they are added to the internal event log.  After the events are
     * processed, they are retained for a configurable number of minutes.  The events are no longer
     * needed after they are processed, and a configured retention amount of 0 will cause them to be
     * purged immediately after they are processed.
     *
     * @return Amount of time in minutes to retain internal events.
     */
    public Long getEntitySavingsEventLogRetentionMinutes() {
        return this.entitySavingsEventLogRetentionHours;
    }

    /**
     * Get the configured action lifetime in milliseconds. All active actions except for volume
     * delete actions will remain active until it is reversed by a subsequent action or the
     * configured interval passes, whichever comes first.
     *
     * @return maximum number of milliseconds that an action can stay active.
     */
    public Long getActionLifetimeMs() {
        return TimeUnit.HOURS.toMillis(this.actionLifetimeHours);
    }

    /**
     * Get the configured volume delete action lifetime in milliseconds.  The volume delete action
     * will remain active until the configured interval passes.
     *
     * @return maximum number of milliseconds that delete volume action can stay active.
     */
    public Long getDeleteVolumeActionLifetimeMs() {
        return TimeUnit.HOURS.toMillis(this.deleteVolumeActionLifetimeHours);
    }

    /**
     * Gets a reference to entity state cache.
     *
     * @return EntityState cache to keep track of per entity state in memory.
     */
    @Bean
    public EntityStateStore entityStateStore() {
        return new SqlEntityStateStore(databaseConfig.dsl(), persistEntityCostChunkSize);
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
                costService(), realtimeTopologyContextId,
                supportedEntityTypes, supportedActionTypes,
                getActionLifetimeMs(), getDeleteVolumeActionLifetimeMs());
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
                entityEventsJournal(), getActionLifetimeMs(), getDeleteVolumeActionLifetimeMs());
    }

    /**
     * Entity Savings Tracker: object responsible for coordinating the generation of entity savings
     * stats.
     *
     * @return singleton instance of EntitySavingsTracker
     */
    @Bean
    public EntitySavingsTracker entitySavingsTracker() {
        return new EntitySavingsTracker(entitySavingsStore(), entityEventsJournal(), entityStateStore(),
                getClock(), cloudTopologyFactory(), repositoryClient,
                realtimeTopologyContextId, persistEntityCostChunkSize);
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
                        rollupSavingsProcessor(), entitySavingsStore(), entityEventsJournal(), getClock());

        if (isEnabled()) {
            int initialDelayMinutes = getInitialStartDelayMinutes();
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    entitySavingsProcessor::execute, initialDelayMinutes, 60, TimeUnit.MINUTES);
            logger.info("EntitySavingsProcessor is enabled, will run at hour+{} min, after {} mins.",
                    startMinuteMark, initialDelayMinutes);
        } else {
            logger.info("EntitySavingsProcessor is disabled.");
        }

        return entitySavingsProcessor;
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
        return currentMinute <= startMinuteMark
                ? (startMinuteMark - currentMinute)
                : (60 - currentMinute + startMinuteMark);
    }

    /**
     * Get access to savings DB store.
     *
     * @return Savings store.
     */
    @Bean
    public EntitySavingsStore entitySavingsStore() {
        return new SqlEntitySavingsStore(getDslContext(), getClock(),
                persistEntityCostChunkSize);
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
     * Gets Cost information.
     *
     * @return CostServiceBlockingStub.
     */
    @Bean
    public CostServiceBlockingStub costService() {
        return CostServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Gets access to events store.
     *
     * @return Events store.
     */
    @Bean
    public EntityEventsJournal entityEventsJournal() {
        return new InMemoryEntityEventsJournal(auditLogWriter());
    }

    /**
     * Starts the event injector.
     *
     * @return the EventInjector instance.
     */
    @Bean
    public EventInjector eventInjector() {
        EventInjector injector = new EventInjector(entitySavingsTracker(), entityEventsJournal(),
                getActionLifetimeMs(), getDeleteVolumeActionLifetimeMs());
        injector.start();
        return injector;
    }

    /**
     * Get instance of rollup processor.
     *
     * @return Rollup processor.
     */
    @Bean
    public RollupSavingsProcessor rollupSavingsProcessor() {
        return new RollupSavingsProcessor(entitySavingsStore(), getClock());
    }

    /**
     * Gets the audit log writer.
     *
     * @return Audit log writer.
     */
    @Bean
    public AuditLogWriter auditLogWriter() {
        return new SqlAuditLogWriter(getDslContext(), getClock(),
                persistEntityCostChunkSize);
    }

    /**
     * DB DSL context.
     *
     * @return DSL context.
     */
    DSLContext getDslContext() {
        return databaseConfig.dsl();
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
    static Set<EntityType> getSupportedEntityTypes() {
        return supportedEntityTypes;
    }

    /**
     * Supported action types - SCALE, and DELETE volumes later.
     * @return Set of supported actions.
     */
    @Nonnull
    static Set<ActionType> getSupportedActionTypes() {
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
}
