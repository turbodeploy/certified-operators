package com.vmturbo.cost.component.savings;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.CostDBConfig;

/**
 * Configuration for cloud savings/investment tracking.  Interesting event types are: entity
 * powered on, entity powered off, entity provider change, and entity deleted.
 */
@Configuration
@Import({ActionOrchestratorClientConfig.class,
        CostComponentGlobalConfig.class,
        CostClientConfig.class})
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

    /**
     * Chunk size configuration.
     */
    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    /*
     * Enable cloud savings tracking.
     */
    @Value("${enableEntitySavings:false}")
    private boolean enableEntitySavings;

    /*
     * The amount of time to retain state in the internal savings event log.
     */
    @Value("${entitySavingsEventLogRetentionHours:2400}")
    private Long entitySavingsEventLogRetentionHours;

    /**
     * Real-Time Context Id.
     */
    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

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
     * Gets a reference to entity state cache.
     *
     * @return EntityState cache to keep track of per entity state in memory.
     */
    @Bean
    public EntityStateCache entityStateCache() {
        // TODO switch to SqlEntityStateStore
        return new InMemoryEntityStateCache();
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
                                                   costService(), realtimeTopologyContextId);
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
        return new TopologyEventsPoller();
    }

    /**
     * Entity Savings Tracker: object responsible for coordinating the generation of entity savings
     * stats.
     *
     * @return singleton instance of EntitySavingsTracker
     */
    @Bean
    public EntitySavingsTracker entitySavingsTracker() {
        return new EntitySavingsTracker(entitySavingsStore(), entityEventsJournal(), entityStateCache());
    }

    /**
     * Task that executes once an hour to process entity events.
     *
     * @return singleton instance of EntitySavingsProcessor
     */
    @Bean
    public EntitySavingsProcessor entitySavingsProcessor() {
        EntitySavingsProcessor entitySavingsProcessor =
                new EntitySavingsProcessor(entitySavingsTracker(), topologyEventsPoller());

        if (isEnabled()) {
            logger.info("EntitySavingsProcessor is enabled.");
            Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                    entitySavingsProcessor::execute, 0, 1, TimeUnit.HOURS);
        } else {
            logger.info("EntitySavingsProcessor is disabled.");
        }

        return entitySavingsProcessor;
    }

    /**
     * Get access to savings DB store.
     *
     * @return Savings store.
     */
    @Bean
    public EntitySavingsStore entitySavingsStore() {
        return new SqlEntitySavingsStore(databaseConfig.dsl(), costComponentGlobalConfig.clock(),
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
        return new InMemoryEntityEventsJournal();
    }

    /**
     * Starts the event injector.
     *
     * @return the EventInjector instance.
     */
    @Bean
    public EventInjector eventInjector() {
        EventInjector injector = new EventInjector(entitySavingsTracker(), entityEventsJournal());
        injector.start();
        return injector;
    }
}
