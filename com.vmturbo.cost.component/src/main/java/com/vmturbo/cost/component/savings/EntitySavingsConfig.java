package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.cost.component.CostDBConfig;

/**
 * Configuration for cloud savings/investment tracking.  Interesting event types are: entity
 * powered on, entity powered off, entity provider change, and entity deleted.
 */
@Configuration
@Import({ActionOrchestratorClientConfig.class})
public class EntitySavingsConfig {
    /**
     * DB config for DSL access.
     */
    @Autowired
    private CostDBConfig databaseConfig;

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
        return new InMemoryEntityStateCache();
    }

    /**
     * Create and return an entity savings tracker.
     *
     * @return the entity savings tracker.
     */
    public EntitySavingsTracker entitySavingsTracker() {
       return new EntitySavingsTracker(this, aoClientConfig,
               Executors.newSingleThreadScheduledExecutor());
    }

    /**
     * Get access to savings DB store.
     *
     * @return Savings store.
     */
    @Bean
    public EntitySavingsStore entitySavingsStore() {
        return new SqlEntitySavingsStore(databaseConfig.dsl(), Clock.systemUTC(),
                persistEntityCostChunkSize);
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
}
