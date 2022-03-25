package com.vmturbo.cost.component.savings.bottomup;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.savings.tem.TopologyEventsMonitor;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;

/**
 * Converts topology events related to tracked entities into SavingsEvents.
 */
public class EntitySavingsTopologyMonitor implements  LiveCloudTopologyListener {
    private final Logger logger = LogManager.getLogger();

    private final TopologyEventsMonitor topologyEventsMonitor;
    @Nonnull private final EntityStateStore<DSLContext> entityStateStore;
    @Nonnull private final EntityEventsJournal entityEventsJournal;

    /**
     * Constructor.
     *
     * @param topologyEventsMonitor TEM to use.
     * @param entityStateStore entity state store containing state to process
     * @param entityEventsJournal event journal to populate with generated topology events.
     */
    public EntitySavingsTopologyMonitor(@Nonnull TopologyEventsMonitor topologyEventsMonitor,
            @Nonnull EntityStateStore<DSLContext> entityStateStore,
            @Nonnull EntityEventsJournal entityEventsJournal) {
        this.topologyEventsMonitor = Objects.requireNonNull(topologyEventsMonitor);
        this.entityStateStore = Objects.requireNonNull(entityStateStore);
        this.entityEventsJournal = entityEventsJournal;
    }

    /**
     * Generate savings events based on topology changes.
     *
     * @param cloudTopology The cloud topology to process.
     * @param topologyInfo Info about the topology
     */
    @Override
    public void process(CloudTopology cloudTopology, TopologyInfo topologyInfo) {
        try {
            entityStateStore.getAllEntityStates(entityState ->
                entityState.handleTopologyUpdate(topologyEventsMonitor, entityStateStore,
                        entityEventsJournal, topologyInfo.getCreationTime(), cloudTopology));
        } catch (EntitySavingsException e) {
            logger.error("Error processing topology update for topology ID {}: {}",
                    topologyInfo.getTopologyId(), e);
        }
    }
}
