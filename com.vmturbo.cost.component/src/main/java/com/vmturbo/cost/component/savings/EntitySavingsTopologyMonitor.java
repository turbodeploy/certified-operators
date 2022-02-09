package com.vmturbo.cost.component.savings;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;

/**
 * Converts topology events related to tracked entities into SavingsEvents.
 */
public class EntitySavingsTopologyMonitor implements  LiveCloudTopologyListener {
    private final Logger logger = LogManager.getLogger();

    private final TopologyEventsMonitor topologyEventsMonitor;
    @Nonnull private final EntityStateStore entityStateStore;
    @Nonnull private final EntityEventsJournal entityEventsJournal;

    /**
     * Constructor.
     *
     * @param topologyEventsMonitor TEM to use.
     * @param entityStateStore entity state store containing state to process
     * @param entityEventsJournal event journal to populate with generated topology events.
     */
    public EntitySavingsTopologyMonitor(@Nonnull TopologyEventsMonitor topologyEventsMonitor,
            @Nonnull EntityStateStore entityStateStore, EntityEventsJournal entityEventsJournal) {
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
        Stream entityStateStream = null;
        try {
            entityStateStream = entityStateStore.getAllEntityStates();
        } catch (com.vmturbo.cost.component.savings.EntitySavingsException e) {
            logger.error("Error processing topology update for topology ID {}: {}",
                    topologyInfo.getTopologyId(), e);
            return;
        }
        entityStateStream.forEach(state -> {
            EntityState entityState = (EntityState)state;
            entityState.handleTopologyUpdate(topologyEventsMonitor,
                    entityStateStore, entityEventsJournal, topologyInfo.getCreationTime(),
                    cloudTopology);
        });
    }
}
