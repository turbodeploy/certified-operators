package com.vmturbo.market.topology;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.runner.MarketRunner;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    private final MarketRunner marketRunner;

    // TODO: we need to make sure that only a single instance of TopologyEntitiesListener
    // be created and used. Using public constructor here can not guarantee it!
    public TopologyEntitiesListener(@Nonnull MarketRunner marketRunner) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {
        final Set<TopologyEntityDTO> entities = new HashSet<>();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        try {
            while (entityIterator.hasNext()) {
                entities.addAll(entityIterator.nextChunk());
            }
            marketRunner.scheduleAnalysis(topologyInfo, entities, false);
        } catch (CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId + " with for " +
                    "context " + topologyContextId, e);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId + " with for " +
                    "context " + topologyContextId, e);
        }
    }
}
