package com.vmturbo.cost.component.topology;

import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeToplogyConextId;

    public LiveTopologyEntitiesListener(long realtimeTopologyContextId) {
        this.realtimeToplogyConextId = realtimeTopologyContextId;
    }

    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {

        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        if (topologyContextId != realtimeToplogyConextId) {
            logger.error("Received topology with wrong topologyContextId. Expected:{}, Received:{}",
                    realtimeToplogyConextId, topologyContextId);
            return;
        }
        logger.info("Received live topology with topologyId: {}", topologyInfo.getTopologyId());
        // TODO - karthikt: For now, this is a stub method. Parsing the topology and
        // storing to DB will come later.
        try {
            while (entityIterator.hasNext()) {
                entityIterator.nextChunk();
            }
        } catch (CommunicationException |TimeoutException ex) {
            logger.error("Error occurred while receiving topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ex);
        } catch (InterruptedException ie) {
            logger.info("Thread interrupted while processing topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ie);
        }
    }
}


