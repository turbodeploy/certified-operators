package com.vmturbo.cost.component.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeToplogyConextId;

    private final TopologyCostCalculator topologyCostCalculator;

    private final EntityCostStore entityCostStore;

    public LiveTopologyEntitiesListener(final long realtimeTopologyContextId,
                                        @Nonnull final TopologyCostCalculator topologyCostCalculator,
                                        @Nonnull final EntityCostStore entityCostStore) {
        this.realtimeToplogyConextId = realtimeTopologyContextId;
        this.topologyCostCalculator = Objects.requireNonNull(topologyCostCalculator);
        this.entityCostStore = Objects.requireNonNull(entityCostStore);
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
        Map<Long, TopologyEntityDTO> cloudEntities = null;
        try {
            cloudEntities = readCloudEntities(entityIterator);
        } catch (CommunicationException |TimeoutException ex) {
            logger.error("Error occurred while receiving topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ex);
            cloudEntities = Collections.emptyMap();
        } catch (InterruptedException ie) {
            logger.info("Thread interrupted while processing topology:{}, topologyContext:{}",
                    topologyId, topologyContextId, ie);
            cloudEntities = Collections.emptyMap();
        }

        Objects.requireNonNull(cloudEntities);

        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudEntities);
        try {
            entityCostStore.persistEntityCost(costs);
        } catch (DbException e) {
            logger.error("Failed to persist entity costs.", e);
        }
    }

    private Map<Long, TopologyEntityDTO> readCloudEntities(@Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator)
            throws InterruptedException, TimeoutException, CommunicationException {
        final Map<Long, TopologyEntityDTO> topologyMap = new HashMap<>();
        while (entityIterator.hasNext()) {
            entityIterator.nextChunk().stream()
                .filter(this::isCloudEntity)
                .forEach(entity -> topologyMap.put(entity.getOid(), entity));
        }
        return topologyMap;
    }

    private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
        // TODO (roman, Sept 4 2018): We can safely filter out a lot of entities here, to reduce
        // the amount of data that we keep in memory in the cost component.
        return true;
    }
}

