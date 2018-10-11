package com.vmturbo.cost.component.entity.cost;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.ProjectedEntityCostsListener;

/**
 * Listener that receives the projected entity costs from the market and forwards them to
 * the classes in the cost component that store them and make them available for queries.
 */
public class CostComponentProjectedEntityCostListener implements ProjectedEntityCostsListener {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedEntityCostStore projectedEntityCostStore;

    CostComponentProjectedEntityCostListener(@Nonnull final ProjectedEntityCostStore projectedEntityCostStore) {
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
    }

    @Override
    public void onProjectedEntityCostsReceived(final long projectedTopologyId,
                                               @Nonnull final TopologyInfo originalTopologyInfo,
                                               @Nonnull final RemoteIterator<EntityCost> entityCosts) {
        logger.debug("Receiving projected entity costs for topology {}", projectedTopologyId);
        if (originalTopologyInfo.getTopologyType() == TopologyType.PLAN) {
            logger.warn("Received unexpected plan topology. Expecting realtime only. (id: {})",
                    projectedTopologyId);
            return;
        }

        final Stream.Builder<EntityCost> costStreamBuilder = Stream.builder();
        long costCount = 0;
        int chunkCount = 0;
        while (entityCosts.hasNext()) {
            try {
                final Collection<EntityCost> nextChunk = entityCosts.nextChunk();
                for (EntityCost cost : nextChunk) {
                    costCount++;
                    costStreamBuilder.add(cost);
                }
                chunkCount++;
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for processing projected entity costs chunk." +
                        "Processed " + chunkCount + " chunks so far.", e);
            } catch (TimeoutException e) {
                logger.error("Timed out waiting for next entity costs chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            } catch (CommunicationException e) {
                logger.error("Connection error when waiting for next entity costs chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            }
        }
        projectedEntityCostStore.updateProjectedEntityCosts(costStreamBuilder.build());
        logger.debug("Finished processing projected entity costs. Got costs for {} entities, " +
                "delivered in {} chunks.", costCount, chunkCount);
    }
}
