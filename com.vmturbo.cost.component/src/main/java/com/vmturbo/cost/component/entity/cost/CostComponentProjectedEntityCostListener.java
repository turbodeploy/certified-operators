package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.antlr.v4.runtime.misc.Array2DHashSet;
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

    private final PlanProjectedEntityCostStore planProjectedEntityCostStore;

    CostComponentProjectedEntityCostListener(@Nonnull final ProjectedEntityCostStore projectedEntityCostStore,
                                             @Nonnull final PlanProjectedEntityCostStore planProjectedEntityCostStore) {
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.planProjectedEntityCostStore = Objects.requireNonNull(planProjectedEntityCostStore);
    }

    @Override
    public void onProjectedEntityCostsReceived(final long projectedTopologyId,
                                               @Nonnull final TopologyInfo originalTopologyInfo,
                                               @Nonnull final RemoteIterator<EntityCost> entityCosts) {
        logger.debug("Receiving projected entity costs for topology {}", projectedTopologyId);
        final List<EntityCost> costList = new ArrayList<>();
        long costCount = 0;
        int chunkCount = 0;
        while (entityCosts.hasNext()) {
            try {
                final Collection<EntityCost> nextChunk = entityCosts.nextChunk();
                for (EntityCost cost : nextChunk) {
                    costCount++;
                    costList.add(cost);
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
        if (originalTopologyInfo.getTopologyType() == TopologyType.PLAN) {
            planProjectedEntityCostStore.updatePlanProjectedEntityCostsTableForPlan(originalTopologyInfo,
                    costList);
        } else {
            projectedEntityCostStore.updateProjectedEntityCosts(costList);
        }
        logger.debug("Finished processing projected entity costs. Got costs for {} entities, " +
                "delivered in {} chunks.", costCount, chunkCount);
    }
}
