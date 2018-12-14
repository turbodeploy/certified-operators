package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.ProjectedReservedInstanceCoverageListener;

/**
 * Listener that receives the projected entity RI coverage from the market and forwards them to
 * the classes in the cost component that store them and make them available for queries.
 */
public class ProjectedRICoverageListener implements ProjectedReservedInstanceCoverageListener {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedRICoverageStore projectedRICoverageStore;

    ProjectedRICoverageListener(@Nonnull final ProjectedRICoverageStore projectedRICoverageStore) {
        this.projectedRICoverageStore = Objects.requireNonNull(projectedRICoverageStore);
    }

    @Override
    public void onProjectedEntityRiCoverageReceived(final long projectedTopologyId,
                                               @Nonnull final TopologyInfo originalTopologyInfo,
                                               @Nonnull final RemoteIterator<EntityReservedInstanceCoverage> entityCosts) {
        logger.debug("Receiving projected RI coverage information for topology {}", projectedTopologyId);
        if (originalTopologyInfo.getTopologyType() == TopologyType.PLAN) {
            logger.warn("Received unexpected plan topology. Expecting real time only. (id: {})",
                    projectedTopologyId);
            return;
        }

        final Stream.Builder<EntityReservedInstanceCoverage> costStreamBuilder = Stream.builder();
        long coverageCount = 0;
        int chunkCount = 0;
        while (entityCosts.hasNext()) {
            try {
                final Collection<EntityReservedInstanceCoverage> nextChunk = entityCosts.nextChunk();
                for (EntityReservedInstanceCoverage riCoverage : nextChunk) {
                    coverageCount++;
                    costStreamBuilder.add(riCoverage);
                }
                chunkCount++;
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for processing projected RI coverage chunk." +
                        "Processed " + chunkCount + " chunks so far.", e);
            } catch (TimeoutException e) {
                logger.error("Timed out waiting for next entity RI coverage chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            } catch (CommunicationException e) {
                logger.error("Connection error when waiting for next entity RI coverage chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            }
        }
        projectedRICoverageStore.updateProjectedRICoverage(costStreamBuilder.build());
        logger.debug("Finished processing projected RI coverage info. Got RI coverage for {} entities, " +
                "delivered in {} chunks.", coverageCount, chunkCount);
    }
}
