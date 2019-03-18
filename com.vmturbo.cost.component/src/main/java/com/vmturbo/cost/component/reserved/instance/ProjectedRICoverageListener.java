package com.vmturbo.cost.component.reserved.instance;

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

    private final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore;

    private final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore;

    ProjectedRICoverageListener(@Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
                                @Nonnull final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore) {
        this.projectedRICoverageAndUtilStore = Objects.requireNonNull(projectedRICoverageStore);
        this.planProjectedRICoverageAndUtilStore = Objects.requireNonNull(planProjectedRICoverageAndUtilStore);
    }

    @Override
    public void onProjectedEntityRiCoverageReceived(final long projectedTopologyId,
                                                    @Nonnull final TopologyInfo originalTopologyInfo,
                                                    @Nonnull final RemoteIterator<EntityReservedInstanceCoverage>
                                                    riCoverageIterator) {
        logger.debug("Receiving projected RI coverage information for topology {}", projectedTopologyId);
        final List<EntityReservedInstanceCoverage> riCoverageList= new ArrayList<>();
        long coverageCount = 0;
        int chunkCount = 0;
        while (riCoverageIterator.hasNext()) {
            try {
                final Collection<EntityReservedInstanceCoverage> nextChunk = riCoverageIterator.nextChunk();
                for (EntityReservedInstanceCoverage riCoverage : nextChunk) {
                    coverageCount++;
                    riCoverageList.add(riCoverage);
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
        if (originalTopologyInfo.getTopologyType() == TopologyType.PLAN) {
            planProjectedRICoverageAndUtilStore.updateProjectedEntityToRIMappingTableForPlan(originalTopologyInfo,
                                                                                             riCoverageList);
            planProjectedRICoverageAndUtilStore.updateProjectedRICoverageTableForPlan(projectedTopologyId,
                                                                                      originalTopologyInfo,
                                                                                      riCoverageList);
            planProjectedRICoverageAndUtilStore.updateProjectedRIUtilTableForPlan(originalTopologyInfo,
                                                                                  riCoverageList);
        } else {
            projectedRICoverageAndUtilStore.updateProjectedRICoverage(riCoverageList);
        }
        logger.debug("Finished processing projected RI coverage info. Got RI coverage for {} entities, " +
                        "delivered in {} chunks.", coverageCount, chunkCount);
    }
}
