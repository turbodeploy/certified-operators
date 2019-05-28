package com.vmturbo.market.topology;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
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

    private Optional<Integer> maxPlacementsOverride;

    private final float rightsizeLowerWatermark;

    private final float rightsizeUpperWatermark;

    // TODO: we need to make sure that only a single instance of TopologyEntitiesListener
    // be created and used. Using public constructor here can not guarantee it!
    @SuppressWarnings("unused")
    private TopologyEntitiesListener() {
        // private - do not call
        throw new RuntimeException("private constructor called");
    }

    public TopologyEntitiesListener(@Nonnull MarketRunner marketRunner,
                                    @Nonnull final Optional<Integer> maxPlacementsOverride,
                                    final float rightsizeLowerWatermark,
                                    final float rightsizeUpperWatermark) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
        this.maxPlacementsOverride = Objects.requireNonNull(maxPlacementsOverride);
        this.rightsizeLowerWatermark = rightsizeLowerWatermark;
        this.rightsizeUpperWatermark = rightsizeUpperWatermark;

        maxPlacementsOverride.ifPresent(maxPlacementIterations ->
            logger.info("Overriding max placement iterations to: {}", maxPlacementIterations));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        // Do not cache {@link TopologyEntityDTO}'s if analysis is already running on a RT topology
        if (marketRunner.isAnalysisRunningForRtTopology(topologyInfo)) {
            try {
                // drain the iterator and exit.
                while (entityIterator.hasNext()) {
                    entityIterator.nextChunk();
                }
            } catch (CommunicationException | TimeoutException e) {
                logger.error("Error occurred while receiving topology " + topologyId + " with for " +
                        "context " + topologyContextId, e);
            } catch (InterruptedException e) {
                logger.info("Thread interrupted receiving topology " + topologyId + " with for " +
                        "context " + topologyContextId, e);
            }
            return;
        }
        // TODO: karthikt : Do we really need a Set here. Duplicated entities
        // can be easily detected by just checking the Ids.Computing the hash
        // for the entire EntityDTO object would be expensive as it would need
        // to look at all the fields.
        final Set<TopologyEntityDTO> entities = new HashSet<>();
        try {
            while (entityIterator.hasNext()) {
                entities.addAll(entityIterator.nextChunk());
            }
        } catch (CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId + " with for " +
                    "context " + topologyContextId, e);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId + " with for " +
                    "context " + topologyContextId, e);
        }
        marketRunner.scheduleAnalysis(topologyInfo, entities, false, maxPlacementsOverride,
                rightsizeLowerWatermark, rightsizeUpperWatermark);
    }
}
