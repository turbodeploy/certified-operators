package com.vmturbo.market.topology;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private final SettingServiceBlockingStub settingServiceClient;

    // TODO: we need to make sure that only a single instance of TopologyEntitiesListener
    // be created and used. Using public constructor here can not guarantee it!
    @SuppressWarnings("unused")
    private TopologyEntitiesListener() {
        // private - do not call
        throw new RuntimeException("private constructor called");
    }

    public TopologyEntitiesListener(@Nonnull MarketRunner marketRunner,
                                    @Nonnull SettingServiceBlockingStub settingServiceClient) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
        this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {
        // TODO: karthikt : Do we really need a Set here. Duplicated entities
        // can be easily detected by just checking the Ids.Computing the hash
        // for the entire EntityDTO object would be expensive as it would need
        // to look at all the fields.
        final Set<TopologyEntityDTO> entities = new HashSet<>();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
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
        marketRunner.scheduleAnalysis(topologyInfo, entities, false,
            settingServiceClient);
    }
}
