package com.vmturbo.cost.component.topology;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final TopologyCostCalculator topologyCostCalculator;

    private final EntityCostStore entityCostStore;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    public LiveTopologyEntitiesListener(long realtimeTopologyContextId,
                                        @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final TopologyCostCalculator topologyCostCalculator,
                                        @Nonnull final EntityCostStore entityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.topologyCostCalculator = Objects.requireNonNull(topologyCostCalculator);
        this.entityCostStore = Objects.requireNonNull(entityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
    }

    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {

        final long topologyContextId = topologyInfo.getTopologyContextId();
        if (topologyContextId != realtimeTopologyContextId) {
            logger.error("Received topology with wrong topologyContextId."
                    + "Expected:{}, Received:{}", realtimeTopologyContextId,
                    topologyContextId);
            return;
        }
        logger.info("Received live topology with topologyId: {}", topologyInfo.getTopologyId());
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                cloudTopologyFactory.newCloudTopology(topologyContextId, entityIterator);

        computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(topologyInfo, cloudTopology.getEntities(), false);

        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudTopology);
        try {
            entityCostStore.persistEntityCost(costs);
        } catch (DbException e) {
            logger.error("Failed to persist entity costs.", e);
        }

        // update reserved instance coverage data.
        reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo.getTopologyId(), cloudTopology);
    }
}

