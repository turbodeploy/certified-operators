package com.vmturbo.cost.component.topology;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

public class PlanTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;
    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;
    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;
    private final EntityCostStore planEntityCostStore;
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;
    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;
    private final BusinessAccountHelper businessAccountHelper;
    private final ReservedInstanceAnalysisInvoker invoker;

    public PlanTopologyEntitiesListener(final long realtimeTopologyContextId,
                                        @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
                                        @Nonnull final EntityCostStore planEntityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
                                        @Nonnull final BusinessAccountHelper businessAccountHelper,
                                        @Nonnull final ReservedInstanceAnalysisInvoker invoker) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
        this.planEntityCostStore = Objects.requireNonNull(planEntityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.invoker = Objects.requireNonNull(invoker);
    }

    @Override
    public void onTopologyNotification(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        if (topologyContextId == realtimeTopologyContextId) {
            logger.error("Received plan topology with wrong topologyContextId."
                         + "Expected:{}, Received:{}", topologyContextId, realtimeTopologyContextId);
            return;
        }
        logger.info("Received plan topology with topologyId: {}", topologyInfo.getTopologyId());
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                        cloudTopologyFactory.newCloudTopology(topologyContextId, entityIterator);

        // if no Cloud entity, skip further processing
        if (cloudTopology.size() > 0) {
            final TopologyCostCalculator topologyCostCalculator =
                        topologyCostCalculatorFactory.newCalculator(topologyInfo, cloudTopology);
            final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                            topologyCostCalculator.calculateCosts(cloudTopology);
            try {
                // Persist plan cost data
                planEntityCostStore.persistEntityCost(costs, cloudTopology, topologyContextId, true);
            } catch (DbException e) {
                logger.error("Failed to persist entity costs.", e);
            }
        } else {
            logger.debug("Plan topology with topologyId: {}  doesn't have Cloud entity, skip processing",
                        topologyInfo.getTopologyId());
        }
    }
}
