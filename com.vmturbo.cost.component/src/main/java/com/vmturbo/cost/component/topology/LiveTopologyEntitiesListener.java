package com.vmturbo.cost.component.topology;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

    private final EntityCostStore entityCostStore;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private final BusinessAccountHelper businessAccountHelper;

    private final CostJournalRecorder journalRecorder;

    private final ReservedInstanceAnalysisInvoker invoker;

    public LiveTopologyEntitiesListener(final long realtimeTopologyContextId,
                                        @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
                                        @Nonnull final EntityCostStore entityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
                                        @Nonnull final BusinessAccountHelper businessAccountHelper,
                                        @Nonnull final CostJournalRecorder journalRecorder,
                                        @Nonnull final ReservedInstanceAnalysisInvoker invoker) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
        this.entityCostStore = Objects.requireNonNull(entityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.journalRecorder = Objects.requireNonNull(journalRecorder);
        this.invoker = Objects.requireNonNull(invoker);
    }

    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator) {

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

        // if no Cloud entity, skip further processing
        if (cloudTopology.size() > 0) {
            // Store allocation demand in db
            computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(topologyInfo, cloudTopology, false);
            // Consumption demand in stored in db in CostComponentProjectedEntityTopologyListener
            if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                storeBusinessAccountIdToTargetIdMapping(cloudTopology.getEntities());
            }

            invoker.invokeRIBuyIfBusinessAccountsUpdated(businessAccountHelper.getAllBusinessAccounts());

            // update reserved instance coverage data. RI coverage must be updated
            // before cost calculation to accurately reflect costs based on up-to-date
            // RI coverage
            reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo, cloudTopology);

            final TopologyCostCalculator topologyCostCalculator = topologyCostCalculatorFactory.newCalculator(topologyInfo, cloudTopology);
            final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudTopology);

            journalRecorder.recordCostJournals(costs);

            try {
                entityCostStore.persistEntityCost(costs, cloudTopology);
            } catch (DbException e) {
                logger.error("Failed to persist entity costs.", e);
            }
        } else {
            logger.info("live topology with topologyId: {}  doesn't have Cloud entity, skip processing",
                    topologyInfo.getTopologyId());
        }
    }

    // store the mapping between business account id to discovered target id
    private void storeBusinessAccountIdToTargetIdMapping(@Nonnull final Map<Long, TopologyEntityDTO> cloudEntities) {
        for (TopologyEntityDTO entityDTO : cloudEntities.values()) {
            // If the entity is a business account, store the discovered target id.
            if (entityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                long baOID = entityDTO.getOid();
                businessAccountHelper
                        .storeTargetMapping(baOID, getTargetId(entityDTO));
            }
        }
    }

    // get target Id, it should have only one target for a business account
    private Collection<Long> getTargetId(final TopologyEntityDTO entityDTO) {
        return entityDTO
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveredTargetDataMap().keySet();
    }
}

