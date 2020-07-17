package com.vmturbo.cost.component.topology;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.cost.calculation.journal.CostJournal;
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

    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    private final CloudCommitmentDemandWriter cloudCommitmentDemandWriter;

    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

    private final EntityCostStore entityCostStore;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private final BusinessAccountHelper businessAccountHelper;

    private final CostJournalRecorder journalRecorder;

    private final ReservedInstanceAnalysisInvoker invoker;

    private final TopologyInfoTracker liveTopologyInfoTracker;

    private final Object topologyProcessorLock = new Object();

    public LiveTopologyEntitiesListener(@Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
                                        @Nonnull final EntityCostStore entityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
                                        @Nonnull final BusinessAccountHelper businessAccountHelper,
                                        @Nonnull final CostJournalRecorder journalRecorder,
                                        @Nonnull final ReservedInstanceAnalysisInvoker invoker,
                                        @Nonnull final TopologyInfoTracker liveTopologyInfoTracker,
                                        @Nonnull final CloudCommitmentDemandWriter cloudCommitmentDemandWriter) {
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
        this.entityCostStore = Objects.requireNonNull(entityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.journalRecorder = Objects.requireNonNull(journalRecorder);
        this.invoker = Objects.requireNonNull(invoker);
        this.liveTopologyInfoTracker = Objects.requireNonNull(liveTopologyInfoTracker);
        this.cloudCommitmentDemandWriter = Objects.requireNonNull(cloudCommitmentDemandWriter);
    }

    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                                       @Nonnull final SpanContext tracingContext) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();

        try (TracingScope tracingScope = Tracing.trace("cost_live_topology", tracingContext)) {
            if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                logger.info("Queuing topology for processing (Context ID={}, Topology ID={})",
                    topologyContextId, topologyId);

                // Block on processing the topology so that we only keep one cloud topology in memory
                // concurrently.
                synchronized (topologyProcessorLock) {
                    processLiveTopologyUpdate(topologyInfo, entityIterator);
                }
            } else {
                logger.debug("Skipping plan topology broadcast (Topology Context Id={}, Topology ID={}, Creation Time={})",
                    topologyContextId, topologyId, Instant.ofEpochMilli(topologyInfo.getCreationTime()));

                RemoteIteratorDrain.drainIterator(
                    entityIterator,
                    TopologyDTOUtil.getSourceTopologyLabel(topologyInfo),
                    false);
            }
        }
    }

    private void processLiveTopologyUpdate(@Nonnull final TopologyInfo topologyInfo,
                                           @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();

        // Once we start to process the topology, make sure it is the latest topology based on topology
        // summary notifications. It is possible that while waiting on the topology processing lock,
        // a subsequent topology broadcast occurs.
        if (liveTopologyInfoTracker.isLatestTopology(topologyInfo)) {
            logger.info("Processing live topology with topologyId: {}", topologyId);

            final CloudTopology<TopologyEntityDTO> cloudTopology =
                    cloudTopologyFactory.newCloudTopology(topologyContextId, entityIterator);

            // If no Cloud entity, skip further processing.
            // Run Buy RI in most situations except when we fail to persist costs.
            boolean runBuyRI = true;
            if (cloudTopology.size() > 0) {
                // Store allocation demand in db
                computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(topologyInfo, cloudTopology, false);
                // Consumption demand in stored in db in CostComponentProjectedEntityTopologyListener
                storeBusinessAccountIdToTargetIdMapping(cloudTopology.getEntities());

                // update reserved instance coverage data. RI coverage must be updated
                // before cost calculation to accurately reflect costs based on up-to-date
                // RI coverage
                reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo, cloudTopology);

                final TopologyCostCalculator topologyCostCalculator = topologyCostCalculatorFactory.newCalculator(topologyInfo, cloudTopology);
                final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                        topologyCostCalculator.calculateCosts(cloudTopology);

                journalRecorder.recordCostJournals(costs);

                try {
                    entityCostStore.persistEntityCost(costs, cloudTopology, topologyInfo.getCreationTime(), false);
                } catch (DbException e) {
                    runBuyRI = false;
                    logger.error("Failed to persist entity costs.", e);
                }
            } else {
                logger.info("Live topology with topologyId {}  doesn't have Cloud entities."
                        + " Partial processing will include Buy RI Analysis only.",
                        topologyInfo.getTopologyId());
            }
            // Store allocation demand in db RI Buy 2.0
            cloudCommitmentDemandWriter.writeAllocationDemand(cloudTopology, topologyInfo);
            if (runBuyRI) {
                invoker.invokeBuyRIAnalysis(cloudTopology, businessAccountHelper.getAllBusinessAccounts());
            }
        } else {
            logger.warn("Skipping stale topology broadcast (Topology Context Id={}, Topology ID={}, Creation Time={})",
                    topologyContextId, topologyId, Instant.ofEpochMilli(topologyInfo.getCreationTime()));

            reservedInstanceCoverageUpdate.skipCoverageUpdate(topologyInfo);

            RemoteIteratorDrain.drainIterator(
                    entityIterator,
                    TopologyDTOUtil.getSourceTopologyLabel(topologyInfo),
                    false);
        }
    }

    // store the mapping between business account id to discovered target id
    private void storeBusinessAccountIdToTargetIdMapping(@Nonnull final Map<Long, TopologyEntityDTO> cloudEntities) {
        for (TopologyEntityDTO entityDTO : cloudEntities.values()) {
            // If the entity is a business account, store the discovered target id.
            if (entityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                long baOID = entityDTO.getOid();
                businessAccountHelper.storeTargetMapping(baOID,
                        entityDTO.getDisplayName(), getTargetId(entityDTO));
                logger.debug("TopologyEntityDTO for BA {}", entityDTO);
            }
        }
    }

    // get target Id, it should have only one target for a business account
    private Collection<Long> getTargetId(@Nonnull final TopologyEntityDTO entityDTO) {
        return entityDTO
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveredTargetDataMap().keySet();
    }
}

