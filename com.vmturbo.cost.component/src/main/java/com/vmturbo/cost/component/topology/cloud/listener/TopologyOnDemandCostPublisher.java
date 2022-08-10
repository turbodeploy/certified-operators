package com.vmturbo.cost.component.topology.cloud.listener;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.Data;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.End;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.notification.TopologyCostSender;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * The TopologyOnDemandCostPublisher is responsible for gathering on-demand compute and license
 * rates for all VMs in the realtime topology and publishing them using a designated sender.
 */
public class TopologyOnDemandCostPublisher implements LiveCloudTopologyListener {

    private final int chunkSize;
    private static final Logger logger = LogManager.getLogger();
    private final EntityCostStore entityCostStore;
    private final TopologyCostSender topologyCostSender;
    private final long realtimeTopologyContextId;

    /**
     * Construct the TopologyOnDemandCostPublisher.
     *
     * @param entityCostStore source of entity costs
     * @param realtimeTopologyContextId OID for realtime topology context
     * @param topologyCostSender sender to publish fetched costs
     */
    public TopologyOnDemandCostPublisher(@Nonnull final EntityCostStore entityCostStore,
            final long realtimeTopologyContextId, final int chunkSize,
            @Nonnull final TopologyCostSender topologyCostSender) {
        this.entityCostStore = entityCostStore;
        this.topologyCostSender = topologyCostSender;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.chunkSize = chunkSize;
    }

    @Override
    public void process(final CloudTopology rawTopology, final TopologyInfo topologyInfo) {
        final CloudTopology<TopologyEntityDTO> cloudTopology = (CloudTopology<TopologyEntityDTO>)rawTopology;
        final Set<Long> vmOids =
                cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE).stream()
                        .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        if (vmOids.isEmpty()) {
            logger.warn("Cloud topology {} contains no VMs; skipping export of VM on-demand rates.", topologyInfo.getTopologyId());
            return;
        }
        logger.debug("Exporting on-demand rates for VMs: {} within topology {}", () -> vmOids, topologyInfo::toString);
        final EntityCostFilter filter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, realtimeTopologyContextId)
                        .entityIds(vmOids)
                        .costCategoryFilter(CostCategoryFilter.newBuilder()
                                .setExclusionFilter(false)
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                                .addCostCategory(CostCategory.ON_DEMAND_LICENSE)
                                .build())
                        .latestTimestampRequested(true)
                        .costSources(false, Collections.singleton(CostSource.ON_DEMAND_RATE))
                        .build();
        try {
            final Map<Long, Map<Long, EntityCost>> entityCostsByTimestamp = entityCostStore.getEntityCosts(filter);
            if (!entityCostsByTimestamp.isEmpty()) {
                // query is for latest, so only one timestamp
                final Collection<EntityCost> entityCosts =
                        entityCostsByTimestamp.values().iterator().next().values();
                final long topologyId = topologyInfo.getTopologyId();
                topologyCostSender.sendTopologyCostChunk(TopologyOnDemandCostChunk.newBuilder()
                        .setTopologyOid(topologyId)
                        .setStart(Start.newBuilder().setTopologyInfo(topologyInfo)).build());
                for (List<EntityCost> chunkData : Iterables.partition(entityCosts, chunkSize)) {
                    topologyCostSender.sendTopologyCostChunk(TopologyOnDemandCostChunk.newBuilder()
                            .setTopologyOid(topologyId)
                            .setData(Data.newBuilder().addAllEntityCosts(chunkData)).build());
                }
                topologyCostSender.sendTopologyCostChunk(TopologyOnDemandCostChunk.newBuilder()
                        .setTopologyOid(topologyId).setEnd(End.getDefaultInstance()).build());
            } else {
                logger.error("No on-demand rates found for VMs {}", vmOids);
                logger.debug("No entity costs found using filter {}", () -> filter);
            }
        } catch (final DbException e) {
            logger.error("Could not fetch on-demand rates for VMs {}", vmOids);
            logger.debug("Failed to fetch entity costs from store using filter {}: {}", () -> filter, e::getMessage);
        } catch (final CommunicationException | InterruptedException e) {
            logger.error("Failed to send on-demand rate chunk", e);
        }
    }
}
