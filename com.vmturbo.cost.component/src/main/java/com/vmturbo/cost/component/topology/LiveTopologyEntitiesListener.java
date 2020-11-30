package com.vmturbo.cost.component.topology;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for new topologies and store cloud cost entries in the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private final BusinessAccountHelper businessAccountHelper;

    private final TopologyInfoTracker liveTopologyInfoTracker;

    private final Object topologyProcessorLock = new Object();

    private ThreadFactory threadFactory;

    private ExecutorService threadPool;

    private final List<LiveCloudTopologyListener> scheduledTasks = new ArrayList<>();

    public LiveTopologyEntitiesListener(@Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
                                        @Nonnull final BusinessAccountHelper businessAccountHelper,
                                        @Nonnull final TopologyInfoTracker liveTopologyInfoTracker,
                                        @Nonnull final List<LiveCloudTopologyListener> cloudTopologyListenerList) {
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.liveTopologyInfoTracker = Objects.requireNonNull(liveTopologyInfoTracker);
        threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("LiveCloudTopologyListener-%d")
                .build();
        threadPool = Executors.newCachedThreadPool(threadFactory);
        scheduledTasks.addAll(cloudTopologyListenerList);
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
            if (cloudTopology.size() > 0) {
                storeBusinessAccountIdToTargetIdMapping(cloudTopology.getEntities());
                Map<LiveCloudTopologyListener, Future> futureTasks = new HashMap<>();
                for (LiveCloudTopologyListener task: scheduledTasks) {
                    futureTasks.put(task, threadPool.submit(() -> {
                        task.process(cloudTopology, topologyInfo);
                    }));
                }
                for (Map.Entry<LiveCloudTopologyListener, Future> task: futureTasks.entrySet()) {
                    LiveCloudTopologyListener currentTask = task.getKey();
                    try {
                        task.getValue().get();
                    } catch (InterruptedException e) {
                        logger.error("Interrupted Exception encountered while running live topology cloud listener {}", currentTask, e);
                    } catch (ExecutionException e) {
                        logger.error("Execution Exception encountered while running live topology cloud listener {}", currentTask, e);
                    }
                }
            } else {
                logger.info("Live topology with topologyId {}  doesn't have Cloud entities."
                        + " Partial processing will include Buy RI Analysis only.",
                        topologyInfo.getTopologyId());
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
                if (entityDTO.hasTypeSpecificInfo()
                        && entityDTO.getTypeSpecificInfo().hasBusinessAccount()
                        && entityDTO.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId()) {
                    businessAccountHelper.storeDiscoveredBusinessAccount(entityDTO);
                }

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

