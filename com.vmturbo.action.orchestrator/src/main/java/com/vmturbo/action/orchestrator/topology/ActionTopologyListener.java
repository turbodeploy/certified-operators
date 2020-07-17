package com.vmturbo.action.orchestrator.topology;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology.ActionRealtimeTopologyBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.EntitiesWithNewStateListener;

/**
 * Listens to live topologies and entity state updates from the topology processor.
 */
public class ActionTopologyListener implements EntitiesWithNewStateListener, EntitiesListener {
    private static final Logger logger = LogManager.getLogger();

    private final ActionStorehouse actionStorehouse;

    private final ActionTopologyStore actionTopologyStore;

    private final long realTimeTopologyContextId;

    /**
     * Constructs new instance of {@code MarketActionListener}.
     *
     * @param actionStorehouse Action store house.
     * @param actionTopologyStore The {@link ActionTopologyStore} which will receive the topology
     *  updates.
     * @param realTimeTopologyContextId the real time topology context id.
     */
    public ActionTopologyListener(@Nonnull final ActionStorehouse actionStorehouse,
                                     @Nonnull final ActionTopologyStore actionTopologyStore,
                                     final long realTimeTopologyContextId) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionTopologyStore = actionTopologyStore;
        this.realTimeTopologyContextId = realTimeTopologyContextId;
    }

    @Override
    public void onEntitiesWithNewState(@Nonnull final EntitiesWithNewState entitiesWithNewState) {
        if (actionStorehouse.getStore(realTimeTopologyContextId).isPresent()) {

            LiveActionStore actionStore =
                ((LiveActionStore)actionStorehouse.getStore(realTimeTopologyContextId).get());
            actionStore.updateActionsBasedOnNewStates(entitiesWithNewState);
        }

        actionTopologyStore.setEntityWithUpdatedState(entitiesWithNewState);
    }

    @Override
    public void onTopologyNotification(@Nonnull TopologyInfo topologyInfo,
                @Nonnull RemoteIterator<DataSegment> topologyDTOs,
                @Nonnull SpanContext tracingContext) {
        if (topologyInfo.getTopologyContextId() != realTimeTopologyContextId) {
            RemoteIteratorDrain.drainIterator(topologyDTOs,
                    TopologyDTOUtil.getSourceTopologyLabel(topologyInfo), false);
            return;
        }

        ActionRealtimeTopologyBuilder bldr = actionTopologyStore.newRealtimeSourceTopology(topologyInfo);
        try (TracingScope scope = Tracing.trace("AO on_topology_notification", tracingContext)) {
            while (topologyDTOs.hasNext()) {
                Collection<DataSegment> segment = topologyDTOs.nextChunk();
                segment.forEach(e -> bldr.addEntity(e.getEntity()));
            }
            ActionRealtimeTopology topology = bldr.finish();
            logger.info("Successfully updated topology. New topology has {} entities.", topology.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for topology chunk.", e);
        } catch (TimeoutException e) {
            logger.error("Timed out waiting for next topology chunk.", e);
        } catch (CommunicationException e) {
            logger.error("Failed to get next topology chunk.", e);
        }
    }
}
