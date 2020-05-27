package com.vmturbo.action.orchestrator.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.topology.processor.api.EntitiesWithNewStateListener;

/**
 * Listens to action recommendations from the market.
 */
public class TpEntitiesWithNewStateListener implements EntitiesWithNewStateListener {

    private final ActionStorehouse actionStorehouse;

    private final long realTimeTopologyContextId;


    /**
     * Constructs new instance of {@code MarketActionListener}.
     *
     * @param actionStorehouse Action store house.
     * @param realTimeTopologyContextId the real time topology context id.
     */
    public TpEntitiesWithNewStateListener(@Nonnull final ActionStorehouse actionStorehouse,
                                     final long realTimeTopologyContextId) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.realTimeTopologyContextId = realTimeTopologyContextId;
    }

    @Override
    public void onEntitiesWithNewState(@Nonnull final EntitiesWithNewState entitiesWithNewState) {
        if (actionStorehouse.getStore(realTimeTopologyContextId).isPresent()) {

            LiveActionStore actionStore =
                ((LiveActionStore)actionStorehouse.getStore(realTimeTopologyContextId).get());
            actionStore.updateActionsBasedOnNewStates(entitiesWithNewState);
        }
    }
}
