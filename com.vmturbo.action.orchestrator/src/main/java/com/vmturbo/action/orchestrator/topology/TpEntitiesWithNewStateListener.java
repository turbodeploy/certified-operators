package com.vmturbo.action.orchestrator.topology;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
        Set<Long> hostsInMaintenanceIds = getHostInMaintenanceIds(entitiesWithNewState.getTopologyEntityList());

        if (actionStorehouse.getStore(realTimeTopologyContextId).isPresent() &&
            !hostsInMaintenanceIds.isEmpty()) {

            LiveActionStore actionStore =
                ((LiveActionStore)actionStorehouse.getStore(realTimeTopologyContextId).get());
            actionStore.clearActionsOnHosts(hostsInMaintenanceIds,
                entitiesWithNewState.getStateChangeId());
        }
    }

    private Set<Long> getHostInMaintenanceIds(List<TopologyEntityDTO> entityDtos) {
        return entityDtos.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE &&
                entity.getEntityState() == EntityState.MAINTENANCE).map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());
    }
}
