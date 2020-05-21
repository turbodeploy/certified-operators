package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility class that contains the cache for entity with new states and the methods to update
 * actions based on them.
 */
class EntitiesWithNewStateCache {

    /**
     * Map containing the id of the host with the corresponding HostWithUpdatedState object.
     */
    private final Map<Long, HostWithUpdatedState> hostsWithNewState = new ConcurrentHashMap<>();

    private final Logger logger = LogManager.getLogger();


    private final LiveActions actions;

    EntitiesWithNewStateCache(@Nonnull final LiveActions actions) {
        this.actions = actions;
    }

    int clearActionsAndUpdateCache(long topologyId) {
        hostsWithNewState.entrySet().removeIf(entry -> (
            topologyId > entry.getValue().getStateChangeId()));
        return clearActionsAffectingHostsWithNewState(hostsWithNewState.values());
    }

    void updateHostsWithNewState(@Nonnull final EntitiesWithNewState entitiesWithNewState) {
        List<HostWithUpdatedState> hostsWithUpdatedState = getHostsWithUpdatedState(entitiesWithNewState.getTopologyEntityList(),
            entitiesWithNewState.getStateChangeId());

        for (HostWithUpdatedState hostWitUpdatedState: hostsWithUpdatedState) {
            this.hostsWithNewState.put(hostWitUpdatedState.getId(), hostWitUpdatedState);
        }
        clearActionsAffectingHostsWithNewState(hostsWithUpdatedState);
    }

    /**
     * Collects actions that are affecting hosts with an updated state. After removing the
     * actions from the live action store it refreshes the severity cache. This needs to happen
     * in two steps to ensure thread safety.
     *
     * @param hostsWithUpdatedState the affected entities.
     * @return the number of actions cleared
     */
    private int clearActionsAffectingHostsWithNewState(Collection<HostWithUpdatedState> hostsWithUpdatedState) {
        if (hostsWithUpdatedState.isEmpty()) {
            return 0;
        }
        Set<Long> hostsInMaintenanceIds = new HashSet<>();
        Set<Long> activeHostIds = new HashSet<>();

        for (HostWithUpdatedState hostWitUpdatedState: hostsWithUpdatedState) {
            if (hostWitUpdatedState.getState() == EntityState.MAINTENANCE) {
                hostsInMaintenanceIds.add(hostWitUpdatedState.getId());
            }
            if (hostWitUpdatedState.getState() == EntityState.POWERED_ON) {
                activeHostIds.add(hostWitUpdatedState.getId());
            }
        }

        Set<Long> affectedActions = new HashSet<>();
        actions.doForEachMarketAction((action) -> {
            if (actionAffectsHostWithUpdatedState(hostsInMaintenanceIds, activeHostIds, action)) {
                affectedActions.add(action.getId());
            }
        });
        if (affectedActions.isEmpty()) {
            return 0;
        }
        actions.deleteActionsById(affectedActions);
        return affectedActions.size();
    }

    /**
     * Find actions that are targeting hosts with a new state.
     * @param hostsInMaintenanceIds ids of host that went into maintenance
     * @param activeHostIds ids of host that are active.
     * @param action the action to check.
     * @return true if the action affects the entity
     */
     private boolean actionAffectsHostWithUpdatedState(@Nonnull final Set<Long> hostsInMaintenanceIds,
                                                      @Nonnull final Set<Long> activeHostIds,
                                                      @Nonnull Action action) {
        ActionInfo actionInfo = action.getRecommendation().getInfo();
        if (actionInfo.hasMove()) {
            for (ChangeProvider changeProvider : actionInfo.getMove().getChangesList()) {
                // For hosts that went into maintenance we want to remove all the move actions that
                // have that host as the destination
                if (hostsInMaintenanceIds.contains(changeProvider.getDestination().getId())) {
                    return true;
                }
                // For hosts that went into powered_on state we want to remove all the move actions
                // that have that host as the source
                if (activeHostIds.contains(changeProvider.getSource().getId())) {
                    return true;
                }
            }
        }
        try {
            // For hosts that went into maintenance we want to remove all the  actions that have
            // the host as the primary entity
            if (hostsInMaintenanceIds.contains(ActionDTOUtil.getPrimaryEntity(action.getTranslationResultOrOriginal()).getId())) {
                return true;
            }
        } catch (UnsupportedActionException e) {
            logger.warn("Unable to get primary entity for action {}", action);
        }
        return false;
    }

    private List<HostWithUpdatedState> getHostsWithUpdatedState(List<TopologyEntityDTO> entityDtos, long stateChangeId ) {
        return entityDtos.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .map(entity -> new HostWithUpdatedState(entity.getOid(), entity.getEntityState(), stateChangeId))
            .collect(Collectors.toList());
    }

    /**
     * Static object that contains the information about a host with an updated state.
     */
    public static class HostWithUpdatedState {
        private final long id;
        private final EntityState state;

        long getStateChangeId() {
            return stateChangeId;
        }

        private final long stateChangeId;

        /**
         * Constructs a HostWithUpdatedState object.
         * @param id the id of the host
         * @param state the new state of the host
         * @param stateChangeId the id corresponding to the change of state. This id is used to
         * compare against topology ids, to see when a change was detected
         * with respect to a topology
         */
        HostWithUpdatedState(long id, EntityState state, long stateChangeId) {
            this.id = id;
            this.state = state;
            this.stateChangeId = stateChangeId;
        }

        public long getId() {
            return id;
        }

        public EntityState getState() {
            return state;
        }
    }
}
