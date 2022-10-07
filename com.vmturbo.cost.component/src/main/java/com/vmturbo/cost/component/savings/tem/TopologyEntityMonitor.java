package com.vmturbo.cost.component.savings.tem;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.savings.SavingsActionStore;
import com.vmturbo.cost.component.savings.SavingsException;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;

/**
 * Sets Liveness State and Start, End Times of Executed Actions based on topology updates.
 */
public class TopologyEntityMonitor implements LiveCloudTopologyListener {
    private final Logger logger = LogManager.getLogger();

    private final SavingsActionStore savingsActionStore;

    /**
     * Constructor.
     *
     * @param savingsActionStore  The Savings Action Store.
     */
    public TopologyEntityMonitor(@Nonnull final SavingsActionStore savingsActionStore) {
        this.savingsActionStore = Objects.requireNonNull(savingsActionStore);
    }

    /**
     * Helper class to compare and sort ExecutedActionsChangeWindow objects by execution completion time.
     */
    static final class SortByCompletionTime implements Comparator<ExecutedActionsChangeWindow> {
        @Override
        public int compare(ExecutedActionsChangeWindow cw1, ExecutedActionsChangeWindow cw2) {
            return Long.compare(cw1.getActionSpec().getExecutionStep().getCompletionTime(),
                    cw2.getActionSpec().getExecutionStep().getCompletionTime());
        }
    }

    /**
     * Process LivenessState Changes of Executed Action Change Windows based on topology updates.
     *
     * @param cloudTopology The cloud topology to process.
     * @param topologyInfo  Info about the cloud topology.
     */
    @Override
    public void process(CloudTopology cloudTopology, TopologyInfo topologyInfo) {
        try {
            if (!savingsActionStore.isInitialized()) {
                // Cache is not yet ready, otherwise we always get exception on cost pod startup.
                logger.warn("Skipping topology (id: {}, time: {}) as cache is not yet ready after startup.",
                        topologyInfo.getTopologyId(), topologyInfo.getCreationTime());
                return;
            }
            logger.info("Billed Savings Entity Monitor processing topology (id: {}, time: {}) updates.",
                    topologyInfo.getTopologyId(), topologyInfo.getCreationTime());

            handleActions(cloudTopology, topologyInfo);

            // Save all the Liveness State updates to cache
            savingsActionStore.saveChanges();
        } catch (Exception e) {
            logger.error("TEM2 Savings Exception for topology (id: {}, time: {})",
                    topologyInfo.getTopologyId(), topologyInfo.getCreationTime(), e);
        }
    }

    /**
     * Create mapping of entity oid to change windows for the entity sorted by completion time.
     *
     * @param changeWindows change windows foe entities.
     * @return Map of entityOids of entities to ordered treeset of action change windows for the entities.
     */
    private Map<Long, TreeSet<ExecutedActionsChangeWindow>> getOrderedChangeWindows(
            @Nonnull final Set<ExecutedActionsChangeWindow> changeWindows) {
        // Create a mapping of entityOid to sorted set of scale change windows for the entity.
        final Map<Long, TreeSet<ExecutedActionsChangeWindow>> entityOidToScaleWindows = changeWindows
                .stream()
                .collect(Collectors
                        .groupingBy(
                                ExecutedActionsChangeWindow::getEntityOid,
                                Collectors
                                        .mapping(
                                                Function.identity(), Collectors
                                                        .toCollection(() ->
                                                                new TreeSet<ExecutedActionsChangeWindow>(
                                                                        new SortByCompletionTime())))));

        return entityOidToScaleWindows;
    }

    /**
     * Process any change window updates for a generic actions.
     *
     * @param cloudTopology the cloud topology being processed.
     * @param topologyInfo Info about the cloud topology.
     */
    private void handleActions(final CloudTopology cloudTopology, final TopologyInfo topologyInfo) throws SavingsException {
        Set<ExecutedActionsChangeWindow> changeWindows = new HashSet<>();
        // Get all NEW and LIVE action change windows.
        changeWindows.addAll(savingsActionStore.getActions(LivenessState.NEW));
        changeWindows.addAll(savingsActionStore.getActions(LivenessState.LIVE));

        // Get ordered set of actions, and handle different action types.
        final Map<Long, TreeSet<ExecutedActionsChangeWindow>> entityOidToChangeWindows
                = getOrderedChangeWindows(changeWindows);
        // Compile list of action liveness updates.
        for (TreeSet<ExecutedActionsChangeWindow> entityChangeWindows : entityOidToChangeWindows.values()) {
            final ExecutedActionsChangeWindow latestScaleWindowForEntity = entityChangeWindows.last();
            if (latestScaleWindowForEntity == null) {
                // This shouldn't happen as each entity in the treeset will contain at least one scale change window.
                // This is a defensive check.
                continue;
            }

            final ActionSpec actionSpec = latestScaleWindowForEntity.getActionSpec();
            final ActionInfo actionInfo = actionSpec.getRecommendation().getInfo();
            if (actionInfo.hasScale()) {
                handleScale(entityChangeWindows, latestScaleWindowForEntity, actionSpec, cloudTopology, topologyInfo);
            } else if (actionInfo.hasDelete()) {
                handleDelete(entityChangeWindows, latestScaleWindowForEntity, cloudTopology, topologyInfo);
            }
        }
    }

    /**
     * Process any change window updates for scale actions.
     *
     * @param changeWindowsForEntity a sorted set of change windows for the entity.
     * @param latestChangeWindowForEntity the latest change window for the entity.
     * @param actionSpec the ActionSpec corresponding to the action change window.
     * @param cloudTopology the cloud topology being processed.
     * @param topologyInfo Info about the cloud topology.
     */
    private void handleScale(@Nonnull final TreeSet<ExecutedActionsChangeWindow> changeWindowsForEntity,
                             @Nonnull final ExecutedActionsChangeWindow latestChangeWindowForEntity,
                             @Nonnull final ActionSpec actionSpec,
                             final CloudTopology cloudTopology, final TopologyInfo topologyInfo) {
        // Compile list of scale action liveness updates.
        try {
            final long currentTimestamp = topologyInfo.getCreationTime();

            Optional<TopologyEntityDTO> discoveredEntity =
                    cloudTopology.getEntity(latestChangeWindowForEntity.getEntityOid());
            if (!discoveredEntity.isPresent()) {
                return;
            }

            final ProviderInfo discoveredProviderInfo = ProviderInfoFactory.getDiscoveredProviderInfo(
                    cloudTopology, discoveredEntity.get());
            final long actionOid = latestChangeWindowForEntity.getActionOid();
            final long entityOid = latestChangeWindowForEntity.getEntityOid();
            final boolean destinationMatches = discoveredProviderInfo.matchesAction(actionSpec, true);
            if (destinationMatches) {
                // Add request to update Start Time and Liveness if current state is NEW.  If already LIVE, no need to
                // update the latest action.
                if (latestChangeWindowForEntity.getLivenessState() == LivenessState.NEW) {
                    logger.debug("Destination match present for action {}, entity {}, provider info {}, updating to LIVE", actionOid,
                            entityOid,  discoveredProviderInfo);
                    savingsActionStore.activateAction(actionOid, currentTimestamp);
                }
                // Add requests to deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                deactivatePreviousChangeWindows(currentTimestamp, changeWindowsForEntity);
            } else {
                // LIVE --> REVERTED
                boolean sourceMatches = discoveredProviderInfo.matchesAction(actionSpec, false);
                if (sourceMatches) {
                    if (latestChangeWindowForEntity.getLivenessState() == LivenessState.NEW) {
                        logger.debug("NEW executed action tier change not detected yet ..could be that the"
                                + " action got Reverted or the entity deleted, or the update will be in the next"
                                + " broadcast, action {}, entity {}", actionOid, entityOid);
                    } else if (latestChangeWindowForEntity.getLivenessState() == LivenessState.LIVE) {
                        // Add request to update Start Time and Liveness to REVERTED.
                        logger.debug("Source match present for action {}, entity {}, provider info {}, updating to REVERTED", actionOid,
                                entityOid, discoveredProviderInfo);
                        savingsActionStore.deactivateAction(actionOid, currentTimestamp, LivenessState.REVERTED);
                        // Add requests to deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                        deactivatePreviousChangeWindows(currentTimestamp, changeWindowsForEntity);
                    }
                } else { // Neither source nor destination matches.
                    // The current provider is neither the source nor the destination of the action.
                    // Add request to update Start Time and Liveness to EXTERNAL_MODIFICATION.
                    logger.debug("Neither source nor destination match present for action {}, entity {}, provider info {},"
                            + " updating to EXTERNAL_MODIFICATION", actionOid, entityOid, discoveredProviderInfo);
                    savingsActionStore.deactivateAction(actionOid, currentTimestamp, LivenessState.EXTERNAL_MODIFICATION);
                    // Add requests to deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                    deactivatePreviousChangeWindows(currentTimestamp, changeWindowsForEntity);
                }
            }
        } catch (SavingsException se) {
            logger.error("TEM2 Savings Exception in handleScale at {}",  topologyInfo.getCreationTime(), se);
        }
    }

    /**
     * Process any change window updates for delete actions.
     *
     * @param changeWindowsForEntity a sorted set of change windows for the entity.
     * @param latestChangeWindowForEntity the latest change window for the entity.
     * @param cloudTopology the cloud topology being processed.
     * @param topologyInfo Info about the cloud topology.
     */
    private void handleDelete(@Nonnull final TreeSet<ExecutedActionsChangeWindow> changeWindowsForEntity,
                              @Nonnull final ExecutedActionsChangeWindow latestChangeWindowForEntity,
                              final CloudTopology cloudTopology, final TopologyInfo topologyInfo) {
        try {
            if (latestChangeWindowForEntity.getLivenessState() == LivenessState.NEW) {
                final long currentTimestamp = topologyInfo.getCreationTime();
                final long entityOid = latestChangeWindowForEntity.getEntityOid();
                Optional<TopologyEntityDTO> discoveredEntity = cloudTopology.getEntity(entityOid);
                if (!discoveredEntity.isPresent()) {
                    final long actionOid = latestChangeWindowForEntity.getActionOid();
                    logger.debug("Updating delete action {}, entity {} to LIVE", actionOid, entityOid);
                    // Create request to set the delete action for the entity to LIVE.
                    savingsActionStore.activateAction(actionOid, currentTimestamp);
                    // Add requests to deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                    deactivatePreviousChangeWindows(currentTimestamp, changeWindowsForEntity);
                }
            }
        } catch (SavingsException se) {
            logger.error("TEM2 Savings Exception in handleDelete at {}", topologyInfo.getCreationTime(), se);
        }
    }

    /**
     * Deactivate a collection of scale change windows, omitting the latest window, in case we missed updating them in
     * previous broadcast cycles.
     *
     * <p>This method is being used to deactivate older scale change windows for en entity by setting their LIVENESS state
     * to the default inactive state SUPERSEDED, unless they're already in another inactive state</p>
     * @param timestamp the time when the action became inactive (timestamp of topology being processed)
     * @param changeWindowsForEntity the action change windows for the entity.
     * @throws SavingsException if there's an error when deactivating an action.
     */
    private void deactivatePreviousChangeWindows(final long timestamp,
                                                 @Nonnull final TreeSet<ExecutedActionsChangeWindow> changeWindowsForEntity)
                            throws SavingsException {
        // Most of the time, after latest change window is processed, scaleWindowsForEntity should be empty,
        // with nothing to do. Abnormal situations, exceptions, discovery failures or missed topology updates could cause
        // actions stuck in incorrect liveness states. We add requests to update all of those older actions to SUPERSEDED
        // (the default inactive state).
        final ExecutedActionsChangeWindow latestScaleWindowForEntity = changeWindowsForEntity.pollLast();
        for (ExecutedActionsChangeWindow prevChangeWindow : changeWindowsForEntity) {
            // Change windows processed here would be active, since methods like handleScale only process NEW and
            // LIVE actions.
            // However we are double checking here, just in case we start processing actions already in REVERTED,
            // DELETED, SUPERSEDED ... states.  We don't want to update those.
            if (isActiveChangeWindow(prevChangeWindow)) {
                final long cwActionOid = prevChangeWindow.getActionOid();
                logger.debug("More recent NEW action match present for action {} which is {}, updating to SUPERSEDED",
                        cwActionOid, latestScaleWindowForEntity.getActionOid());
                savingsActionStore.deactivateAction(cwActionOid, timestamp, LivenessState.SUPERSEDED);
            }
        }
        // Add back latest change window after deactivating the rest.
        changeWindowsForEntity.add(latestScaleWindowForEntity);
    }

    /**
     * Checks if a change window is active.
     *
     * @param changeWindow  The changeWindow.
     * @return true if it's active (LIVE and NEW Liveness States), false otherwise.
     */
    private boolean isActiveChangeWindow(@Nonnull final ExecutedActionsChangeWindow changeWindow) {
        return (changeWindow.getLivenessState() == LivenessState.NEW
                || changeWindow.getLivenessState() == LivenessState.LIVE);

    }
}
