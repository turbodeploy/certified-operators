package com.vmturbo.cost.component.savings.tem;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
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
            logger.info("Billed Savings Entity Monitor processing topology updates");
            List<ExecutedActionsChangeWindow> changeWindows = new ArrayList<>();
            // Get all NEW and LIVE action change windows.
            changeWindows.addAll(savingsActionStore.getActions(LivenessState.NEW));
            changeWindows.addAll(savingsActionStore.getActions(LivenessState.LIVE));

            handleScale(changeWindows, cloudTopology, topologyInfo);

            // Save all the Liveness State updates to cache
            savingsActionStore.saveChanges();
        } catch (Exception e) {
            logger.error("TEM2 Savings Exception processing topology update for topology at {}",
                    topologyInfo.getCreationTime(), e);
        }

    }

    /**
     * Process any change window updates.
     *
     * @param changeWindows the list of all change windows.
     * @param cloudTopology the cloud topology being processed.
     * @param topologyInfo Info about the cloud topology.
     */
    private void handleScale(@Nonnull final List<ExecutedActionsChangeWindow> changeWindows,
                             final CloudTopology cloudTopology, final TopologyInfo topologyInfo) {
        // Create a mapping of entityOid to sorted set of scale change windows for the entity.
        Map<Long, TreeSet<ExecutedActionsChangeWindow>> entityOidToScaleWindows = changeWindows
                .stream()
                .filter(cw -> cw.getActionSpec().getRecommendation().getInfo().hasScale())
                .collect(Collectors
                        .groupingBy(
                                ExecutedActionsChangeWindow::getEntityOid,
                                Collectors
                                        .mapping(
                                                Function.identity(), Collectors
                                                        .toCollection(() ->
                                                                new TreeSet<ExecutedActionsChangeWindow>(
                                                                        new SortByCompletionTime())))));

        // Compile list of scale action liveness updates.
        try {
            for (TreeSet<ExecutedActionsChangeWindow> entityScaleWindows : entityOidToScaleWindows.values()) {
                final long currentTimestamp = topologyInfo.getCreationTime();
                final ExecutedActionsChangeWindow latestScaleWindowForEntity = entityScaleWindows.pollLast();
                if (latestScaleWindowForEntity == null) {
                    // This shouldn't happen as each entity in the treeset will contain at least one change window.
                    // This is a defensive check.
                    continue;
                }
                final ActionInfo actionInfo = latestScaleWindowForEntity.getActionSpec().getRecommendation().getInfo();
                final Optional<TopologyEntityDTO> primaryTier = cloudTopology.getPrimaryTier(latestScaleWindowForEntity
                        .getEntityOid());
                final List<ChangeProvider> changeProviders = ActionDTOUtil.getChangeProviderList(actionInfo);
                boolean destinationTierMatches = changeProviders.stream().anyMatch(cp -> primaryTier.isPresent()
                        && primaryTier.get().getOid() == cp.getDestination().getId());
                final long actionOid = latestScaleWindowForEntity.getActionOid();
                if (destinationTierMatches) {
                    // Add request to update Start Time and Liveness if current state is NEW.  If already LIVE, no need to
                    // update the latest action.
                    if (latestScaleWindowForEntity.getLivenessState() == LivenessState.NEW) {
                        logger.debug("Destination match present for action {}, tier {}, updating to LIVE", actionOid,
                                primaryTier.get().getOid());
                        savingsActionStore.activateAction(actionOid, currentTimestamp);
                    }
                    // Deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                    deactivateChangeWindows(actionOid, currentTimestamp, entityScaleWindows);
                } else {
                    // LIVE --> REVERTED
                    boolean sourceTierMatches = changeProviders.stream().anyMatch(cp -> primaryTier.isPresent()
                            && primaryTier.get().getOid() == cp.getSource().getId());
                    if (sourceTierMatches) {
                        if (latestScaleWindowForEntity.getLivenessState() == LivenessState.NEW) {
                            logger.debug("NEW executed action tier change not detected yet ..could be an error or the"
                                + " action got Reverted or the entity deleted, or the update will be in the next"
                                + " broadcast, action {} entity {}", actionOid, latestScaleWindowForEntity.getEntityOid());
                        } else if (latestScaleWindowForEntity.getLivenessState() == LivenessState.LIVE) {
                            logger.debug("Source match present for action {}, tier {}, updating to REVERTED", actionOid,
                                    primaryTier.get().getOid());
                            savingsActionStore.deactivateAction(actionOid, currentTimestamp, LivenessState.REVERTED);
                            // Deactivate older change windows, in case we missed updating them in previous broadcast cycles.
                            deactivateChangeWindows(actionOid, currentTimestamp, entityScaleWindows);
                        }
                    }
                }
            }
        } catch (SavingsException se) {
            // We still have elements in the set after processing the latest change window.
            logger.error("TEM2 Savings Exception in handleScale at {}",  topologyInfo.getCreationTime(), se);
        }
    }

    /**
     * Deactivate a collection of change windows.
     *
     * <p>This method is being used to deactivate older change windows for en entity by setting their LIVENESS state
     * to the default inactive state SUPERSEDED, unless they're already in another inactive state</p>
     * @param latestCwOid the most recent change window action oid for the entity.
     * @param timestamp the time when the action became inactive (timestamp of topology being processed)
     * @param changeWindows the change windows to deactivate (the older change windows for the entity).
     * @throws SavingsException if there's an error when deactivating an action.
     */
    private void deactivateChangeWindows(final long latestCwOid, final long timestamp,
                                         @Nonnull final TreeSet<ExecutedActionsChangeWindow> changeWindows)
                            throws SavingsException {
        // Ideally there should be only one NEW and one previous LIVE action, however if we missed any topology
        // updates, multiple previous actions could be stuck in NEW or LIVE states.  OR possibly multiple actions
        // were executed between two topology updates.  Unusual runtime exceptions could cause states to not be
        // updated as expected, as well. We add requests to update all of those actions to SUPERSEDED (the default
        // inactive state, there are other inactive states such as REVERTED, DELETED etc) here.
        for (ExecutedActionsChangeWindow prevChangeWindow : changeWindows) {
            // Change windows processed here would be active, since methods like handleScale only process NEW and
            // LIVE actions.
            // However we are double checking here, just in case we start processing actions already in REVERTED,
            // DELETED, SUPERSEDED ... states.  We don't want to update those.
            if (isActiveChangeWindow(prevChangeWindow)) {
                final long cwActionOid = prevChangeWindow.getActionOid();
                logger.debug("More recent NEW action match present for action {} which is {}, updating to SUPERSEDED",
                        cwActionOid, latestCwOid);
                savingsActionStore.deactivateAction(cwActionOid, timestamp, LivenessState.SUPERSEDED);
            }
        }
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
