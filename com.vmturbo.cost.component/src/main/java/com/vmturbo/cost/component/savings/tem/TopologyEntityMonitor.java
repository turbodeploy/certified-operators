package com.vmturbo.cost.component.savings.tem;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;

/**
 * Sets Liveness State and Start, End Times of Executed Actions based on topology updates.
 */
public class TopologyEntityMonitor implements LiveCloudTopologyListener {
    private final Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionsRpcService;

    /**
     * Constructor.
     *
     * @param actionsRpcService Actions Rpc Service.
     */
    public TopologyEntityMonitor(@Nonnull final ActionsServiceBlockingStub actionsRpcService) {
        this.actionsRpcService = Objects.requireNonNull(actionsRpcService);
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
        logger.info("Billed Savings Entity Monitor processing topology updates");
        final GetActionChangeWindowRequest getActionChangeWindowsRequest = GetActionChangeWindowRequest.newBuilder()
                .addLivenessStates(LivenessState.NEW)
                .addLivenessStates(LivenessState.LIVE)
                .build();
        List<ExecutedActionsChangeWindow> changeWindows = new ArrayList<>();
        actionsRpcService.getActionChangeWindows(getActionChangeWindowsRequest).forEachRemaining(changeWindows::add);

        final List<ActionLivenessInfo> actionLivenessUpdates = handleScale(changeWindows, cloudTopology,
                topologyInfo);

        // Check if any updates were added, for the actions processed.
        if (actionLivenessUpdates.isEmpty()) {
            logger.debug("No suitable updates for executed actions LIVENESS state found in this topology update at {}",
                    topologyInfo.getCreationTime());
        } else {
            // Call rpc with full list of updates.
            updateLivenessState(actionLivenessUpdates);
        }
    }

    /**
     * Create/ Add to list of LivenessState updates for executed actions.
     *
     * @param actionOid              The action Oid of the action.
     * @param livenessState          The LivenessState to update to.
     * @param timeStamp              The start_time of the actioon Liveness.
     * @return ActionLivenessInfo    An ActionLivenessInfo staged to be passed to the update request with the full list
     * of ActionLivenessInfo updates.
     */
    @VisibleForTesting
    ActionLivenessInfo createLivenessUpdate(@Nonnull final Long actionOid, @Nonnull final LivenessState livenessState,
                                            final Long timeStamp) {
        final ActionLivenessInfo actionLivenessInfo = ActionLivenessInfo.newBuilder()
                .setActionOid(actionOid)
                .setLivenessState(livenessState)
                .setTimestamp(timeStamp).build();
        return actionLivenessInfo;
    }

    /**
     * Update the LivenessState of an executed action.
     *
     * <p>This method takes a compiled list of action liveness updates every cycle, and makes one update request to
     * update the executed actions liveness state changes in the database.
     *
     * @param actionLivenessInfoList The list of Liveness State updates to process.
     */
    @VisibleForTesting
    void updateLivenessState(@Nonnull List<ActionLivenessInfo> actionLivenessInfoList) {
        final UpdateActionChangeWindowRequest updateActionChangeWindowRequest = UpdateActionChangeWindowRequest
                .newBuilder()
                .addAllLivenessInfo(actionLivenessInfoList)
                .build();
        actionsRpcService.updateActionChangeWindows(updateActionChangeWindowRequest);
    }

    /**
     * Process any change window updates.
     *
     * @param changeWindows the list of all change windows.
     * @param cloudTopology the cloud topology being processed.
     * @param topologyInfo Info about the cloud topology.
     * @return List of updates to be passed to the rpc to update Liveness States.
     */
    private List<ActionLivenessInfo> handleScale(@Nonnull final List<ExecutedActionsChangeWindow> changeWindows,
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

        List<ActionLivenessInfo> actionLivenessUpdates = new ArrayList<>();
        // Compile list of scale action liveness updates.
        for (TreeSet<ExecutedActionsChangeWindow> entityScaleWindows : entityOidToScaleWindows.values()) {
            try {
                final Long currentTimestamp = topologyInfo.getCreationTime();
                final ExecutedActionsChangeWindow latestScaleWindowForEntity = entityScaleWindows.pollLast();
                final ActionInfo actionInfo = latestScaleWindowForEntity.getActionSpec().getRecommendation().getInfo();
                final Optional<TopologyEntityDTO> primaryTier = cloudTopology.getPrimaryTier(latestScaleWindowForEntity
                        .getEntityOid());
                final List<ChangeProvider> changeProviders = ActionDTOUtil.getChangeProviderList(actionInfo);
                boolean destinationTierMatches = changeProviders.stream().anyMatch(cp -> primaryTier.isPresent()
                        && primaryTier.get().getOid() == cp.getDestination().getId());
                final Long actionOid = latestScaleWindowForEntity.getActionOid();
                if (destinationTierMatches) {
                    logger.debug("Destination match present for action {}, tier {}, updating to LIVE", actionOid,
                            primaryTier.get().getOid());
                    // Add request to pdate Start Time and Liveness if current state is NEW.  If already LIVE, no need to
                    // update the latest action.
                    if (latestScaleWindowForEntity.getLivenessState() == LivenessState.NEW) {
                        actionLivenessUpdates.add(createLivenessUpdate(actionOid, LivenessState.LIVE, currentTimestamp));
                    }
                    // Set the rest of the change windows for the entity to SUPERSEDED.
                    // Ideally there should be only one NEW and one previous LIVE action, however if we missed any topology
                    // updates, multiple previous actions could be stuck in NEW or LIVE states.  OR possibly multiple actions
                    // were executed between two topology updates.  Unusual runtime exceptions could cause states to not be
                    // updated as expected as well. We add requests to update all of those actions to SUPERSEDED state here.
                    for (ExecutedActionsChangeWindow prevChangeWindowForEntity : entityScaleWindows) {
                        final Long cwActionOid = prevChangeWindowForEntity.getActionOid();
                        logger.debug("More recent NEW action match present for action {} which is {}, updating to SUPERSEDED",
                                cwActionOid, actionOid);
                        actionLivenessUpdates.add(createLivenessUpdate(cwActionOid, LivenessState.SUPERSEDED, currentTimestamp));
                    }
                } else {
                    logger.debug("NEW executed action destination change not detected yet ..could be an error or the action"
                            + " {} got reverted or the entity deleted, or the update will be in the next broadcast,"
                            + " entity {}", actionOid, latestScaleWindowForEntity.getEntityOid());
                }
            } catch (Exception e) {
                Iterator<ExecutedActionsChangeWindow> entityScaleWindowsIterator = entityScaleWindows.iterator();
                final ExecutedActionsChangeWindow cwWithProcessingException = entityScaleWindowsIterator.next();
                logger.error("Exception processing update for entity {}, action {}in Liveness State {}",
                        cwWithProcessingException.getEntityOid(), cwWithProcessingException.getActionOid(),
                        cwWithProcessingException.getLivenessState());
            }
        }
        return actionLivenessUpdates;
    }
}
