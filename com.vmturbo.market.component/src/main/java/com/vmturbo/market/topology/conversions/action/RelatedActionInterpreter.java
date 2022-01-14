package com.vmturbo.market.topology.conversions.action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.MarketRelatedActionsList;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation.BlockedByResize;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation.CausedByProvision;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation.CausedBySuspension;
import com.vmturbo.common.protobuf.action.ActionDTO.MarketRelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.utils.GuestLoadFilters;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisResult;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.ActionRelationTypeCase;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.BlockedByRelation.BlockedByRelationTypeCase;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.CausedByRelation.CausedByRelationTypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to interpret related actions from ActionTO and return a map of action OID to
 * corresponding MarketRelatedActions list.
 */
public class RelatedActionInterpreter {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;
    private final String logPrefix;

    /**
     * RelatedActionInterpreter constructor.
     *
     * @param topologyInfo {@link TopologyInfo}
     */
    public RelatedActionInterpreter(@Nonnull final TopologyInfo topologyInfo) {
        this.topologyInfo = topologyInfo;
        logPrefix = String.format("%s topology [ID=%d, context=%d]:",
                topologyInfo.getTopologyType(), topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId());
    }

    /**
     * Interpret related actions on the interpreted actions based on RelatedActionTO in ActionTO.
     *
     * @param actionTOs                    Given list of ActionTO generated from analysis.
     * @param actions                      List of interpreted actions.
     * @param projectedEntities            Map of projected topologyEntity by entity OID.
     * @param namespaceQuotaAnalysisResult {@link NamespaceQuotaAnalysisResult} used to interpret related
     *                                     namespace resizing for container resizing actions.
     * @return Map of action ID to corresponding {@link MarketRelatedActionsList}.
     */
    public Map<Long, MarketRelatedActionsList> interpret(@Nonnull final List<ActionTO> actionTOs,
                                                         @Nonnull final List<Action> actions,
                                                         @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
                                                         @Nullable final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult) {
        final Map<Long, MarketRelatedActionsList> actionIdToRelatedActionsMap = new HashMap<>();

        final Map<Long, Action> actionsMap = actions.stream()
                .collect(Collectors.toMap(Action::getId, Function.identity()));
        actionTOs.stream()
                .filter(actionTO -> actionTO.getRelatedActionsCount() > 0
                        && actionsMap.containsKey(actionTO.getId()))
                .forEach(actionTO -> {
                    final Action action = actionsMap.get(actionTO.getId());
                    // Skip processing related action if target action entity is a guestLoad.
                    if (isGuestLoadAction(action, projectedEntities)) {
                        return;
                    }
                    final MarketRelatedActionsList.Builder relatedActionsListBuilder = MarketRelatedActionsList.newBuilder();
                    actionTO.getRelatedActionsList().forEach(relatedActionTO -> {
                        if (isContainerResizingWithNSRelatedAction(actionTO, relatedActionTO, projectedEntities,
                                namespaceQuotaAnalysisResult)) {
                            // If this is relatedActionTO from container resizing, relatedActionId
                            // doesn't exist in relatedActionTO because related namespace resizing
                            // actions are generated after M2 analysis.
                            // Interpret related action based on given namespaceQuotaAnalysisResult.
                            interpretContainerNSRelatedAction(relatedActionTO, namespaceQuotaAnalysisResult, projectedEntities)
                                    .ifPresent(relatedActionsListBuilder::addRelatedActions);
                        } else {
                            interpretRelatedAction(relatedActionTO, actionsMap, projectedEntities)
                                    .ifPresent(relatedActionsListBuilder::addRelatedActions);
                        }
                    });
                    if (relatedActionsListBuilder.getRelatedActionsCount() > 0) {
                        actionIdToRelatedActionsMap.put(actionTO.getId(), relatedActionsListBuilder.build());
                    }
                });
        return actionIdToRelatedActionsMap;
    }

    /**
     * Check if given action is generated on guest load entity.
     *
     * @param action            Given action to check if target entity is guest load.
     * @param projectedTopology Map of entity OID to projected TopologyEntity.
     * @return True if given action is generated on guest load entity.
     */
    private boolean isGuestLoadAction(@Nonnull final Action action,
                                      @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        try {
            ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
            ProjectedTopologyEntity entity = projectedTopology.get(actionEntity.getId());
            if (entity != null && GuestLoadFilters.isGuestLoad(entity.getEntity())) {
                return true;
            }
        } catch (UnsupportedActionException e) {
            logger.error("{} Failed to interpret related action due to unsupported action type", logPrefix, e);
        }
        return false;
    }

    /**
     * Check if given actionTO is container resizing action with related action which is blockedBy
     * namespace resizing.
     *
     * @param actionTO                     Map of {@link ActionTO} by action ID.
     * @param relatedActionTO              Given {@link RelatedActionTO} to check if it's related namespace resizing.
     * @param projectedEntities            Given map of projected entity OID to projectedTopologyEntity.
     * @param namespaceQuotaAnalysisResult {@link NamespaceQuotaAnalysisResult} which includes namespace
     *                                     resizing actions by entity OID and commodity type.
     * @return True if container resizing has related action which is blocked by namespace resizing.
     */
    private boolean isContainerResizingWithNSRelatedAction(
            @Nonnull final ActionTO actionTO,
            @Nonnull final RelatedActionTO relatedActionTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities,
            @Nullable final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult) {
        // Only process real-time container resizing actions with namespace related actions here
        // because all actions in plan are executable.
        if (namespaceQuotaAnalysisResult == null
                || topologyInfo.getTopologyType() == TopologyType.PLAN
                || !actionTO.hasResize()) {
            return false;
        }
        ProjectedTopologyEntity entity = projectedEntities.get(actionTO.getResize().getSellingTrader());
        if (entity == null) {
            return false;
        }
        if (entity.getEntity().getEntityType() != EntityType.CONTAINER_VALUE) {
            return false;
        }
        return isBlockedByNSRelatedAction(relatedActionTO, projectedEntities);
    }

    private boolean isBlockedByNSRelatedAction(@Nonnull final RelatedActionTO relatedActionTO,
                                               @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {

        return relatedActionTO.hasBlockedByRelation()
                && relatedActionTO.getBlockedByRelation().hasResize()
                && relatedActionTO.hasTargetTrader()
                && projectedEntities.containsKey(relatedActionTO.getTargetTrader())
                && projectedEntities.get(relatedActionTO.getTargetTrader()).getEntity().getEntityType() == EntityType.NAMESPACE_VALUE;
    }

    /**
     * Interpret related namespace resizing actions for corresponding container resizing actions and
     * store the map of container resizing action ID to list of related namespace resizing actions in
     * namespaceQuotaAnalysisResult.
     *
     * @param relatedActionTO              {@link RelatedActionTO} to be converted to {@link MarketRelatedAction}.
     * @param namespaceQuotaAnalysisResult @link NamespaceQuotaAnalysisResult}.
     * @param projectedEntities            Map of entity OID to projectedTopologyEntity.
     * @return Optional of {@link MarketRelatedAction}.
     */
    private Optional<MarketRelatedAction> interpretContainerNSRelatedAction(
            @Nonnull final RelatedActionTO relatedActionTO,
            @Nonnull final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
        long nsOID = projectedEntities.get(relatedActionTO.getTargetTrader()).getEntity().getOid();
        int commodityType = relatedActionTO.getBlockedByRelation()
                .getResize()
                .getCommodityType()
                .getType();
        Map<Integer, Action> nsActionsByCommodityType =
                namespaceQuotaAnalysisResult.getNSActionsByEntityOIDAndCommodityType().get(nsOID);
        if (nsActionsByCommodityType == null) {
            logger.error("{} Namespace {} doesn't have resizing action generated", logPrefix, nsOID);
            return Optional.empty();
        }
        Action nsAction = nsActionsByCommodityType.get(commodityType);
        if (nsAction == null) {
            logger.error("{} Namespace {} doesn't have resizing {} action generated", logPrefix, nsOID, commodityType);
            return Optional.empty();
        }
        return createMarketRelatedAction(relatedActionTO, nsAction);
    }

    /**
     * Interpret related action based on relatedActionTO and return optional of {@link MarketRelatedAction}.
     *
     * @param relatedActionTO   {@link RelatedActionTO} to be converted to {@link MarketRelatedAction}.
     * @param actionsMap        Map of action ID to action.
     * @param projectedEntities Map of entity OID to projected TopologyEntity.
     * @return Optional of {@link MarketRelatedAction}.
     */
    private Optional<MarketRelatedAction> interpretRelatedAction(@Nonnull final RelatedActionTO relatedActionTO,
                                                                 @Nonnull final Map<Long, Action> actionsMap,
                                                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
        if (!relatedActionTO.hasRelatedActionId()) {
            return Optional.empty();
        }
        final Action relatedAction = actionsMap.get(relatedActionTO.getRelatedActionId());
        if (relatedAction == null) {
            logger.error("{} Action {} does not exist in the interpreted actions",
                    logPrefix, relatedActionTO.getRelatedActionId());
            return Optional.empty();
        }
        // Skip processing related action if target action entity is a guestLoad.
        if (isGuestLoadAction(relatedAction, projectedEntities)) {
            return Optional.empty();
        }
        return createMarketRelatedAction(relatedActionTO, relatedAction);
    }

    /**
     * Create an optional of {@link MarketRelatedAction} based on given {@link RelatedActionTO} and
     * {@link Action}.
     *
     * @param relatedActionTO RelatedActionTO which provides action relation type generated from analysis.
     * @param relatedAction   Given action from which ActionEntity is extracted.
     * @return Optional of {@link MarketRelatedAction}.
     */
    private Optional<MarketRelatedAction> createMarketRelatedAction(@Nonnull final RelatedActionTO relatedActionTO,
                                                                    @Nonnull final Action relatedAction) {
        final MarketRelatedAction.Builder relatedActionBuilder = MarketRelatedAction.newBuilder()
                .setActionId(relatedAction.getId());
        try {
            relatedActionBuilder.setActionEntity(ActionDTOUtil.getPrimaryEntity(relatedAction));
        } catch (UnsupportedActionException e) {
            logger.error("{} Failed to create MarketRelatedAction", logPrefix, e);
            return Optional.empty();
        }

        final ActionRelationTypeCase actionRelationTypeCase = relatedActionTO.getActionRelationTypeCase();
        switch (actionRelationTypeCase) {
            case CAUSED_BY_RELATION:
                final CausedByRelation.Builder causedByRelationBuilder = CausedByRelation.newBuilder();
                final CausedByRelationTypeCase causedByRelationTypeCase =
                        relatedActionTO.getCausedByRelation().getCausedByRelationTypeCase();
                switch (causedByRelationTypeCase) {
                    case SUSPENSION:
                        causedByRelationBuilder.setSuspension(CausedBySuspension.newBuilder());
                        break;
                    case PROVISION:
                        causedByRelationBuilder.setProvision(CausedByProvision.newBuilder());
                        break;
                    default:
                        logger.error("{} Failed to create MarketRelatedAction {} due to unsupported CAUSED_BY_RELATION type {}",
                                logPrefix, relatedAction.getId(), causedByRelationTypeCase);
                        return Optional.empty();
                }
                relatedActionBuilder.setCausedByRelation(causedByRelationBuilder.build());
                break;
            case BLOCKED_BY_RELATION:
                final BlockedByRelation.Builder blockedByRelationBuilder = BlockedByRelation.newBuilder();
                final BlockedByRelationTypeCase blockedByRelationTypeCase =
                        relatedActionTO.getBlockedByRelation().getBlockedByRelationTypeCase();
                if (blockedByRelationTypeCase == BlockedByRelationTypeCase.RESIZE) {
                    blockedByRelationBuilder.setResize(BlockedByResize.newBuilder()
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(relatedActionTO.getBlockedByRelation().getResize().getCommodityType().getBaseType())
                                    .build()));
                } else {
                    logger.error("{} Failed to create MarketRelatedAction {} due to unsupported BLOCKED_BY_RELATION type {}",
                            logPrefix, relatedAction.getId(), blockedByRelationTypeCase);
                    return Optional.empty();
                }
                relatedActionBuilder.setBlockedByRelation(blockedByRelationBuilder.build());
                break;
            default:
                logger.error("{} Failed to create MarketRelatedAction {} due to unsupported action relation type {}",
                        logPrefix, relatedAction.getId(), actionRelationTypeCase);
                return Optional.empty();
        }
        return Optional.of(relatedActionBuilder.build());
    }
}
