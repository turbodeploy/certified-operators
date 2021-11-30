package com.vmturbo.extractor.action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.commons.Units;
import com.vmturbo.extractor.action.commodity.ActionCommodityData;
import com.vmturbo.extractor.action.commodity.ActionCommodityDataRetriever;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.ActionEntity;
import com.vmturbo.extractor.schema.json.common.ActionImpactedCosts;
import com.vmturbo.extractor.schema.json.common.ActionImpactedCosts.ActionImpactedCost;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ImpactedMetric;
import com.vmturbo.extractor.schema.json.common.BuyRiInfo;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.EntitySettings;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory.RICoverageData;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Responsible for extracting action attributes (mostly type-specific info) shared by the
 * reporting and data extraction code (in {@link com.vmturbo.extractor.schema.json.common}.
 */
public class ActionAttributeExtractor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Set of action types for which the impacted metrics for target entity will be populated.
     */
    public static final Set<ActionType> ACTION_TYPES_TO_POPULATE_TARGET_METRIC_IMPACT = ImmutableSet.of(
            ActionType.START, ActionType.ACTIVATE, ActionType.SCALE, ActionType.PROVISION,
            ActionType.DELETE, ActionType.ALLOCATE);

    private final ActionCommodityDataRetriever actionCommodityDataRetriever;
    private final DataProvider dataProvider;
    private final VolumeAttachmentHistoryRetriever volumeAttachmentHistoryRetriever;

    ActionAttributeExtractor(@Nonnull final ActionCommodityDataRetriever actionCommodityDataRetriever,
                             @Nonnull final DataProvider dataProvider,
                             @Nonnull final VolumeAttachmentHistoryRetriever volumeAttachmentHistoryRetriever) {
        this.actionCommodityDataRetriever = actionCommodityDataRetriever;
        this.dataProvider = dataProvider;
        this.volumeAttachmentHistoryRetriever = volumeAttachmentHistoryRetriever;
    }

    /**
     * Extract {@link ActionAttributes} from a collection of {@link ActionSpec}s.
     *
     * @param actionSpecs The {@link ActionSpec}s, arranged by id.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @return The {@link ActionAttributes}, arranged by id (as in the actionSpecs input).
     *         Any actions that fail to be mapped will be absent from this map.
     */
    @Nonnull
    public Long2ObjectMap<ReportingActionAttributes> extractReportingAttributes(
            @Nonnull final List<ActionSpec> actionSpecs,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph) {
        final Long2ObjectMap<ReportingActionAttributes> attrs = new Long2ObjectOpenHashMap<>(actionSpecs.size());
        actionSpecs.forEach(e -> attrs.put(e.getRecommendation().getId(), new ReportingActionAttributes()));
        populateAttributes(actionSpecs, topologyGraph, attrs, this::processResizeChanges,
                this::processMoveChanges, this::processScaleChanges);
        return attrs;
    }

    /**
     * Populate the {@link ActionAttributes} for exported actions, and return a list of actions
     * with attributes populated.
     *
     * @param actionSpecs List of {@link ActionSpec}s containing info for attributes
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @param actions The list of {@link Action}s to populate, arranged by id.
     * @return list of {@link Action}s with attributes populated
     */
    @Nonnull
    public List<Action> populateExporterAttributes(@Nonnull final List<ActionSpec> actionSpecs,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull final Long2ObjectMap<Action> actions) {
        return populateAttributes(actionSpecs, topologyGraph, actions,
                this::flattenAtomicResizeAction, this::flattenCompoundMoveAction, this::flattenScaleAction);
    }

    /**
     * Populate the {@link ActionAttributes} in an action, and return a list of actions or
     * attributes.
     *
     * @param actionSpecs The {@link ActionSpec}s, arranged by id.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @param attributes The attributes to populate.
     * @param processMoveChanges function which tells how to handle move changes in a single action
     * @param processResizeChanges function which tells how to handle resize changes in a single action
     * @param processScaleChanges function which tells how to handle scale changes in a single action
     * @param <T> The subtype of {@link ActionAttributes} returned by this method.
     * @return list of Actions or ActionAttributes after being populated
     */
    private <T extends ActionAttributes> List<T> populateAttributes(
            @Nonnull final List<ActionSpec> actionSpecs,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull final Long2ObjectMap<T> attributes,
            @Nonnull BiFunction<T, List<CommodityChange>, List<T>> processResizeChanges,
            @Nonnull BiFunction<T, List<MoveChange>, List<T>> processMoveChanges,
            @Nonnull BiFunction<T, List<MoveChange>, List<T>> processScaleChanges) {
        final Map<Long, Integer> unattachedDaysMap = volumeAttachmentHistoryRetriever.getVolumeAttachmentDays(actionSpecs);

        final List<T> result = new ArrayList<>();
        final ActionCommodityData commodityChanges =
                actionCommodityDataRetriever.getActionCommodityData(actionSpecs);
        actionSpecs.forEach(actionSpec -> {
            final T actionAttrs = attributes.get(actionSpec.getRecommendation().getId());
            if (actionAttrs != null) {
                result.addAll(populateAttributes(actionSpec, actionAttrs, topologyGraph,
                        commodityChanges, processResizeChanges, processMoveChanges, processScaleChanges, unattachedDaysMap));
            }
        });
        return result;
    }

    private <T extends ActionAttributes> List<T> populateAttributes(ActionSpec actionSpec,
            T actionOrAttributes,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            ActionCommodityData actionCommodityData,
            BiFunction<T, List<CommodityChange>, List<T>> processResizeChanges,
            BiFunction<T, List<MoveChange>, List<T>> processMoveChanges,
            BiFunction<T, List<MoveChange>, List<T>> processScaleChanges,
            Map<Long, Integer> unattachedDaysMap) {
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(recommendation);
        final ActionDTO.ActionInfo actionInfo = recommendation.getInfo();

        // We only populate metric impact for pending actions.
        // Metric impact doesn't really make sense in the context of completed actions.
        final boolean populateMetricImpact = actionSpec.getActionState() == ActionState.READY;

        // set target and related
        try {
            ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(recommendation);
            final boolean populateTargetMetricImpact = populateMetricImpact
                    && ACTION_TYPES_TO_POPULATE_TARGET_METRIC_IMPACT.contains(actionType);
            // we populate before/after costs for both pending and completed actions
            ActionImpactedEntity targetEntity = buildImpactedEntity(primaryEntity,
                    populateTargetMetricImpact, true, getCloudSavingsDetails(actionType, actionInfo),
                    topologyGraph, actionCommodityData);
            actionOrAttributes.setTarget(targetEntity);
        } catch (UnsupportedActionException e) {
            // this should not happen
            logger.error("Unable to get primary entity for unsupported action {}", actionSpec, e);
        }
        switch (actionType) {
            case MOVE:
                return processMoveChanges.apply(actionOrAttributes,
                        getMoveChanges(recommendation, populateMetricImpact, topologyGraph, actionCommodityData));
            case SCALE:
                return processScaleChanges.apply(actionOrAttributes,
                        getMoveChanges(recommendation, populateMetricImpact, topologyGraph, actionCommodityData));
            case RESIZE:
                final List<CommodityChange> resizeChanges;
                if (actionInfo.hasAtomicResize()) {
                    resizeChanges = getAtomicResizeChanges(actionInfo.getAtomicResize(), topologyGraph,
                            actionCommodityData);
                } else {
                    resizeChanges = Collections.singletonList(getNormalResizeChange(
                            actionInfo.getResize(), actionCommodityData));
                }
                return processResizeChanges.apply(actionOrAttributes, resizeChanges);
            case DELETE:
                actionOrAttributes.setDeleteInfo(getDeleteInfo(actionInfo.getDelete(),
                                                                recommendation.getExplanation().getDelete(),
                                                                unattachedDaysMap));
                return Collections.singletonList(actionOrAttributes);
            case BUY_RI:
                actionOrAttributes.setBuyRiInfo(getBuyRiInfo(actionInfo.getBuyRi(), topologyGraph));
                return Collections.singletonList(actionOrAttributes);
            // add additional info for other action types if needed
            default:
                return Collections.singletonList(actionOrAttributes);
        }
    }

    private Optional<CloudSavingsDetails> getCloudSavingsDetails(ActionType actionType,
            ActionDTO.ActionInfo actionInfo) {
        switch (actionType) {
            case SCALE:
                if (actionInfo.getScale().hasCloudSavingsDetails()) {
                    return Optional.of(actionInfo.getScale().getCloudSavingsDetails());
                }
                break;
            case ALLOCATE:
                if (actionInfo.getAllocate().hasCloudSavingsDetails()) {
                    return Optional.of(actionInfo.getAllocate().getCloudSavingsDetails());
                }
                break;
        }
        return Optional.empty();
    }

    /**
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} with
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return {@link ActionEntity}
     */
    @Nonnull
    public static ActionEntity getActionEntityWithType(ActionDTO.ActionEntity actionEntity,
            TopologyGraph<SupplyChainEntity> topologyGraph) {
        final ActionEntity ae = getActionEntityWithoutType(actionEntity, topologyGraph);
        ae.setType(ExportUtils.getEntityTypeJsonKey(actionEntity.getType()));
        return ae;
    }

    private ActionImpactedEntity buildImpactedEntity(ActionDTO.ActionEntity actionEntity,
            final boolean populateMetricImpact,
            final boolean populateCostImpact,
            Optional<CloudSavingsDetails> cloudSavingsDetails,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            ActionCommodityData actionCommodityData) {
        ActionImpactedEntity entity = new ActionImpactedEntity();
        // TODO: refactor common code between ActionEntity and ActionImpactedEntity
        entity.setOid(actionEntity.getId());
        entity.setType(ExportUtils.getEntityTypeJsonKey(actionEntity.getType()));
        if (topologyGraph != null) {
            topologyGraph.getEntity(actionEntity.getId())
                    .ifPresent(e -> entity.setName(e.getDisplayName()));
        }
        if (populateMetricImpact) {
            final Map<String, ImpactedMetric> entityImpact = actionCommodityData.getEntityImpact(actionEntity.getId());
            entity.setAffectedMetrics(entityImpact);
            // add percentile settings to the target entity level if any of the affected metrics has percentile data
            if (entityImpact != null && entityImpact.values().stream()
                    .anyMatch(impactedMetric -> impactedMetric.getBeforeActions() != null
                            && impactedMetric.getBeforeActions().getPercentileUtilization() != null)) {
                actionCommodityDataRetriever.getPercentileSetting(actionEntity.getId(), actionEntity.getType())
                        .ifPresent(percentileSetting -> {
                            EntitySettings entitySettings = new EntitySettings();
                            entitySettings.setPercentileAggressiveness(percentileSetting.getAggresiveness());
                            entitySettings.setPercentileObservationPeriodDays(percentileSetting.getObservationPeriod());
                            entity.setSettings(entitySettings);
                        });
            }
        }
        if (populateCostImpact) {
            entity.setAffectedCosts(getAffectedCosts(actionEntity.getId(), actionEntity.getType(),
                    cloudSavingsDetails));
        }
        return entity;
    }

    @Nullable
    private ActionImpactedCosts getAffectedCosts(long entityId, int entityType,
            Optional<CloudSavingsDetails> optCloudSavingsDetails) {
        // if cloudSavingsDetails is available on action dto, then get before/after costs from
        // it directly, otherwise get from the projected costs fetched from cost component
        if (optCloudSavingsDetails.isPresent()) {
            final CloudSavingsDetails cloudSavingsDetails = optCloudSavingsDetails.get();

            final TierCostDetails sourceTierCostDetails = cloudSavingsDetails.getSourceTierCostDetails();
            final ActionImpactedCost beforeCost = new ActionImpactedCost();
            if (sourceTierCostDetails.getOnDemandCost().hasAmount()) {
                beforeCost.setOnDemandCost((float)sourceTierCostDetails.getOnDemandCost().getAmount());
            }
            if (sourceTierCostDetails.getOnDemandRate().hasAmount()) {
                beforeCost.setOnDemandRate((float)sourceTierCostDetails.getOnDemandRate().getAmount());
            }
            if (sourceTierCostDetails.hasCloudCommitmentCoverage()) {
                CloudCommitmentCoverage coverage = sourceTierCostDetails.getCloudCommitmentCoverage();
                beforeCost.setRiCoveragePercentage(ExportUtils.toPercentage(
                        coverage.getUsed().getCoupons(), coverage.getCapacity().getCoupons()));
            }

            final TierCostDetails projectedTierCostDetails = cloudSavingsDetails.getProjectedTierCostDetails();
            final ActionImpactedCost afterCost = new ActionImpactedCost();
            if (projectedTierCostDetails.getOnDemandCost().hasAmount()) {
                afterCost.setOnDemandCost((float)projectedTierCostDetails.getOnDemandCost().getAmount());
            }
            if (projectedTierCostDetails.getOnDemandRate().hasAmount()) {
                afterCost.setOnDemandRate((float)projectedTierCostDetails.getOnDemandRate().getAmount());
            }
            if (projectedTierCostDetails.hasCloudCommitmentCoverage()) {
                CloudCommitmentCoverage coverage = projectedTierCostDetails.getCloudCommitmentCoverage();
                afterCost.setRiCoveragePercentage(ExportUtils.toPercentage(
                        coverage.getUsed().getCoupons(), coverage.getCapacity().getCoupons()));
            }

            ActionImpactedCosts actionImpactedCosts = new ActionImpactedCosts();
            actionImpactedCosts.setBeforeActions(beforeCost);
            actionImpactedCosts.setAfterActions(afterCost);
            return actionImpactedCosts;
        }

        final BottomUpCostData currentCostData = dataProvider.getBottomUpCostData();
        final BottomUpCostData projectedCostData = dataProvider.getProjectedBottomUpCostData();
        final RICoverageData currentRiCoverageData = dataProvider.getCurrentRiCoverageData();
        final RICoverageData projectedRiCoverageData = dataProvider.getProjectedRiCoverageData();

        if (currentCostData == null || projectedCostData == null
                || currentRiCoverageData == null || projectedRiCoverageData == null) {
            logger.debug("Cost data is not available yet");
            return null;
        }

        final ActionImpactedCost beforeCost = new ActionImpactedCost();
        final ActionImpactedCost afterCost = new ActionImpactedCost();

        // on demand cost
        Optional<Float> currentOnDemandCost = currentCostData.getOnDemandCost(entityId, entityType);
        if (!currentOnDemandCost.isPresent()) {
            // it's expected that onprem entity doesn't have cost
            logger.debug("No on demand cost data for entity {}", entityId);
            return null;
        }
        // on demand rate
        Optional<Float> currentOnDemandRate = currentCostData.getOnDemandRate(entityId, entityType);
        if (!currentOnDemandRate.isPresent() ) {
            logger.debug("No on demand rate data for entity {}", entityId);
            return null;
        }

        // projected cost may not exist for some entities like delete volume action, in this case
        // we use 0 as the projected cost.
        final float projectedOnDemandCost = projectedCostData.getOnDemandCost(entityId, entityType).orElse(0f);
        final float projectedOnDemandRate = projectedCostData.getOnDemandRate(entityId, entityType).orElse(0f);

        beforeCost.setOnDemandRate(currentOnDemandRate.get());
        afterCost.setOnDemandRate(projectedOnDemandRate);
        beforeCost.setOnDemandCost(currentOnDemandCost.get());
        afterCost.setOnDemandCost(projectedOnDemandCost);

        // ri coverage
        float riCoveragePercentageBefore = currentRiCoverageData.getRiCoveragePercentage(entityId);
        float riCoveragePercentageAfter = projectedRiCoverageData.getRiCoveragePercentage(entityId);
        if (riCoveragePercentageBefore != -1 && riCoveragePercentageAfter != -1) {
            beforeCost.setRiCoveragePercentage(riCoveragePercentageBefore);
            afterCost.setRiCoveragePercentage(riCoveragePercentageAfter);
        }

        ActionImpactedCosts actionImpactedCosts = new ActionImpactedCosts();
        actionImpactedCosts.setBeforeActions(beforeCost);
        actionImpactedCosts.setAfterActions(afterCost);
        return actionImpactedCosts;
    }

    /**
     * Get a list of move changes the given action. For normal moves, it will contain a single
     * move change, but it may return multiple for compound moves.
     *
     * @param recommendation action from AO
     * @param populateMetricImpact If before/after metrics for involved entities should be set.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @param actionCommodityData Used to look up commodity information.
     * @return map of move change by entity type
     */
    private List<MoveChange> getMoveChanges(ActionDTO.Action recommendation,
            final boolean populateMetricImpact,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            ActionCommodityData actionCommodityData) {
        final List<MoveChange> moveInfo = new ArrayList<>();
        for (ChangeProvider change : ActionDTOUtil.getChangeProviderList(recommendation)) {
            final MoveChange moveChange = new MoveChange();
            // no need to populate cost for provider in scale action like ComputeTier
            moveChange.setFrom(buildImpactedEntity(change.getSource(), populateMetricImpact,
                    false, Optional.empty(), topologyGraph, actionCommodityData));
            moveChange.setTo(buildImpactedEntity(change.getDestination(), populateMetricImpact,
                    false, Optional.empty(), topologyGraph, actionCommodityData));
            // resource (like volume of a VM)
            if (!change.getResourceList().isEmpty()) {
                moveChange.setResource(change.getResourceList().stream()
                        .map(rsrc -> getActionEntityWithType(rsrc, topologyGraph))
                        .collect(Collectors.toList()));
            }
            moveInfo.add(moveChange);
        }
        return moveInfo;
    }

    /**
     * Get the commodity change for the given resize action.
     *
     * @param resize action from AO
     * @param actionCommodityData Used to look up percentile changes for the resized commodities.
     * @return commodity change
     */
    @Nonnull
    private CommodityChange getNormalResizeChange(Resize resize,
            @Nonnull final ActionCommodityData actionCommodityData) {
        final int commodityTypeInt = resize.getCommodityType().getType();
        final CommodityChange commodityChange = new CommodityChange();
        commodityChange.setFrom(resize.getOldCapacity());
        commodityChange.setTo(resize.getNewCapacity());
        if (resize.hasCommodityAttribute()) {
            commodityChange.setAttribute(resize.getCommodityAttribute().name());
        }
        commodityChange.setPercentileChange(
                actionCommodityData.getPercentileChange(resize.getTarget().getId(), resize.getCommodityType()));
        CommodityTypeMapping.getCommodityUnitsForActions(commodityTypeInt, null)
                .ifPresent(commodityChange::setUnit);
        commodityChange.setCommodityType(ExportUtils.getCommodityTypeJsonKey(commodityTypeInt));
        return commodityChange;
    }

    /**
     * Get the list of commodity changes for the given AtomicResize action.
     *
     * @param atomicResize action from AO
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @param actionCommodityData Used to look up percentile changes for the resized commodities.
     * @return list of commodity changes
     */
    @Nonnull
    private List<CommodityChange> getAtomicResizeChanges(AtomicResize atomicResize,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            ActionCommodityData actionCommodityData) {
        final List<CommodityChange> resizeInfo = new ArrayList<>();
        for (ResizeInfo resize : atomicResize.getResizesList()) {
            final CommodityChange commodityChange = new CommodityChange();
            commodityChange.setFrom(resize.getOldCapacity());
            commodityChange.setTo(resize.getNewCapacity());
            if (resize.hasCommodityAttribute()) {
                commodityChange.setAttribute(resize.getCommodityAttribute().name());
            }
            final int commodityTypeInt = resize.getCommodityType().getType();

            CommodityTypeMapping.getCommodityUnitsForActions(commodityTypeInt, resize.getTarget().getType())
                    .ifPresent(commodityChange::setUnit);

            // set target (where this commodity comes from) for each sub action, if it's different from main target
            if (resize.getTarget().getId() != atomicResize.getExecutionTarget().getId()) {
                commodityChange.setTarget(getActionEntityWithType(resize.getTarget(), topologyGraph));
            }
            commodityChange.setPercentileChange(
                    actionCommodityData.getPercentileChange(resize.getTarget().getId(), resize.getCommodityType()));
            commodityChange.setCommodityType(ExportUtils.getCommodityTypeJsonKey(commodityTypeInt));
            resizeInfo.add(commodityChange);
        }
        return resizeInfo;
    }

    /**
     * Get the type specific info for delete action.
     *
     * @param delete delete action
     * @param deleteExplanation delete explanation
     * @param unattachedDaysMap map contains [volumeOid, unattachedDays]
     * @return {@link DeleteInfo}
     */
    private DeleteInfo getDeleteInfo(Delete delete, DeleteExplanation deleteExplanation, Map<Long, Integer> unattachedDaysMap) {
        DeleteInfo deleteInfo = new DeleteInfo();
        if (deleteExplanation.getSizeKb() > 0) {
            deleteInfo.setFileSize(deleteExplanation.getSizeKb() / (double)Units.NUM_OF_KB_IN_MB);
            deleteInfo.setUnit("MB");
        }
        if (delete.hasFilePath()) {
            deleteInfo.setFilePath(delete.getFilePath());
        }
        if (deleteExplanation.hasModificationTimeMs()) {
            deleteInfo.setLastModifiedTimestamp(ExportUtils.getFormattedDate(
                    deleteExplanation.getModificationTimeMs()));
        }

        Long volumeOid = delete.getTarget().getId();
        Integer unattachedDays = unattachedDaysMap.get(volumeOid);
        if (unattachedDays != null) {
            deleteInfo.setUnattachedDays(unattachedDays);
        }
        return deleteInfo;
    }

    /**
     * Get the type specific info for buyRI action.
     *
     * @param buyRi the buyRI action from AO
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return {@link BuyRiInfo}
     */
    private BuyRiInfo getBuyRiInfo(BuyRI buyRi, TopologyGraph<SupplyChainEntity> topologyGraph) {
        BuyRiInfo buyRiInfo = new BuyRiInfo();
        if (buyRi.hasCount()) {
            buyRiInfo.setCount(buyRi.getCount());
        }
        if (buyRi.hasComputeTier()) {
            buyRiInfo.setComputeTier(getActionEntityWithoutType(buyRi.getComputeTier(), topologyGraph));
        }
        if (buyRi.hasRegion()) {
            buyRiInfo.setRegion(getActionEntityWithoutType(buyRi.getRegion(), topologyGraph));
        }
        if (buyRi.hasMasterAccount()) {
            buyRiInfo.setMasterAccount(getActionEntityWithoutType(buyRi.getMasterAccount(), topologyGraph));
        }
        if (buyRi.hasTargetEntity()) {
            buyRiInfo.setTarget(getActionEntityWithoutType(buyRi.getTargetEntity(), topologyGraph));
        }
        return buyRiInfo;
    }

    /**
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} without
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return {@link ActionEntity}
     */
    private static ActionEntity getActionEntityWithoutType(ActionDTO.ActionEntity actionEntity,
                                                    @Nullable TopologyGraph<SupplyChainEntity> topologyGraph) {
        final ActionEntity ae = new ActionEntity();
        ae.setOid(actionEntity.getId());
        if (topologyGraph != null) {
            topologyGraph.getEntity(actionEntity.getId())
                .ifPresent(e -> ae.setName(e.getDisplayName()));
        }
        return ae;
    }

    private List<ReportingActionAttributes> processResizeChanges(
            ReportingActionAttributes actionAttributes, List<CommodityChange> commodityChanges) {
        final Map<String, CommodityChange> resizeInfo = commodityChanges.stream()
                .collect(Collectors.toMap(CommodityChange::getCommodityType,
                        commodityChange -> {
                            // clear commodity type since it's indicated in the key
                            commodityChange.setCommodityType(null);
                            return commodityChange;
                        },
                        (a, b) -> {
                            logger.warn("Detected another resize for same commodity type {}, target1: {}, target2: {}",
                                    a.getCommodityType(), a.getTarget(), b.getTarget());
                            return a;
                        }));
        actionAttributes.setResizeInfo(resizeInfo);
        return Collections.singletonList(actionAttributes);
    }

    private List<ReportingActionAttributes> processMoveChanges(
            ReportingActionAttributes actionAttributes, List<MoveChange> moveChanges) {
        final Map<String, MoveChange> moveInfo = new HashMap<>();
        final Map<String, Integer> lastIndex = new HashMap<>();
        moveChanges.forEach(moveChange -> {
            String providerType = moveChange.getFrom().getType();
            // if multiple changes for same type of provider, key will be: STORAGE, STORAGE_1, etc.
            // (though I haven't see such a case in real environment or customer diags)
            if (moveInfo.containsKey(providerType)) {
                Integer newIndex = lastIndex.getOrDefault(providerType, 0) + 1;
                lastIndex.put(providerType, newIndex);
                providerType = providerType + "_" + newIndex;
            }
            // clear type since it's already specified in map key
            moveChange.getFrom().setType(null);
            moveChange.getTo().setType(null);
            moveInfo.put(providerType, moveChange);
        });
        actionAttributes.setMoveInfo(moveInfo);
        return Collections.singletonList(actionAttributes);
    }

    private List<ReportingActionAttributes> processScaleChanges(
            ReportingActionAttributes actionAttributes, List<MoveChange> scaleChanges) {
        final Map<String, MoveChange> scaleInfo = scaleChanges.stream()
                .collect(Collectors.toMap(scaleChange -> scaleChange.getFrom().getType(),
                        moveChange -> moveChange,
                        (a, b) -> {
                            logger.warn("Detected another scale for same provider type {}, provider1: {}, provider2: {}",
                                    a.getFrom().getType(), a.getFrom(), b.getFrom());
                            return a;
                        }));
        actionAttributes.setScaleInfo(scaleInfo);
        return Collections.singletonList(actionAttributes);
    }

    /**
     * Flatten the given action into multiple actions, one for each CommodityChange. The flattened
     * actions contain same fields except the resizeInfo.
     *
     * @param action {@link Action}
     * @param commodityChanges list of commodity changes in the given action
     * @return list of flattened actions
     */
    private List<Action> flattenAtomicResizeAction(Action action, List<CommodityChange> commodityChanges) {
        return commodityChanges.stream()
                .map(commodityChange -> {
                    Action actionCopy = shallowCopyWithoutAttrs(action);
                    actionCopy.setResizeInfo(commodityChange);
                    return actionCopy;
                }).collect(Collectors.toList());
    }

    /**
     * Flatten the given action into multiple actions, one for each MoveChange. The flattened
     * actions contain same fields except the moveInfo.
     *
     * @param action {@link Action}
     * @param moveChanges list of move changes in the given action
     * @return list of flattened actions
     */
    private List<Action> flattenCompoundMoveAction(Action action, List<MoveChange> moveChanges) {
        return moveChanges.stream()
                .map(moveChange -> {
                    Action actionCopy = shallowCopyWithoutAttrs(action);
                    actionCopy.setMoveInfo(moveChange);
                    return actionCopy;
                }).collect(Collectors.toList());
    }

    /**
     * Flatten the given action into multiple actions, one for each MoveChange. It seems so far
     * there is only one MoveChange in a single scale action.
     *
     * @param action {@link Action}
     * @param scaleChanges list of scale changes in the given action
     * @return list of flattened actions
     */
    private List<Action> flattenScaleAction(Action action, List<MoveChange> scaleChanges) {
        // scale changes may be empty for some actions like "Scale up IOPS for Volume vol1 on
        // Managed Ultra SSD from 613 IOPS to 876 IOPS", we should still return them
        if (scaleChanges.isEmpty()) {
            return Collections.singletonList(action);
        }
        return scaleChanges.stream()
                .map(scaleChange -> {
                    Action actionCopy = shallowCopyWithoutAttrs(action);
                    actionCopy.setScaleInfo(scaleChange);
                    return actionCopy;
                }).collect(Collectors.toList());
    }

    /**
     * Create a shallow copy of given action, without any type specific attrs.
     *
     * @param action the action to create shallow copy for
     * @return shallow copy of given action without type specific attrs
     */
    private static Action shallowCopyWithoutAttrs(Action action) {
        Action shallowCopy = new Action();
        shallowCopy.setOid(action.getOid());
        shallowCopy.setCreationTime(action.getCreationTime());
        shallowCopy.setType(action.getType());
        shallowCopy.setState(action.getState());
        shallowCopy.setMode(action.getMode());
        shallowCopy.setCategory(action.getCategory());
        shallowCopy.setSeverity(action.getSeverity());
        shallowCopy.setDescription(action.getDescription());
        shallowCopy.setExplanation(action.getExplanation());
        shallowCopy.setSavings(action.getSavings());
        shallowCopy.setTarget(action.getTarget());
        shallowCopy.setRelated(action.getRelated());
        return shallowCopy;
    }
}
