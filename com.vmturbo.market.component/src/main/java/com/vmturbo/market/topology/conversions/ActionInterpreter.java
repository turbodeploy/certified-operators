package com.vmturbo.market.topology.conversions;

import static com.vmturbo.common.protobuf.CostProtoUtil.calculateFactorForCommodityValues;
import static com.vmturbo.trax.Trax.trax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.TimeSlotReasonInformation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.SingleRegionMarketTier;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO.ActionTypeCase;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO.CommodityContext;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxCollectors;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxNumber;

/**
 * This class has methods which interpret {@link ActionTO} to {@link Action}
 */
public class ActionInterpreter {
    private static final Logger logger = LogManager.getLogger();
    private final CommodityConverter commodityConverter;
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos;
    private final CloudTopologyConverter cloudTc;
    private final Map<Long, TopologyEntityDTO> originalTopology;
    private final Map<Long, EconomyDTOs.TraderTO> oidToProjectedTraderTOMap;
    private final CommoditiesResizeTracker commoditiesResizeTracker;
    private final TierExcluder tierExcluder;
    private final ProjectedRICoverageCalculator projectedRICoverageCalculator;
    private static final Set<EntityState> evacuationEntityState =
        EnumSet.of(EntityState.MAINTENANCE, EntityState.FAILOVER);
    private static final Set<Integer> TRANSLATE_MOVE_TO_SCALE_PROVIDER_TYPE =
            ImmutableSet.of(EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE);
    private final CommodityIndex commodityIndex;
    private final Map<Long, AtomicInteger> provisionActionTracker = new HashMap<>();
    /**
     * Whether compliance action explanation needs to be overridden with perf/efficiency, needed
     * in certain cases like cloud migration.
     */
    private final BiFunction<MoveTO, Map<Long, ProjectedTopologyEntity>,
            Pair<Boolean, ChangeProviderExplanation>> complianceExplanationOverride;

    /**
     * Comparator used to sort the resizeInfo list so StorageAmount resizeInfo comes first,
     * and StorageAccess comes second, and then others(IO_Throughput).
     */
    private static final Comparator<ResizeInfo> RESIZEINFO_LIST_COMPARATOR =
            Comparator.comparingInt(resizeInfo -> {
                final int resizeInfoCommType = resizeInfo.getCommodityType().getType();
                if (resizeInfoCommType == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE) {
                    return 0;
                } else if (resizeInfoCommType == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE) {
                    return 1;
                } else {
                    return 2;
                }
            });

    ActionInterpreter(@Nonnull final CommodityConverter commodityConverter,
                      @Nonnull final Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                      @Nonnull final CloudTopologyConverter cloudTc, Map<Long, TopologyEntityDTO> originalTopology,
                      @Nonnull final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap,
                      @Nonnull final CommoditiesResizeTracker commoditiesResizeTracker,
                      @Nonnull final ProjectedRICoverageCalculator projectedRICoverageCalculator,
                      @Nonnull final TierExcluder tierExcluder,
                      @Nonnull final Supplier<CommodityIndex> commodityIndexSupplier,
                      @Nullable final BiFunction<MoveTO, Map<Long, ProjectedTopologyEntity>,
                              Pair<Boolean, ChangeProviderExplanation>> explanationFunction) {
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.cloudTc = cloudTc;
        this.originalTopology = originalTopology;
        this.oidToProjectedTraderTOMap = oidToTraderTOMap;
        this.commoditiesResizeTracker = commoditiesResizeTracker;
        this.projectedRICoverageCalculator = projectedRICoverageCalculator;
        this.tierExcluder = tierExcluder;
        this.commodityIndex = commodityIndexSupplier.get();
        this.complianceExplanationOverride = explanationFunction;
    }

    /**
     * Interpret the market-specific {@link ActionTO} as a topology-specific {@link Action} proto
     * for use by the rest of the system.
     *
     * It is vital to use the same {@link TopologyConverter} that converted
     * the topology into market-specific objects
     * to interpret the resulting actions in a way that makes sense.
     *
     * @param actionTO An {@link ActionTO} describing an action recommendation
     *                 generated by the market.
     * @param projectedTopology The projected topology entities, by id. The entities involved
     *                       in the action are expected to be in this map.
     * @param originalCloudTopology The original {@link CloudTopology}
     * @param projectedCosts The original {@link CloudTopology}
     * @param topologyCostCalculator The {@link TopologyCostCalculator} used to calculate costs
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    List<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                 @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                 @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                 @Nonnull TopologyCostCalculator topologyCostCalculator) {
        List<Action> actionList = new ArrayList<>();
        try {
            final Action.Builder action;
            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    final boolean translateMoveToScale = translateMoveToScale(actionTO);
                    if (translateMoveToScale) {
                        action.getInfoBuilder().setScale(interpretScaleAction(actionTO.getMove(),
                                projectedTopology, originalCloudTopology));
                        actionList.add(action.build());
                        break;
                    }
                    Optional<ActionDTO.Move> move = interpretMoveAction(
                            actionTO.getMove(), projectedTopology, originalCloudTopology);
                    if (move.isPresent()) {
                        action.getInfoBuilder().setMove(move.get());
                        actionList.add(action.build());
                    }
                    break;
                case COMPOUND_MOVE:
                    if (isSplitCompoundMove(actionTO)) {
                        actionList.addAll(splitCompoundMove(actionTO, projectedTopology,
                                originalCloudTopology, projectedCosts, topologyCostCalculator));
                    } else {
                        action = createAction(actionTO, projectedTopology,
                                originalCloudTopology, projectedCosts, topologyCostCalculator);
                        action.getInfoBuilder().setMove(interpretCompoundMoveAction(actionTO.getCompoundMove(),
                                projectedTopology, originalCloudTopology));
                        actionList.add(action.build());
                    }
                    break;
                case RECONFIGURE:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    action.getInfoBuilder().setReconfigure(interpretReconfigureAction(
                            actionTO.getReconfigure(), projectedTopology, originalCloudTopology));
                    actionList.add(action.build());
                    break;
                case PROVISION_BY_SUPPLY:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    action.getInfoBuilder().setProvision(interpretProvisionBySupply(
                            actionTO.getProvisionBySupply(), projectedTopology));
                    actionList.add(action.build());
                    break;
                case PROVISION_BY_DEMAND:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    action.getInfoBuilder().setProvision(interpretProvisionByDemand(
                            actionTO.getProvisionByDemand(), projectedTopology));
                    actionList.add(action.build());
                    break;
                case RESIZE:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    Optional<ActionDTO.Resize> resize = interpretResize(
                            actionTO.getResize(), projectedTopology);
                    if (resize.isPresent()) {
                        action.getInfoBuilder().setResize(resize.get());
                        actionList.add(action.build());
                    }
                    break;
                case ACTIVATE:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    action.getInfoBuilder().setActivate(interpretActivate(
                            actionTO.getActivate(), projectedTopology));
                    actionList.add(action.build());
                    break;
                case DEACTIVATE:
                    action = createAction(actionTO, projectedTopology,
                            originalCloudTopology, projectedCosts, topologyCostCalculator);
                    action.getInfoBuilder().setDeactivate(interpretDeactivate(
                            actionTO.getDeactivate(), projectedTopology));
                    actionList.add(action.build());
                    break;
            }
        } catch (RuntimeException e) {
            logger.error("Unable to interpret actionTO " + actionTO + " due to: ", e);
        }
        return actionList;
    }

    /**
     * Compound move actions generated for MPC will need to be split into individual move actions
     * for each of the VM and volume.
     * Only cloud move actions have the move contexts.
     *
     * @param actionTO actionTO
     * @return boolean to indicate if the action need to be split into separate move actions.
     */
    private boolean isSplitCompoundMove(@Nonnull final ActionTO actionTO) {
        return actionTO.getActionTypeCase() == ActionTypeCase.COMPOUND_MOVE
                && actionTO.getCompoundMove().getMovesList().stream().anyMatch(MoveTO::hasMoveContext);
    }

    /**
     * Create action build and apply savings to action.
     *
     * @param actionTO An {@link ActionTO} describing an action recommendation
     *                 generated by the market.
     * @param projectedTopology The projected topology entities, by id. The entities involved
     *                       in the action are expected to be in this map.
     * @param originalCloudTopology The original {@link CloudTopology}
     * @param projectedCosts The original {@link CloudTopology}
     * @param topologyCostCalculator The {@link TopologyCostCalculator} used to calculate costs
     * @return action builder
     */
    Action.Builder createAction(@Nonnull final ActionTO actionTO,
                                @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                @Nonnull TopologyCostCalculator topologyCostCalculator) {
        final CalculatedSavings savings;
        final Action.Builder action;
        final boolean translateMoveToScale = translateMoveToScale(actionTO);
        try (TraxContext traxContext = Trax.track("SAVINGS", actionTO.getActionTypeCase().name())) {
            savings = calculateActionSavings(actionTO,
                    originalCloudTopology, projectedCosts, topologyCostCalculator);

            // The action importance should never be infinite, as setImportance() will fail.
            action = Action.newBuilder()
                    // Assign a unique ID to each generated action.
                    .setId(IdentityGenerator.next())
                    .setDeprecatedImportance(actionTO.getImportance())
                    .setExplanation(interpretExplanation(actionTO, savings, projectedTopology, translateMoveToScale))
                    .setExecutable(!actionTO.getIsNotExecutable());
            savings.applySavingsToAction(action);

            if (traxContext.on()) {
                logger.info("{} calculation stack for {} action {}:\n{}",
                        () -> savings.getClass().getSimpleName(),
                        () -> actionTO.getActionTypeCase().name(),
                        () -> Long.toString(action.getId()),
                        savings.savingsAmount::calculationStack);

            }
        }

        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        action.setInfo(infoBuilder);
        return action;
    }

    private List<Action> splitCompoundMove(@Nonnull final ActionTO actionTO,
                                           @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                           @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                           @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                           @Nonnull TopologyCostCalculator topologyCostCalculator) {
        List<Action> actionList = new ArrayList<>();

        List<MoveTO> moveActions = actionTO.getCompoundMove().getMovesList();
        for (MoveTO moveTO : moveActions) {
            // Create ActionTO from MoveTO.
            ActionTO.Builder actionBuilder = ActionTO.newBuilder();
            actionBuilder.setIsNotExecutable(actionTO.getIsNotExecutable());
            actionBuilder.setMove(moveTO);
            actionBuilder.setImportance(actionTO.getImportance());

            Action.Builder moveActionBuilder = createAction(actionBuilder.build(), projectedTopology,
                    originalCloudTopology, projectedCosts, topologyCostCalculator);
            Optional<ActionDTO.Move> moveAction = interpretMoveAction(
                    actionBuilder.getMove(), projectedTopology, originalCloudTopology);
            if (moveAction.isPresent()) {
                moveActionBuilder.getInfoBuilder().setMove(moveAction.get());
                actionList.add(moveActionBuilder.build());
            }
        }
        return actionList;
    }

    /**
     * Calculates savings per action. Savings is calculated as cost at source - the cost at
     * destination. The cost is only the on demand costs. RI Compute is not considered.
     *
     * @param actionTO The actionTO for which savings is to be calculated
     * @param originalCloudTopology the CloudTopology which came into Analysis
     * @param projectedCosts a map of the id of the entity to its projected costJournal
     * @param topologyCostCalculator the topology cost calculator used to calculate costs
     * @return The calculated savings for the action. Returns {@link NoSavings} if no
     *         savings could be calculated.
     */
    @Nonnull
    CalculatedSavings calculateActionSavings(@Nonnull final ActionTO actionTO,
            @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
            @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
            @Nonnull TopologyCostCalculator topologyCostCalculator) {
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                final MoveTO move = actionTO.getMove();
                final MarketTier destMarketTier = cloudTc.getMarketTier(move.getDestination());
                final MarketTier sourceMarketTier = cloudTc.getMarketTier(move.getSource());
                // Savings can be calculated if the destination is cloud
                if (destMarketTier != null) {
                    TopologyEntityDTO cloudEntityMoving = getTopologyEntityMoving(move);
                    if (cloudEntityMoving != null) {
                        final String entityTypeName = EntityType.forNumber(cloudEntityMoving.getEntityType()).name();
                        try (TraxContext traxContext = Trax.track("SAVINGS", cloudEntityMoving.getDisplayName(),
                            Long.toString(cloudEntityMoving.getOid()), entityTypeName,
                            actionTO.getActionTypeCase().name())) {
                            return calculateMoveSavings(originalCloudTopology,
                                projectedCosts, topologyCostCalculator, destMarketTier,
                                sourceMarketTier, cloudEntityMoving);
                        }
                    } else {
                        return new NoSavings(trax(0, "no moving entity"));
                    }
                } else {
                    return new NoSavings(trax(0, "no destination market tier for " + move.getDestination()));
                }
            default:
                return new NoSavings(trax(0, "No savings calculation for "
                    + actionTO.getActionTypeCase().name()));
        }

        // Do not have a return statement here. Ensure all cases in the switch return a value.
    }

    @Nonnull
    private CalculatedSavings calculateMoveSavings(@Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                                   @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                                   @Nonnull TopologyCostCalculator topologyCostCalculator,
                                                   @Nonnull MarketTier destMarketTier,
                                                   @Nullable MarketTier sourceMarketTier,
                                                   @Nonnull TopologyEntityDTO cloudEntityMoving) {
        final CostJournal<TopologyEntityDTO> destCostJournal = projectedCosts
                .get(cloudEntityMoving.getOid());
        if (destCostJournal != null) {
            // Only the on demand costs are considered. RI Compute is not
            // considered. RI component is considered as 0 cost. Inside the market
            // also, RIs are considered as 0 cost. So it makes sense to be
            // consistent. But it can also be viewed from another perspective.
            // RI Compute has 2 components - upfront cost already paid to the
            // CSP and an hourly cost which is not yet paid. So the true savings
            // should consider the hourly cost and ignore the upfront cost.
            // Currently, both of these are clubbed into RI compute and
            // we cannot differentiate.
            TraxNumber onDemandDestCost = getOnDemandCostForMarketTier(
                    cloudEntityMoving, destMarketTier, destCostJournal);
            // Now get the source cost. Assume on prem source cost 0.
            final TraxNumber onDemandSourceCost;
            if (sourceMarketTier != null) {
                Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal =
                        topologyCostCalculator.calculateCostForEntity(
                                originalCloudTopology, cloudEntityMoving);
                if (!sourceCostJournal.isPresent()) {
                    return new NoSavings(trax(0, "no source cost journal"));
                }
                onDemandSourceCost = getOnDemandCostForMarketTier(cloudEntityMoving,
                        sourceMarketTier, sourceCostJournal.get());
            } else {
                onDemandSourceCost = trax(0, "source on-prem");
            }

            final String savingsDescription = String.format("Savings for %s \"%s\" (%d)",
                    EntityType.forNumber(cloudEntityMoving.getEntityType()).name(),
                    cloudEntityMoving.getDisplayName(), cloudEntityMoving.getOid());
            final TraxNumber savings = onDemandSourceCost.minus(onDemandDestCost).compute(savingsDescription);
            logger.debug("{} to move from {} to {} is {}", savingsDescription,
                sourceMarketTier == null ? null : sourceMarketTier.getDisplayName(),
                destMarketTier.getDisplayName(), savings);
            return new CalculatedSavings(savings);
        } else {
            return new NoSavings(trax(0, "no destination cost journal"));
        }
    }

    @Nullable
    private TopologyEntityDTO getTopologyEntityMoving(MoveTO move) {
        final ShoppingListInfo shoppingListInfo =
                shoppingListOidToInfos.get(move.getShoppingListToMove());
        long entityMovingId = shoppingListInfo.getResourceId().orElse(shoppingListInfo.getBuyerId());
        if (shoppingListInfo.getCollapsedBuyerId().isPresent()) {
            entityMovingId = shoppingListInfo.getCollapsedBuyerId().get();
        }
        return originalTopology.get(entityMovingId);
    }

    /**
     * Gets the cost for the market tier. In case of a compute/database market tier, it returns the
     * sum of compute + license + ip costs. In case of a storage market tier, it returns the
     * storage cost.
     *
     * @param cloudEntityMoving the entity moving
     * @param marketTier the market tier for which total cost is desired
     * @param journal the cost journal
     * @return the total cost of the market tier
     */
    @Nonnull
    private TraxNumber getOnDemandCostForMarketTier(TopologyEntityDTO cloudEntityMoving,
                                                    MarketTier marketTier,
                                                    CostJournal<TopologyEntityDTO> journal) {
        final TraxNumber totalOnDemandCost;
        if (TopologyDTOUtil.isPrimaryTierEntityType(marketTier.getTier().getEntityType())) {

            // In determining on-demand costs for SCALE actions, the savings from buy RI actions
            // should be ignored. Therefore, we lookup the on-demand cost, ignoring savings
            // from CostSource.BUY_RI_DISCOUNT
            TraxNumber onDemandComputeCost = journal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_COMPUTE,
                    CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);
            TraxNumber licenseCost = journal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_LICENSE,
                    CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);
            TraxNumber reservedLicenseCost = journal.getHourlyCostFilterEntries(
                    CostCategory.RESERVED_LICENSE,
                    CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);
            TraxNumber ipCost = journal.getHourlyCostForCategory(CostCategory.IP);
            totalOnDemandCost = Stream.of(onDemandComputeCost, licenseCost, reservedLicenseCost, ipCost)
                .collect(TraxCollectors.sum(marketTier.getTier().getDisplayName() + " total cost"));
            logger.debug("Costs for {} on {} are -> on demand compute cost = {}, licenseCost = {}," +
                    " reservedLicenseCost = {}, ipCost = {}",
                    cloudEntityMoving.getDisplayName(), marketTier.getDisplayName(),
                    onDemandComputeCost, licenseCost, reservedLicenseCost, ipCost);
        } else {
            totalOnDemandCost = journal.getHourlyCostForCategory(CostCategory.STORAGE)
                .named(marketTier.getTier().getDisplayName() + " total cost");
            logger.debug("Costs for {} on {} are -> storage cost = {}",
                    cloudEntityMoving.getDisplayName(), marketTier.getDisplayName(), totalOnDemandCost);
        }
        return totalOnDemandCost;
    }

    @Nonnull
    private ActionDTO.Deactivate interpretDeactivate(@Nonnull final DeactivateTO deactivateTO,
                         @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long entityId = deactivateTO.getTraderToDeactivate();
        final List<CommodityType> topologyCommodities =
                deactivateTO.getTriggeringBasketList().stream()
                        .map(commodityConverter::marketToTopologyCommodity)
                        .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        return ActionDTO.Deactivate.newBuilder()
                .setTarget(createActionEntity(entityId, projectedTopology))
            .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    /**
     * Convert the {@link CompoundMoveTO} recieved from M2 into a {@link ActionDTO.Move} action.
     * We do 2 things in this method:
     * 1. Create an action entity and set it as the target for the move
     * 2. Create change providers. Change providers specify the from and to of the move action.
     * Compound move can have multiple change providers.
     *
     * @param compoundMoveTO the input {@link CompoundMoveTO}
     * @param projectedTopology a map of entity id to its {@link ProjectedTopologyEntity}
     * @param originalCloudTopology The original cloud topology
     * @return {@link ActionDTO.Move} representing the compound move
     */
    @Nonnull
    private ActionDTO.Move interpretCompoundMoveAction(
            @Nonnull final CompoundMoveTO compoundMoveTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        List<MoveTO> moves = compoundMoveTO.getMovesList();
        if (moves.isEmpty()) {
            throw new IllegalStateException(
                    "Market returned no moves in a COMPOUND_MOVE: " + compoundMoveTO);
        }
        Set<Long> targetIds = moves.stream()
                .map(MoveTO::getShoppingListToMove)
                .map(shoppingListOidToInfos::get)
                .map(ShoppingListInfo::getBuyerId)
                .collect(Collectors.toSet());
        if (targetIds.size() != 1) {
            throw new IllegalStateException(
                    (targetIds.isEmpty() ? "Empty target ID" : "Non-unique target IDs")
                            + " in COMPOUND_MOVE:" + compoundMoveTO);
        }
        Long targetOid = targetIds.iterator().next();

        return ActionDTO.Move.newBuilder()
            .setTarget(createActionEntity(
                targetIds.iterator().next(), projectedTopology))
            .addAllChanges(moves.stream()
                .map(move -> createChangeProviders(move, projectedTopology, originalCloudTopology, targetOid))
                .flatMap(List::stream)
                .collect(Collectors.toList()))
                .build();
    }

    private ActionDTO.Provision interpretProvisionByDemand(
            @Nonnull final ProvisionByDemandTO provisionByDemandTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        AtomicInteger index =
            provisionActionTracker.computeIfAbsent(provisionByDemandTO.getModelSeller(),
                id -> new AtomicInteger());

        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                    provisionByDemandTO.getModelSeller(), projectedTopology))
                .setProvisionedSeller(provisionByDemandTO.getProvisionedSeller())
                .setProvisionIndex(index.getAndIncrement())
                .build();
    }

    @Nonnull
    private ActionDTO.Provision interpretProvisionBySupply(
            @Nonnull final ProvisionBySupplyTO provisionBySupplyTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        AtomicInteger index =
            provisionActionTracker.computeIfAbsent(provisionBySupplyTO.getModelSeller(),
                id -> new AtomicInteger());

        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                        provisionBySupplyTO.getModelSeller(), projectedTopology))
                .setProvisionedSeller(provisionBySupplyTO.getProvisionedSeller())
                .setProvisionIndex(index.getAndIncrement())
                .build();
    }

    /**
     * Convert the {@link MoveTO} received from M2 into a {@link ActionDTO.Move} action.
     * We do 2 things in this method:
     * 1. Create an action entity and set it as the target for the move
     * 2. Create change providers. Change providers specify the from and to of the move action.
     * A single moveTO can have multiple change providers. Today this can happen in case of cloud
     * moves when a region and tier can change as part of the same moveTO.
     *
     * @param moveTO the input {@link MoveTO}
     * @param projectedTopology a map of entity id to the {@link ProjectedTopologyEntity}.
     * @param originalCloudTopology the original cloud topology
     * @return {@link ActionDTO.Move} representing the move
     */
    @Nonnull
    Optional<ActionDTO.Move> interpretMoveAction(@Nonnull final MoveTO moveTO,
                                       @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                       @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final ShoppingListInfo shoppingList =
                shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingList == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for MOVE: " + moveTO);
        } else {
            MarketTier destination = cloudTc.getMarketTier(moveTO.getDestination());
            if (destination != null && destination.hasRIDiscount()) {
                logger.error("MoveTO for entity {} has RI {} as destination. This should not happen.",
                    shoppingList.buyerId, destination.getDisplayName());
                return Optional.empty();
            }
            List<ChangeProvider> changeProviderList = createChangeProviders(moveTO,
                    projectedTopology, originalCloudTopology, shoppingList.buyerId);
            if (!CollectionUtils.isEmpty(changeProviderList)) {
                ActionDTO.Move.Builder builder = ActionDTO.Move.newBuilder()
                        .setTarget(createActionEntity(shoppingList.buyerId, projectedTopology))
                        .addAllChanges(changeProviderList);
                if (moveTO.hasScalingGroupId()) {
                    builder.setScalingGroupId(moveTO.getScalingGroupId());
                }
                return Optional.of(builder.build());
            }
        }
        return Optional.empty();
    }

    /**
     * Convert the {@link MoveTO} received from M2 into a {@link ActionDTO.Scale} action -
     * Create an action entity and set it as the target for the Scale.
     * Create change providers. Change providers specify the from and to of the Scale action.
     * Create commodity resize info, which specify commodity change of the Scale action.
     *
     * @param moveTO the input {@link MoveTO}
     * @param projectedTopology a map of entity id to the {@link ProjectedTopologyEntity}.
     * @param originalCloudTopology the original cloud topology
     * @return {@link ActionDTO.Scale} representing the Scale
     */
    private ActionDTO.Scale interpretScaleAction(@Nonnull final MoveTO moveTO,
                                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                                 @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final ShoppingListInfo shoppingListInfo =
                shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingListInfo == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for MOVE: " + moveTO);
        }
        // Set action target entity.
        final ActionEntity actionTargetEntity = createActionTargetEntity(shoppingListInfo, projectedTopology);
        ActionDTO.Scale.Builder builder = ActionDTO.Scale.newBuilder().setTarget(actionTargetEntity);
        // Interpret provider change.
        List<ChangeProvider> changeProviderList = createChangeProviders(moveTO,
                projectedTopology, originalCloudTopology, shoppingListInfo.getBuyerId());
        if (!CollectionUtils.isEmpty(changeProviderList)) {
            builder.addAllChanges(changeProviderList);
        }
        // Interpret commodities change.
        List<ResizeInfo> resizeInfoList = createCommodityResizeInfo(moveTO, actionTargetEntity);
        if (!CollectionUtils.isEmpty(resizeInfoList)) {
            builder.addAllCommodityResizes(resizeInfoList);
        }
        return builder.build();
    }

    /**
     * Interpret a market generated reconfigure action.
     *
     * @param reconfigureTO  a {@link ReconfigureTO}
     * @param projectedTopology the projectedTopology
     * @param originalCloudTopology the originalCloudTopology
     * @return a {@link ActionDTO.Reconfigure}
     */
    @Nonnull
    private ActionDTO.Reconfigure interpretReconfigureAction(
            @Nonnull final ReconfigureTO reconfigureTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            @Nonnull final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final ShoppingListInfo shoppingList =
                shoppingListOidToInfos.get(reconfigureTO.getShoppingListToReconfigure());
        if (shoppingList == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for RECONFIGURE: " + reconfigureTO);
        } else {
            final ActionDTO.Reconfigure.Builder builder = ActionDTO.Reconfigure.newBuilder()
                    .setTarget(createActionEntity(shoppingList.getBuyerId(), projectedTopology));

            if (reconfigureTO.hasSource()) {
                Optional<Long> providerIdOptional = getOriginalProviderId(
                    reconfigureTO.getSource(), shoppingList.getBuyerId(), originalCloudTopology);
                providerIdOptional.ifPresent(providerId ->
                    builder.setSource(createActionEntity(providerId, projectedTopology)));
            }
            if (reconfigureTO.hasScalingGroupId()) {
                builder.setScalingGroupId(reconfigureTO.getScalingGroupId());
            }
            return builder.build();
        }
    }

    @Nonnull
    private Optional<ActionDTO.Resize> interpretResize(@Nonnull final ResizeTO resizeTO,
                     @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long entityId = resizeTO.getSellingTrader();
        final CommoditySpecificationTO cs = resizeTO.getSpecification();
        final CommodityType topologyCommodityType =
                commodityConverter.marketToTopologyCommodity(cs)
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Resize commodity can't be converted to topology commodity format! "
                                        + cs));
        // Determine if this is a remove limit or a regular resize.
        final TopologyEntityDTO projectedEntity = projectedTopology.get(entityId).getEntity();

        // Find the CommoditySoldDTO for the resize commodity.
        final Optional<CommoditySoldDTO> resizeCommSold =
            projectedEntity.getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().equals(topologyCommodityType))
                .findFirst();
        Optional<CommoditySoldDTO> originalCommoditySold =
                commodityIndex.getCommSold(projectedEntity.getOid(), topologyCommodityType);
        ResizeTO adjustedResizeTO = resizeTO.toBuilder()
                .setOldCapacity((float)TopologyConverter.reverseScaleComm(resizeTO.getOldCapacity(),
                    originalCommoditySold, CommoditySoldDTO::getScalingFactor))
                .setNewCapacity((float)TopologyConverter.reverseScaleComm(resizeTO.getNewCapacity(),
                    originalCommoditySold, CommoditySoldDTO::getScalingFactor))
                .build();
        if (projectedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE.getNumber()) {
            // If this is a VM and has a restricted capacity, we are going to assume it's a limit
            // removal. This logic seems like it could be fragile, in that limit may not be the
            // only way VM capacity could be restricted in the future, but this is consistent
            // with how classic makes the same decision.
            TraderTO traderTO = oidToProjectedTraderTOMap.get(entityId);
            // find the commodity on the trader and see if there is a limit?
            for (CommoditySoldTO commoditySold : traderTO.getCommoditiesSoldList()) {
                if (commoditySold.getSpecification().equals(cs)) {
                    // We found the commodity sold.  If it has a utilization upper bound < 1.0,
                    // then the commodity is restricted, and according to our VM-rule, we will
                    // treat this as a limit removal.
                    float utilizationPercentage = commoditySold.getSettings().getUtilizationUpperBound();
                    if (utilizationPercentage < 1.0) {
                        // The "limit removal" is actually a resize on the commodity's "limit"
                        // attribute that effectively sets it to zero.
                        //
                        // Ideally we would set the "old capacity" to the current limit
                        // value, but as noted above, we don't have access to the limit here. We
                        // only have the utilization percentage, which we expect to be based on
                        // the limit and raw capacity values. Since we do have the utilization %
                        // and raw capacity here, we can _approximate_ the current limit by
                        // reversing the math used to determine the utilization %.
                        //
                        // We will grudgingly do that here. Note that this may be subject to
                        // precision and rounding errors. In addition, if in the future we have
                        // factors other than "limit" that could drivef the VM resource
                        // utilization threshold to below 100%, then this approximation would
                        // likely be wrong and misleading in those cases.
                        float approximateLimit = commoditySold.getCapacity() * utilizationPercentage;
                        logger.debug("The commodity {} has util% of {}, so treating as limit"
                                        +" removal (approximate limit: {}).",
                                topologyCommodityType.getKey(), utilizationPercentage, approximateLimit);

                        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
                            .setTarget(createActionEntity(entityId, projectedTopology))
                            .setOldCapacity(approximateLimit)
                            .setNewCapacity(0)
                            .setCommodityType(topologyCommodityType)
                            .setCommodityAttribute(CommodityAttribute.LIMIT);
                        setHotAddRemove(resizeBuilder, resizeCommSold);
                        return Optional.of(resizeBuilder.build());
                    }
                    break;
                }
            }
        }
        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
                .setTarget(createActionEntity(entityId, projectedTopology))
                .setNewCapacity(adjustedResizeTO.getNewCapacity())
                .setOldCapacity(adjustedResizeTO.getOldCapacity())
                .setCommodityType(topologyCommodityType);
        setHotAddRemove(resizeBuilder, resizeCommSold);
        if (resizeTO.hasScalingGroupId()) {
            resizeBuilder.setScalingGroupId(resizeTO.getScalingGroupId());
        }
        if (!resizeTO.getResizeTriggerTraderList().isEmpty()) {
            // Scale Up: Show relevant vSan resizes when hosts are provisioned due to
            // the commodity type being scaled.
            if (adjustedResizeTO.getNewCapacity() > adjustedResizeTO.getOldCapacity()) {
                Optional<ResizeTriggerTraderTO> resizeTriggerTraderTO = resizeTO.getResizeTriggerTraderList().stream()
                    .filter(resizeTriggerTrader -> resizeTriggerTrader.getRelatedCommoditiesList()
                        .contains(cs.getBaseType())).findFirst();
                if (!resizeTriggerTraderTO.isPresent()) {
                    return Optional.empty();
                }
            // Scale Down: Pick related vSan host being suspended that has no reason commodities
            // since it is suspension due to low roi.
            } else if (adjustedResizeTO.getNewCapacity() < adjustedResizeTO.getOldCapacity()) {
                Optional<ResizeTriggerTraderTO> resizeTriggerTraderTO = resizeTO.getResizeTriggerTraderList().stream()
                    .filter(resizeTriggerTrader -> resizeTriggerTrader.getRelatedCommoditiesList()
                        .isEmpty()).findFirst();
                if (!resizeTriggerTraderTO.isPresent()) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(resizeBuilder.build());
    }

    /**
     * Set the hot add / hot remove flag on the resize action. This is needed for the resize
     * action execution of probes like VMM.
     */
    private void setHotAddRemove(@Nonnull ActionDTO.Resize.Builder resizeBuilder,
                                 @Nonnull Optional<CommoditySoldDTO> commoditySold) {
        commoditySold.filter(CommoditySoldDTO::hasHotResizeInfo)
            .map(CommoditySoldDTO::getHotResizeInfo)
            .ifPresent(hotResizeInfo -> {
                if (hotResizeInfo.hasHotAddSupported()) {
                    resizeBuilder.setHotAddSupported(hotResizeInfo.getHotAddSupported());
                }
                if (hotResizeInfo.hasHotRemoveSupported()) {
                    resizeBuilder.setHotRemoveSupported(hotResizeInfo.getHotRemoveSupported());
                }
            });
    }

    @Nonnull
    private ActionDTO.Activate interpretActivate(@Nonnull final ActivateTO activateTO,
                                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long entityId = activateTO.getTraderToActivate();
        final List<CommodityType> topologyCommodities =
                activateTO.getTriggeringBasketList().stream()
                        .map(commodityConverter::marketToTopologyCommodity)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        return ActionDTO.Activate.newBuilder()
                .setTarget(createActionEntity(entityId, projectedTopology))
                .addAllTriggeringCommodities(topologyCommodities)
                .build();
    }

    /**
     * Create change providers for the {@link MoveTO}.
     *
     * @param move the input {@link MoveTO}
     * @param projectedTopology A map of the id of the entity to its {@link ProjectedTopologyEntity}.
     * @param targetOid targetOid
     * @param originalCloudTopology The original cloud topology.
     * @return A list of change providers representing the move
     */
    @Nonnull
    private List<ChangeProvider> createChangeProviders(
            @Nonnull final MoveTO move, @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            CloudTopology<TopologyEntityDTO> originalCloudTopology, Long targetOid) {
        List<ChangeProvider> changeProviders = new ArrayList<>();
        MarketTier sourceMarketTier = move.hasSource() ?
                cloudTc.getMarketTier(move.getSource()) : null;
        MarketTier destMarketTier = move.hasDestination() ?
                cloudTc.getMarketTier(move.getDestination()) : null;
        TopologyEntityDTO sourceAzOrRegion = null;
        TopologyEntityDTO destAzOrRegion = null;
        TopologyEntityDTO sourceTier = null;
        TopologyEntityDTO destTier = null;
        TopologyEntityDTO sourceRegion = null;
        TopologyEntityDTO destinationRegion = null;
        TopologyEntityDTO target = originalTopology.get(targetOid);

        if (destMarketTier != null ) {
            if (destMarketTier.isSingleRegion()) {
                // For a RI Discounted market tier we get the region from the destination market tier
                // SingleRegionMarketTier is currently only for RIDiscountedMarketTier. So,
                // region information is coming from Tier itself.
                destinationRegion = ((SingleRegionMarketTier)destMarketTier).getRegion();
            } else {
                // This is a onDemandMarketTier so we get the region from the move context.
                // MultiRegionMarketTier contains all region's cost information so only move context
                // can tell which region was chosen inside market analysis.
                long regionCommSpec = move.getMoveContext().getRegionId();
                destinationRegion = originalTopology.get(regionCommSpec);
            }

            // TODO: We are considering the destination AZ as the first AZ of the destination
            // region. In case of zonal RIs, we need to get the zone of the RI.
            List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(
                    destinationRegion, EntityType.AVAILABILITY_ZONE_VALUE, originalTopology);

            // Try to use the regionId in the moveContext if possible- this is the actual destination
            if (move.hasMoveContext() && move.getMoveContext().hasRegionId()) {
                ProjectedTopologyEntity projectedTopologyRegion = projectedTopology.get(
                        move.getMoveContext().getRegionId());
                if (Objects.nonNull(projectedTopologyRegion)) {
                    destAzOrRegion = projectedTopologyRegion.getEntity();
                }
            }
            // We weren't able to get this from the moveContext...
            if (Objects.isNull(destAzOrRegion)) {
                destAzOrRegion = connectedEntities.isEmpty()
                        ? destinationRegion
                        : connectedEntities.get(0);
            }
            destTier = destMarketTier.getTier();
        }

        if (sourceMarketTier != null) {
            if (sourceMarketTier.isSingleRegion()) {
                // Ri Discounted MarketTiers have a region associated with them
                sourceRegion = ((SingleRegionMarketTier)sourceMarketTier).getRegion();
            } else {
                 if (originalCloudTopology != null) {
                    Optional<TopologyEntityDTO> regionFromCloudTopo = originalCloudTopology.getConnectedRegion(targetOid);
                    if (regionFromCloudTopo != null && regionFromCloudTopo.isPresent()) {
                        // For an onDemandMarketTier get the source region from the original cloud topology
                        sourceRegion = regionFromCloudTopo.get();
                    }
                }
            }

            // Soruce AZ or Region is the AZ or Region which the target is connected to.
            List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(target,
                Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                originalTopology);
            if (!connectedEntities.isEmpty())
                sourceAzOrRegion = connectedEntities.get(0);
            else
                logger.error("{} is not connected to any AZ or Region", target.getDisplayName());
            if (sourceMarketTier.hasRIDiscount()) {
                Optional<TopologyEntityDTO.CommoditiesBoughtFromProvider> boughtGroupingFromTier =
                    target.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(grouping -> grouping.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                        .findFirst();

                if (boughtGroupingFromTier.isPresent()) {
                    sourceTier = originalCloudTopology.getEntity(boughtGroupingFromTier.get().getProviderId()).get();
                }
            } else {
                sourceTier = sourceMarketTier.getTier();
            }
        }

        Long moveSource = move.hasSource() ? move.getSource() : null;
        // Update moveSource if the original supplier is in MAINTENANCE or FAILOVER state.
        final ShoppingListInfo shoppingListInfo = shoppingListOidToInfos.get(move.getShoppingListToMove());
        if (!move.hasSource() &&
            shoppingListInfo.getSellerId() != null &&
            projectedTopology.containsKey(shoppingListInfo.getSellerId()) &&
            evacuationEntityState.contains(
                projectedTopology.get(shoppingListInfo.getSellerId()).getEntity().getEntityState())) {
            moveSource = shoppingListInfo.getSellerId();
        }

        Long resourceId = shoppingListOidToInfos.get(move.getShoppingListToMove()).resourceId;
        if (resourceId == null && shoppingListInfo.getCollapsedBuyerId().isPresent()) {
            // For cloud->cloud move storage shopping list, use the collapsed buyer (volume id).
            resourceId = shoppingListInfo.getCollapsedBuyerId().get();
        }
        // 4 case of moves:
        // 1) Cloud to cloud. 2) on prem to cloud. 3) cloud to on prem. 4) on prem to on prem.
        if (sourceMarketTier != null && destMarketTier != null) {
            // Cloud to cloud move
            if (destinationRegion != sourceRegion) {
                // AZ or Region change provider. We create an AZ or Region change provider
                // because the target is connected to AZ or Region.
                changeProviders.add(createChangeProvider(sourceAzOrRegion.getOid(),
                    destAzOrRegion.getOid(), null, projectedTopology));
            }
            // Note that we also generate accounting actions for complete loss of RI coverage.
            final boolean generateTierAction;
            final boolean isAccountingAction = destinationRegion == sourceRegion
                    && destTier == sourceTier
                    && (move.hasCouponDiscount() && move.hasCouponId() ||
                        sourceMarketTier.hasRIDiscount());
            if (isAccountingAction) {
                // We need to check if the original projected RI coverage of the target are the same.
                // If they are the same, we should drop the action.
                final double originalRICoverage = getTotalRiCoverage(
                        cloudTc.getRiCoverageForEntity(targetOid).orElse(null));
                final double projectedRICoverage = getTotalRiCoverage(
                        projectedRICoverageCalculator.getProjectedRICoverageForEntity(targetOid));
                generateTierAction = !areEqual(originalRICoverage, projectedRICoverage);
                if (generateTierAction) {
                    logger.debug("Accounting action for {} (OID: {}). " +
                                    "Original RI coverage: {}, projected RI coverage: {}.",
                            target.getDisplayName(), target.getOid(),
                            originalRICoverage, projectedRICoverage);
                }
            } else {
                generateTierAction = destTier != sourceTier;
            }
            if (generateTierAction) {
                // Tier change provider
                changeProviders.add(createChangeProvider(sourceTier.getOid(),
                        destTier.getOid(), resourceId, projectedTopology));
            }
        } else if (sourceMarketTier == null && destMarketTier != null) {
            // On prem to cloud move (with or without source)
            // AZ or Region change provider. We create an AZ or Region change provider
            // because the target is connected to AZ or Region.
            changeProviders.add(createChangeProvider(moveSource,
                destAzOrRegion.getOid(), null, projectedTopology));
            // Tier change provider
            changeProviders.add(createChangeProvider(moveSource,
                    destTier.getOid(), resourceId, projectedTopology));
        } else if (sourceMarketTier != null && destMarketTier == null) {
            // Cloud to on prem move
            changeProviders.add(createChangeProvider(sourceTier.getOid(),
                    move.getDestination(), resourceId, projectedTopology));
        } else {
            // On prem to on prem (with or without source)
            // A cloud container pod moving from a cloud VM to another cloud VM is covered by this case.
            changeProviders.add(createChangeProvider(moveSource,
                    move.getDestination(), resourceId, projectedTopology));
        }
        return changeProviders;
    }

    /**
     * Create a list of ResizeInfo with MoveTO CommodityContext.
     *
     * @param move moveTO
     * @param actionTargetEntity action target entity
     * @return a list of ResizeInfo if CommodityContext is available
     */
    @Nonnull
    private List<ResizeInfo> createCommodityResizeInfo(@Nonnull final MoveTO move,
                                                       @Nonnull final ActionEntity actionTargetEntity) {
        List<ResizeInfo> resizeInfoList = new ArrayList<>();
        for (CommodityContext commodityContext : move.getCommodityContextList()) {
            CommodityType topologyCommodityType =
                    commodityConverter.marketToTopologyCommodity(commodityContext.getSpecification())
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "Resize commodity can't be converted to topology commodity format! "
                                            + commodityContext.getSpecification()));
            final float factor = calculateFactorForCommodityValues(topologyCommodityType.getType(), actionTargetEntity.getType());
            resizeInfoList.add(ResizeInfo.newBuilder()
                    .setCommodityType(topologyCommodityType)
                    .setOldCapacity(commodityContext.getOldCapacity() * factor)
                    .setNewCapacity(commodityContext.getNewCapacity() * factor)
                    .build());
        }
        resizeInfoList.sort(RESIZEINFO_LIST_COMPARATOR);
        return resizeInfoList;
    }

    private static double getTotalRiCoverage(
            @Nullable final EntityReservedInstanceCoverage entityReservedInstanceCoverage) {
        if (entityReservedInstanceCoverage == null) {
            return 0D;
        }
        return entityReservedInstanceCoverage.getCouponsCoveredByRiMap().values().stream()
                .mapToDouble(Double::doubleValue).sum();
    }

    private static boolean areEqual(final double d1, final double d2) {
        return Math.abs(d1 - d2) <= 0.0001;
    }

    @Nonnull
    private ChangeProvider createChangeProvider(@Nullable final Long sourceId,
                            final long destinationId,
                            @Nullable final Long resourceId,
                            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final ChangeProvider.Builder changeProviderBuilder = ChangeProvider.newBuilder()
            .setDestination(createActionEntity(destinationId, projectedTopology));
        if (sourceId != null) {
            changeProviderBuilder.setSource(createActionEntity(sourceId, projectedTopology));
        }
        if (resourceId != null) {
            changeProviderBuilder.setResource(createActionEntity(resourceId, projectedTopology));
        }
        return changeProviderBuilder.build();
    }

    private Explanation interpretExplanation(
            @Nonnull ActionTO actionTO, @Nonnull CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            final boolean translateMoveToScale) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                if (translateMoveToScale) {
                    expBuilder.setScale(interpretScaleExplanation(actionTO, savings, projectedTopology));
                } else {
                    expBuilder.setMove(interpretMoveExplanation(actionTO, savings, projectedTopology));
                }
                break;
            case RECONFIGURE:
                expBuilder.setReconfigure(
                        interpretReconfigureExplanation(actionTO));
                break;
            case PROVISION_BY_SUPPLY:
                expBuilder.setProvision(
                        interpretProvisionExplanation(actionTO.getProvisionBySupply()));
                break;
            case PROVISION_BY_DEMAND:
                expBuilder.setProvision(
                        interpretProvisionExplanation(actionTO.getProvisionByDemand()));
                break;
            case RESIZE:
                expBuilder.setResize(
                        interpretResizeExplanation(actionTO.getResize()));
                break;
            case ACTIVATE:
                expBuilder.setActivate(
                        interpretActivateExplanation(actionTO.getActivate()));
                break;
            case DEACTIVATE:
                expBuilder.setDeactivate(
                        ActionDTO.Explanation.DeactivateExplanation.getDefaultInstance());
                break;
            case COMPOUND_MOVE:
                // TODO(COMPOUND): different moves in a compound move may have different explanations
                expBuilder.setMove(interpretCompoundMoveExplanation(
                    actionTO, projectedTopology));
                break;
            default:
                throw new IllegalArgumentException("Market returned invalid action type "
                        + actionTO.getActionTypeCase());
        }
        return expBuilder.build();
    }

    /**
     * Whether create Scale based on MoveTO.
     *
     * @param actionTO actionTO to check.
     * @return true if need to interpret MoveTO to Scale action.
     */
    private boolean translateMoveToScale(@Nonnull ActionTO actionTO) {
        final MoveTO moveTO = actionTO.getMove();
        if (moveTO == null) {
           return false;
        }
        MarketTier sourceMarketTier = moveTO.hasSource() ? cloudTc.getMarketTier(moveTO.getSource()) : null;
        MarketTier destMarketTier = moveTO.hasDestination() ? cloudTc.getMarketTier(moveTO.getDestination()) : null;
        if (sourceMarketTier == null || destMarketTier == null) {
            return false;
        }
        return sourceMarketTier.getTier() != null
                && TRANSLATE_MOVE_TO_SCALE_PROVIDER_TYPE.contains(sourceMarketTier.getTier().getEntityType())
                && destMarketTier.getTier() != null
                && TRANSLATE_MOVE_TO_SCALE_PROVIDER_TYPE.contains(destMarketTier.getTier().getEntityType());
    }

    private ScaleExplanation interpretScaleExplanation(@Nonnull ActionTO actionTO, @Nonnull CalculatedSavings savings,
                                                       @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        MoveTO moveTO = actionTO.getMove();
        ScaleExplanation.Builder scaleExpBuilder = ScaleExplanation.newBuilder();
        ChangeProviderExplanation.Builder changeProviderExplanation =
                changeExplanation(actionTO, moveTO, savings, projectedTopology, true);
        scaleExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
        if (moveTO.hasScalingGroupId()) {
            scaleExpBuilder.setScalingGroupId(moveTO.getScalingGroupId());
        }
        return scaleExpBuilder.build();
    }

    private MoveExplanation interpretMoveExplanation(
            @Nonnull ActionTO actionTO, @Nonnull CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        MoveTO moveTO = actionTO.getMove();
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        // For simple moves, set the sole change provider explanation to be the primary one
        ChangeProviderExplanation.Builder changeProviderExplanation =
            changeExplanation(actionTO, moveTO, savings, projectedTopology, true);
        moveExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
        if (moveTO.hasScalingGroupId()) {
            moveExpBuilder.setScalingGroupId(moveTO.getScalingGroupId());
        }
        return moveExpBuilder.build();
    }

    /**
     * Generate explanation for a reconfigure action.
     *
     * @param actionTO An {@link ActionTO} describing an action recommendation generated by the market.
     * @return {@link ReconfigureExplanation}
     */
    private ReconfigureExplanation interpretReconfigureExplanation(@Nonnull final ActionTO actionTO) {
        final ReconfigureTO reconfigureTO = actionTO.getReconfigure();
        final ReconfigureExplanation.Builder reconfigureExplanation = ReconfigureExplanation.newBuilder()
            .addAllReconfigureCommodity(reconfigureTO.getCommodityToReconfigureList().stream()
                .map(commodityConverter::commodityIdToCommodityType)
                .filter(commType -> !tierExcluder.isCommodityTypeForTierExclusion(commType))
                .map(commType2ReasonCommodity())
                .collect(Collectors.toList()));
        tierExcluder.getReasonSettings(actionTO).ifPresent(reconfigureExplanation::addAllReasonSettings);
        if (reconfigureTO.hasScalingGroupId()) {
            reconfigureExplanation.setScalingGroupId(reconfigureTO.getScalingGroupId());
        }
        return reconfigureExplanation.build();
    }

    private ProvisionExplanation
    interpretProvisionExplanation(ProvisionByDemandTO provisionByDemandTO) {
        final ShoppingListInfo shoppingList =
                shoppingListOidToInfos.get(provisionByDemandTO.getModelBuyer());
        if (shoppingList == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for PROVISION_BY_DEMAND: "
                            + provisionByDemandTO);
        } else {
            ProvisionExplanation.Builder expBuilder = ProvisionExplanation.newBuilder();
            List<ProvisionByDemandExplanation.CommodityNewCapacityEntry> capacityPerType =
                    new ArrayList<>();
            List<ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry> maxAmountPerType =
                    new ArrayList<>();
            provisionByDemandTO.getCommodityNewCapacityEntryList().forEach(newCapacityEntry ->
                    capacityPerType.add(
                            ProvisionByDemandExplanation.CommodityNewCapacityEntry.newBuilder()
                                    .setCommodityBaseType(newCapacityEntry.getCommodityBaseType())
                                    .setNewCapacity(newCapacityEntry.getNewCapacity()).build()));
            provisionByDemandTO.getCommodityMaxAmountAvailableList().forEach(maxAmount ->
                    maxAmountPerType.add(
                            ActionDTO.Explanation.ProvisionExplanation
                                    .ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry.newBuilder()
                                    .setCommodityBaseType(maxAmount.getCommodityBaseType())
                                    .setMaxAmountAvailable(maxAmount.getMaxAmountAvailable())
                                    .setRequestedAmount(maxAmount.getRequestedAmount()).build()));
            return expBuilder.setProvisionByDemandExplanation(ProvisionByDemandExplanation
                    .newBuilder().setBuyerId(shoppingList.buyerId)
                    .addAllCommodityNewCapacityEntry(capacityPerType)
                    .addAllCommodityMaxAmountAvailable(maxAmountPerType).build()).build();
        }
    }

    private ProvisionExplanation
    interpretProvisionExplanation(ProvisionBySupplyTO provisionBySupply) {
        CommodityType commType = commodityConverter.marketToTopologyCommodity(
                provisionBySupply.getMostExpensiveCommodity()).orElseThrow(() -> new IllegalArgumentException(
            "Most expensive commodity in provision can't be converted to topology commodity format! "
                + provisionBySupply.getMostExpensiveCommodity()));
        return ProvisionExplanation.newBuilder()
            .setProvisionBySupplyExplanation(
                ProvisionBySupplyExplanation.newBuilder()
                    .setMostExpensiveCommodityInfo(
                        ReasonCommodity.newBuilder().setCommodityType(commType).build())
                    .build())
                .build();
    }

    private ResizeExplanation interpretResizeExplanation(ResizeTO resizeTO) {
        ResizeExplanation.Builder builder = ResizeExplanation.newBuilder()
            .setDeprecatedStartUtilization(resizeTO.getStartUtilization())
            .setDeprecatedEndUtilization(resizeTO.getEndUtilization());
        if (resizeTO.hasScalingGroupId()) {
            builder.setScalingGroupId(resizeTO.getScalingGroupId());
        }
        return builder.build();
    }

    private ActivateExplanation interpretActivateExplanation(ActivateTO activateTO) {
        return ActivateExplanation.newBuilder()
                .setMostExpensiveCommodity(activateTO.getMostExpensiveCommodity())
                .build();
    }

    private MoveExplanation interpretCompoundMoveExplanation(
            @Nonnull ActionTO actionTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        for (MoveTO moveTO : actionTO.getCompoundMove().getMovesList()) {
            TraderTO destinationTrader = oidToProjectedTraderTOMap.get(moveTO.getDestination());
            boolean isPrimaryChangeExplanation = destinationTrader != null
                    && ActionDTOUtil.isPrimaryEntityType(destinationTrader.getType());
            ChangeProviderExplanation.Builder changeProviderExplanation =
                changeExplanation(actionTO, moveTO, new NoSavings(trax(0)), projectedTopology,
                    isPrimaryChangeExplanation);
            moveExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
        }
        return moveExpBuilder.build();
    }

    private ChangeProviderExplanation.Builder changeExplanation(
            @Nonnull ActionTO actionTO,
            @Nonnull MoveTO moveTO,
            @Nonnull CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            boolean isPrimaryChangeExplanation) {
        ActionDTOs.MoveExplanation moveExplanation = moveTO.getMoveExplanation();
        final ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        final long actionTargetId = slInfo.getCollapsedBuyerId().orElse(slInfo.getBuyerId());
        ChangeProviderExplanation.Builder changeProviderExplanation;
        switch (moveExplanation.getExplanationTypeCase()) {
            case COMPLIANCE:
                changeProviderExplanation = null;
                // For Optimized Migration Plan VM/Volume moves, we want to
                // show Performance/Efficiency actions instead of Compliance.
                if (complianceExplanationOverride != null) {
                    final Pair<Boolean, ChangeProviderExplanation> overrideAndExplanation =
                            complianceExplanationOverride.apply(moveTO, projectedTopology);
                    if (overrideAndExplanation.first) {
                        // Need to override, see if there is an explanation available, e.g for vol.
                        if (overrideAndExplanation.second != null) {
                            changeProviderExplanation = ChangeProviderExplanation
                                    .newBuilder(overrideAndExplanation.second);
                        } else {
                            // No existing override explanation, get one from tracker, e.g for VMs.
                            changeProviderExplanation = changeExplanationFromTracker(moveTO, savings, projectedTopology)
                                    .orElse(ChangeProviderExplanation.newBuilder().setEfficiency(
                                            ChangeProviderExplanation.Efficiency.getDefaultInstance()));
                        }
                    }
                }
                if (changeProviderExplanation == null) {
                    Compliance.Builder compliance =
                            ChangeProviderExplanation.Compliance.newBuilder()
                                    .addAllMissingCommodities(moveExplanation.getCompliance()
                                    .getMissingCommoditiesList()
                                    .stream()
                                    .map(commodityConverter::commodityIdToCommodityType)
                                    .filter(commType -> !tierExcluder.isCommodityTypeForTierExclusion(
                                            commType))
                                    .map(commType2ReasonCommodity())
                                    .collect(Collectors.toList()));
                    if (isPrimaryChangeExplanation) {
                        tierExcluder.getReasonSettings(actionTO)
                                .ifPresent(compliance::addAllReasonSettings);
                    }
                    changeProviderExplanation =
                            ChangeProviderExplanation.newBuilder().setCompliance(compliance);
                }
                break;
            case CONGESTION:
                // If the congested commodities contains segmentationCommodities, we categorize such an action as COMPLIANCE.
                List<ReasonCommodity> congestedSegments = moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                        .map(commodityConverter::commodityIdToCommodityTypeAndSlot)
                        .map(p -> p.first)
                        .filter(ct -> ct.getType() == CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                        .map(commType2ReasonCommodity())
                        .collect(Collectors.toList());
                if (!congestedSegments.isEmpty()) {
                    changeProviderExplanation = ChangeProviderExplanation.newBuilder()
                            .setCompliance(ChangeProviderExplanation.Compliance.newBuilder()
                            .addAllMissingCommodities(congestedSegments));
                    break;
                }
                // For cloud entities we create either an efficiency or congestion change explanation
                Optional<ChangeProviderExplanation.Builder> explanationFromTracker = changeExplanationFromTracker(moveTO, savings, projectedTopology);
                ChangeProviderExplanation.Builder explanationFromM2 = ChangeProviderExplanation.newBuilder().setCongestion(
                        ChangeProviderExplanation.Congestion.newBuilder()
                        .addAllCongestedCommodities(moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                                .map(commodityConverter::commodityIdToCommodityTypeAndSlot)
                                .map(commTypeAndSlot2ReasonCommodity(actionTargetId, moveTO.getSource()))
                                .collect(Collectors.toList()))
                        .build());
                changeProviderExplanation = explanationFromTracker.isPresent()
                        ? mergeM2AndTrackerExplanations(moveTO, explanationFromM2, explanationFromTracker.get())
                        : explanationFromM2;
                break;
            case EVACUATION:
                changeProviderExplanation = ChangeProviderExplanation.newBuilder()
                    .setEvacuation(ChangeProviderExplanation.Evacuation.newBuilder()
                        .setSuspendedEntity(moveExplanation.getEvacuation().getSuspendedTrader())
                        .build());
                break;
            case INITIALPLACEMENT:
                final ShoppingListInfo shoppingListInfo =
                    shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
                // Create Evacuation instead of InitialPlacement
                // if the original supplier is in MAINTENANCE or FAILOVER state.
                if (shoppingListInfo.getSellerId() != null &&
                    projectedTopology.containsKey(shoppingListInfo.getSellerId()) &&
                    evacuationEntityState.contains(
                        projectedTopology.get(shoppingListInfo.getSellerId()).getEntity().getEntityState())) {
                    changeProviderExplanation = ChangeProviderExplanation.newBuilder()
                        .setEvacuation(ChangeProviderExplanation.Evacuation.newBuilder()
                            .setSuspendedEntity(shoppingListInfo.getSellerId())
                            .setIsAvailable(false)
                            .build());
                } else {
                    changeProviderExplanation = ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(ChangeProviderExplanation.InitialPlacement.getDefaultInstance());
                }
                break;
            case PERFORMANCE:
                // For cloud entities we explain create either an efficiency or congestion change
                // explanation
                changeProviderExplanation = changeExplanationFromTracker(moveTO, savings, projectedTopology)
                    .orElse(ChangeProviderExplanation.newBuilder()
                        .setPerformance(ChangeProviderExplanation.Performance.getDefaultInstance()));
                break;
            default:
                logger.error("Unknown explanation case for move action: "
                        + moveExplanation.getExplanationTypeCase());
                changeProviderExplanation = ChangeProviderExplanation.getDefaultInstance().toBuilder();
                break;
        }
        changeProviderExplanation.setIsPrimaryChangeProviderExplanation(isPrimaryChangeExplanation);
        return changeProviderExplanation;
    }

    @Nonnull
    private Optional<ChangeProviderExplanation.Builder> changeExplanationFromTracker(
            @Nonnull final MoveTO moveTO, @Nonnull final CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        final long actionTargetId = slInfo.getCollapsedBuyerId().orElse(slInfo.getBuyerId());
        long sellerId = slInfo.getSellerId();
        Set<CommodityType> underutilizedCommodityTypes = commoditiesResizeTracker.getUnderutilizedCommodityTypes(actionTargetId, sellerId);
        // Pair[LowerCapacity, HigherCapacity].
        Pair<Set<CommodityType>, Set<CommodityType>> lowerAndHigherCapacityPair =
                calculateCommWithChangingProjectedCapacity(
                        actionTargetId,
                        underutilizedCommodityTypes,
                        projectedTopology);

        // Collection of commodities with lower projected capacity in projected topology.
        // Also include commodities with unknown capacity.
        final Set<CommodityType> lowerProjectedCapacity = lowerAndHigherCapacityPair.first;
        // Collection of commodities with higher projected capacity in projected topology.
        final Set<CommodityType> higherProjectedCapacity = lowerAndHigherCapacityPair.second;

        Optional<ChangeProviderExplanation.Builder> explanation =
                // Check if this SE has congested commodities pre-stored.
                Optional.ofNullable(getCongestedExplanationFromTracker(actionTargetId, sellerId)
                        // If this SE is cloud and has RI change.
                        .orElseGet(() -> getRIIncreaseExplanation(actionTargetId, moveTO)
                                // If this SE has underutilized commodities pre-stored.
                                .orElseGet(() -> getUnderUtilizedExplanationFromTracker(actionTargetId, sellerId, lowerProjectedCapacity)
                                        // Get from savings
                                        .orElseGet(() -> getExplanationFromSaving(savings)
                                                    .orElse(null)))));
        // To add commodities whose projected capacities are higher than current capacities,
        // if changeProviderExplanation is Efficiency.
        boolean isEfficiencyOrNoChange = !explanation.isPresent() ||
                explanation.get().hasEfficiency();
        if (isEfficiencyOrNoChange  && !higherProjectedCapacity.isEmpty()) {
            ChangeProviderExplanation.Builder changeProviderExplanation = explanation.orElseGet(ChangeProviderExplanation::newBuilder);
            // Add commodities which are scaling up to Efficiency. scaleUpCommodity.
            explanation = Optional.of(changeProviderExplanation.setEfficiency(changeProviderExplanation.getEfficiencyBuilder()
                    .addAllScaleUpCommodity(higherProjectedCapacity)));
        }
        // Default move explanation.
        return Optional.ofNullable(explanation
                .orElseGet(() -> getDefaultExplanationForCloud(actionTargetId, moveTO)
                        .orElse(null)));
    }

    private Optional<ChangeProviderExplanation.Builder> getCongestedExplanationFromTracker(long actionTargetId, long sellerId) {
        Set<CommodityType> congestedCommodityTypes = commoditiesResizeTracker.getCongestedCommodityTypes(actionTargetId, sellerId);
        if (congestedCommodityTypes != null && !congestedCommodityTypes.isEmpty()) {
            Congestion.Builder congestionBuilder = ChangeProviderExplanation.Congestion.newBuilder()
                    .addAllCongestedCommodities(commTypes2ReasonCommodities(congestedCommodityTypes));
            logger.debug("CongestedCommodities from tracker for buyer:{}, seller: {} : [{}]",
                    actionTargetId, sellerId,
                    congestedCommodityTypes.stream().map(type -> type.toString()).collect(Collectors.joining()));
            return Optional.of(ChangeProviderExplanation.newBuilder().setCongestion(
                    congestionBuilder));
        }
        return Optional.empty();
    }

    @Nonnull
    private Optional<ChangeProviderExplanation.Builder> getUnderUtilizedExplanationFromTracker(
            long actionTargetId,
            long sellerId,
            @Nonnull Set<CommodityType> underutilizedCommodityTypes) {
        if (!underutilizedCommodityTypes.isEmpty()) {
            Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder()
                    .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(underutilizedCommodityTypes));
            logger.debug("Underutilized Commodities from tracker for buyer:{}, seller: {} : [{}]",
                    actionTargetId, sellerId,
                    underutilizedCommodityTypes.stream().map(AbstractMessage::toString).collect(Collectors.joining()));
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
        }
        return Optional.empty();
    }

    /**
     * Commodities with either lower projected capacity values or higher projected values compared to original topology.
     * If a underutilizedCommodityType is not found in either  projectedCommoditySoldMap or originalCommoditySoldMap,
     * then it is anyways accepted as an underUtilizedCommodityType.
     *
     * @param actionTargetId   current action.
     * @param underutilizedCommodityTypes set of underutilized {@link CommodityType}.
     * @param projectedTopology    projected topology indexed by id.
     * @return Pair set of lower projected commodities and higher projected commodity.
     */
    @Nonnull
    private Pair<Set<CommodityType>, Set<CommodityType>> calculateCommWithChangingProjectedCapacity(
            final long actionTargetId,
            @Nonnull final Set<CommodityType> underutilizedCommodityTypes,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {

        // Get projected commoditySold for entity indexed by underutilizedCommodityTypes.
        final Map<CommodityType, CommoditySoldDTO> projectedCommoditySoldMap = projectedTopology.containsKey(actionTargetId) ?
                projectedTopology.get(actionTargetId).getEntity()
                        .getCommoditySoldListList().stream()
                        .filter(commoditySoldDTO -> underutilizedCommodityTypes.contains(commoditySoldDTO.getCommodityType()))
                        .collect(Collectors.toMap(CommoditySoldDTO::getCommodityType, Function.identity()))
                : Collections.emptyMap();

        // Get original commoditySold for entity indexed by underutilizedCommodityTypes.
        final Map<CommodityType, CommoditySoldDTO> originalCommoditySoldMap = originalTopology.containsKey(actionTargetId) ?
                originalTopology.get(actionTargetId)
                        .getCommoditySoldListList().stream()
                        .filter(commoditySoldDTO -> underutilizedCommodityTypes.contains(commoditySoldDTO.getCommodityType()))
                        .collect(Collectors.toMap(CommoditySoldDTO::getCommodityType, Function.identity()))
                : Collections.emptyMap();

        // map of commodities with lower or unknown projected capacity values.
        Set<CommodityType> lowerProjectedCapacity = Sets.newHashSet();
        Set<CommodityType> higherProjectedCapacity = Sets.newHashSet();
        underutilizedCommodityTypes
                .forEach(underutilizedCommodityType -> {
                    if (projectedCommoditySoldMap.containsKey(underutilizedCommodityType) &&
                            originalCommoditySoldMap.containsKey(underutilizedCommodityType) &&
                            projectedCommoditySoldMap.get(underutilizedCommodityType).hasCapacity() &&
                            originalCommoditySoldMap.get(underutilizedCommodityType).hasCapacity()) {
                        final double projectedCapacity =
                                projectedCommoditySoldMap.get(underutilizedCommodityType).getCapacity();
                        final double currentCapacity =
                                originalCommoditySoldMap.get(underutilizedCommodityType).getCapacity();
                        // capture the change to projected Capacity in Set.
                        if (projectedCapacity > currentCapacity) {
                            higherProjectedCapacity.add(underutilizedCommodityType);
                        } else if (projectedCapacity < currentCapacity) {
                            lowerProjectedCapacity.add(underutilizedCommodityType);
                        }
                    }
                });
        return new Pair<>(lowerProjectedCapacity, higherProjectedCapacity);
    }

    private Optional<ChangeProviderExplanation.Builder> getRIIncreaseExplanation(long id, @Nonnull final MoveTO moveTO) {
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())
                && TopologyDTOUtil.isPrimaryTierEntityType(cloudTc.getMarketTier(moveTO.getDestination()).getTier().getEntityType())
                && didRiCoverageIncrease(id)) {
            Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder()
                    .setIsRiCoverageIncreased(true);
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
        }
        return Optional.empty();
    }

    private Optional<ChangeProviderExplanation.Builder> getExplanationFromSaving(@Nonnull final CalculatedSavings savings) {
        if (savings.savingsAmount.getValue() > 0) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.newBuilder().setIsWastedCost(true)));
        }
        return Optional.empty();
    }

    private Optional<ChangeProviderExplanation.Builder> getDefaultExplanationForCloud(long id, @Nonnull final MoveTO moveTO) {
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())) {
            logger.error("Could not explain cloud scale action. MoveTO = {} .Entity oid = {}", moveTO, id);
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.getDefaultInstance()));
        }
        return Optional.empty();
    }

    /**
     * Merge the tracker explanation into the m2 congestion explanation.
     * If exp2 is congestion, then merge them.
     * If exp2 is efficiency and has different commodity as exp1, keep only the different commodity in exp1.
     * If exp2 is efficiency and has the same commodity as exp1, use exp2.
     * If exp2 is efficiency but has no commodity, it is efficiency for other reason(Ri, saving, etc) keep the result as efficiency then.
     * eg, exp1: congestion [PoolCPU, PoolMem, ImageMem, ImageCPU], exp2: congestion [ImageMem, ImageCPU] -> congestion[PoolCPU, PoolMem, ImageMem, ImageCPU]
     * exp1: congestion [PoolCPU, PoolMem, ImageMem, ImageCPU], exp2: efficiency [ImageMem, ImageCPU] -> congestion[PoolCPU, PoolMem]
     * exp1: congestion [ImageMem, ImageCPU], exp2: [ImageMem, ImageCPU] -> efficiency[ImageMem, ImageCPU]
     *
     * @param moveTO The moveTO to interpret.
     * @param m2Explanation A congestion explanation
     * @param trackerExplanation A congestion or underutilized explanation.
     * @return The merged explanation
     */
    @Nonnull
    private ChangeProviderExplanation.Builder mergeM2AndTrackerExplanations(
            @Nonnull MoveTO moveTO, @Nonnull ChangeProviderExplanation.Builder m2Explanation, @Nonnull ChangeProviderExplanation.Builder trackerExplanation) {
        //For cloud resizing we ignore m2 explanation
        if (cloudTc.isMarketTier(moveTO.getSource()) && cloudTc.isMarketTier(moveTO.getDestination())) {
            return trackerExplanation;
        }
        if (m2Explanation.getCongestion() != null) {
            Set<ReasonCommodity> mergedCommodities = new HashSet<ReasonCommodity>(
                    m2Explanation.getCongestion().getCongestedCommoditiesList());
            if (trackerExplanation.getCongestion() != null) {
                List<ReasonCommodity> trackerCongestedCommodities = trackerExplanation.getCongestion().getCongestedCommoditiesList();
                if (trackerCongestedCommodities != null && !trackerCongestedCommodities.isEmpty()) {
                    mergedCommodities.addAll(trackerCongestedCommodities);
                    return ChangeProviderExplanation.newBuilder().setCongestion(
                        ChangeProviderExplanation.Congestion.newBuilder()
                                .addAllCongestedCommodities(mergedCommodities));
                }
            }
            if (trackerExplanation.getEfficiency() != null) {
                List<ReasonCommodity> exp2UnderutilizedCommodities = trackerExplanation.getEfficiency().getUnderUtilizedCommoditiesList();
                if (exp2UnderutilizedCommodities != null && !exp2UnderutilizedCommodities.isEmpty()) {
                    mergedCommodities.removeAll(exp2UnderutilizedCommodities);
                    if (mergedCommodities.isEmpty()) {
                        return trackerExplanation;
                    }
                    return ChangeProviderExplanation.newBuilder().setCongestion(
                        ChangeProviderExplanation.Congestion.newBuilder()
                                .addAllCongestedCommodities(mergedCommodities));
                }
            }
        }
        return trackerExplanation;
    }

    /**
     * Check if RI coverage increased for entity.
     * Get the original number of coupons covered from the original EntityReservedInstanceCoverage
     * Get the projected number of coupons covered from the projected EntityReservedInstanceCoverage
     * The RI coverage for the entity increased if the projected coupons covered is greater than
     * the original coupons covered.
     *
     * @param entityId id of entity for which ri coverage increase is to be checked
     * @return true if the RI coverage increased. False otherwise.
     */
    private boolean didRiCoverageIncrease(long entityId) {
        double projectedCouponsCovered = 0;
        double originalCouponsCovered = 0;
        EntityReservedInstanceCoverage projectedCoverage = projectedRICoverageCalculator
            .getProjectedRICoverageForEntity(entityId);
        if (projectedCoverage != null && !projectedCoverage.getCouponsCoveredByRiMap().isEmpty()) {
            projectedCouponsCovered = projectedCoverage
                .getCouponsCoveredByRiMap().values().stream().reduce(0.0, Double::sum);
        }
        Optional<EntityReservedInstanceCoverage> originalCoverage = cloudTc.getRiCoverageForEntity(entityId);
        if (originalCoverage.isPresent() && !originalCoverage.get().getCouponsCoveredByRiMap().isEmpty()) {
            originalCouponsCovered = originalCoverage.get()
                .getCouponsCoveredByRiMap().values().stream().reduce(0.0, Double::sum);
        }
        return projectedCouponsCovered > originalCouponsCovered;
    }

    private ActionEntity createActionTargetEntity(@Nonnull ShoppingListInfo shoppingListInfo,
                                                  @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long targetEntityId = shoppingListInfo.getCollapsedBuyerId().orElse(shoppingListInfo.getBuyerId());
        return createActionEntity(targetEntityId, projectedTopology);
    }

    private ActionEntity createActionEntity(final long id,
                            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final TopologyEntityDTO projectedEntity = projectedTopology.get(id).getEntity();
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(projectedEntity.getEntityType())
                .setEnvironmentType(projectedEntity.getEnvironmentType())
                .build();
    }

    private static Function<CommodityType, ReasonCommodity> commType2ReasonCommodity() {
        return ct -> ReasonCommodity.newBuilder().setCommodityType(ct).build();
    }

    private Function<Pair<CommodityType, Optional<Integer>>, ReasonCommodity>
        commTypeAndSlot2ReasonCommodity(final long buyer, final long source) {
        return ct -> {
            final CommodityType commType = ct.first;
            final Optional<Integer> slot = ct.second;
            final ReasonCommodity.Builder builder = ReasonCommodity.newBuilder().setCommodityType(commType);
            slot.ifPresent(sl -> builder.setTimeSlot(TimeSlotReasonInformation.newBuilder()
                .setSlot(sl)
                .setTotalSlotNumber(getTotalTimeSlotNumber(commType, buyer, source))
                .build()));
            return builder.build();
        };
    }

    private static Collection<ReasonCommodity>
            commTypes2ReasonCommodities(Collection<CommodityType> types) {
        return types.stream().map(commType2ReasonCommodity()).collect(Collectors.toList());
    }

    /**
     * Get total number of time slots for time slot commodity.
     *
     * @param commodityType Time slot {@link CommodityType}
     * @param buyer Buyer Id
     * @param moveSource Move source ID
     * @return Total number of time slots, or 0 if cannot be determined
     */
    private int getTotalTimeSlotNumber(@Nonnull final CommodityType commodityType,
            final long buyer, final long moveSource) {
        final TopologyEntityDTO origTopology = originalTopology.get(buyer);
        if (null == origTopology) {
            logger.error("No originalTopology value found for key {}", () -> moveSource);
            return 0;
        }
        final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider =
            origTopology.getCommoditiesBoughtFromProvidersList().stream()
                .filter(e -> moveSource == e.getProviderId()).findFirst().orElse(null);
        if (commoditiesBoughtFromProvider == null) {
            logger.error("No commodities bought found for provider {}", () -> moveSource);
            return 0;
        }
        final CommodityBoughtDTO commodityBought = commoditiesBoughtFromProvider.getCommodityBoughtList()
            .stream().filter(e -> commodityType.getType() == (e.getCommodityType().getType()))
            .findFirst().orElse(null);
        if (commodityBought == null) {
            logger.error("No CommodityBoughtDTO found for commodity type {}, entity id {}",
                () -> commodityType, () -> moveSource);
            return 0;
        }
        if (commodityBought.hasHistoricalUsed() && commodityBought.getHistoricalUsed().getTimeSlotCount() > 0) {
            return commodityBought.getHistoricalUsed().getTimeSlotCount();
        } else {
            logger.error("No timeslots found for commodity type {}, entity id {}",
                () -> commodityType, () -> moveSource);
            return 0;
        }
    }

    /**
     * Get the original providerId of a buyer.
     *
     * @param providerId a topology entity oid or a market generated providerId
     * @param buyerId the buyerId
     * @param originalCloudTopology the original {@link CloudTopology}
     * @return the original providerId
     */
    private Optional<Long> getOriginalProviderId(
            final long providerId, final long buyerId,
            @Nonnull final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        if (cloudTc.isMarketTier(providerId)) {
            final MarketTier marketTier = cloudTc.getMarketTier(providerId);
            if (marketTier == null) {
                return Optional.empty();
            }

            long originalProviderId = marketTier.getTier().getOid();
            if (marketTier.hasRIDiscount()) {
                // RIDiscountedMarketTiers can represent multiple tiers in case of instance size
                // flexible RIs. So we cannot directly pick up the tier from RIDiscountedTier.
                // If the destination is an RIDiscountedMarketTier, it means we are looking for
                // the primary tier. We can fetch this information from the original cloud topology.
                originalProviderId = originalCloudTopology.getPrimaryTier(buyerId).get().getOid();
            }
            return Optional.of(originalProviderId);
        }
        return Optional.of(providerId);
    }


    /**
     * Class that wraps a savings amount to model a savings number that was actually
     * calculated, as opposed to {@link com.vmturbo.market.topology.conversions.ActionInterpreter.NoSavings}
     * which models when we could not calculate any savings for an entity.
     */
    @Immutable
    static class CalculatedSavings {
        public final TraxNumber savingsAmount;

        CalculatedSavings(@Nonnull final TraxNumber savingsAmount) {
            this.savingsAmount = savingsAmount;
        }

        /**
         * Apply the savings to an action by setting the action's savingsPerHour
         * field.
         *
         * @param action The action to apply the savings to.
         */
        public void applySavingsToAction(@Nonnull final Action.Builder action) {
            action.setSavingsPerHour(CurrencyAmount.newBuilder()
                .setAmount(savingsAmount.getValue()));
        }

        /**
         * Whether the calculated savings actually has any savings. Note that even zero savings,
         * when it is calculated as opposed to unavailable due to being {@link NoSavings},
         * will still return true here.
         *
         * @return Whether the calculated savings actually has any savings.
         */
        public boolean hasSavings() {
            return true;
        }
    }

    /**
     * Models a situation where we could not actually calculate a savings amount.
     */
    @Immutable
    static class NoSavings extends CalculatedSavings {
        /**
         * Create a new {@link NoSavings} object. Note that we cannot have a helper that
         * creates the {@link TraxNumber} for the caller because then the call site in the logs
         * for the {@link TraxNumber} creation would be our factory method, not the line calling
         * the factory method.
         *
         * @param savingsAmount The amount saved. Should be zero.
         */
        NoSavings(@Nonnull final TraxNumber savingsAmount) {
            super(savingsAmount);

            Preconditions.checkArgument(savingsAmount.valueEquals(0),
                "No Savings must have a zero savings amount");
        }

        /**
         * Do not apply {@link NoSavings} to actions.
         *
         * @param action The action to apply the savings to.
         */
        @Override
        public void applySavingsToAction(@Nonnull final Action.Builder action) {

        }

        /**
         * {@link NoSavings} never has any savings.
         *
         * @return false
         */
        @Override
        public boolean hasSavings() {
            return false;
        }
    }
}
