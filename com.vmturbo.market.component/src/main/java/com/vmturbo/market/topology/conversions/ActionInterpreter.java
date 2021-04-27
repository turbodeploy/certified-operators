package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.TopologyConversionUtils.calculateFactorForCommodityValues;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.TimeSlotReasonInformation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
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
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.SingleRegionMarketTier;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityLookupType;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityTypeWithLookup;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
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
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.Pair;
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
                    final ActionType translatedActionType = findTranslatedActionType(actionTO, originalCloudTopology);
                    if (translatedActionType == ActionType.SCALE) {
                        action.getInfoBuilder().setScale(interpretScaleAction(actionTO.getMove(),
                                projectedTopology, originalCloudTopology));
                        actionList.add(action.build());
                        break;
                    } else if (translatedActionType == ActionType.ALLOCATE) {
                        action.getInfoBuilder().setAllocate(interpretAllocateAction(actionTO.getMove(),
                                projectedTopology));
                        actionList.add(action.build());
                        break;
                    } else if (translatedActionType == ActionType.MOVE) {
                        Optional<ActionDTO.Move> move = interpretMoveAction(actionTO.getMove(),
                                projectedTopology, originalCloudTopology);
                        if (move.isPresent()) {
                            action.getInfoBuilder().setMove(move.get());
                            actionList.add(action.build());
                        }
                        break;
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
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                "interpretAction of actionTO " + actionTO, e.getMessage(), e);

            final String message = "Related shopping list info: ";
            final String messageReconfigureProvider = "Related provider info: ";
            // Add more info to the log
            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    logger.error(message + shoppingListOidToInfos.get(actionTO.getMove().getShoppingListToMove()));
                    break;
                case RECONFIGURE:
                    if (actionTO.getReconfigure().hasConsumer()) {
                        logger.error(message + shoppingListOidToInfos.get(actionTO.getReconfigure().getConsumer().getShoppingListToReconfigure()));
                    } else if (actionTO.getReconfigure().hasProvider()) {
                        long providerOid = actionTO.getReconfigure().getProvider().getTargetTrader();
                        if (oidToProjectedTraderTOMap.containsKey(providerOid)) {
                            logger.error(messageReconfigureProvider + oidToProjectedTraderTOMap.get(providerOid).getDebugInfoNeverUseInCode());
                        }
                    }
                    break;
                case COMPOUND_MOVE:
                    actionTO.getCompoundMove().getMovesList().forEach(move ->
                            logger.error(message + shoppingListOidToInfos.get(move.getShoppingListToMove())));
                    break;
                default:
                    break;
            }
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
        final ActionType translatedActionType = findTranslatedActionType(actionTO, originalCloudTopology);
        try (TraxContext traxContext = Trax.track("SAVINGS", actionTO.getActionTypeCase().name())) {
            savings = calculateActionSavings(actionTO,
                    originalCloudTopology, projectedCosts, topologyCostCalculator);

            // The action importance should never be infinite, as setImportance() will fail.
            action = Action.newBuilder()
                    // Assign a unique ID to each generated action.
                    .setId(IdentityGenerator.next())
                    .setDeprecatedImportance(actionTO.getImportance())
                    .setExplanation(interpretExplanation(actionTO, savings, projectedTopology, translatedActionType))
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
            case DEACTIVATE:
                final DeactivateTO deactivateTO = actionTO.getDeactivate();
                long deactivatedEntityOid = deactivateTO.getTraderToDeactivate();
                TraderTO deactivatingEntityTO =  oidToProjectedTraderTOMap.get(deactivatedEntityOid);

                // get the entity DTO from the original topology
                TopologyEntityDTO deactivatingEntity =  originalTopology.get(deactivatedEntityOid);

                if (deactivatingEntityTO == null || deactivatingEntity == null) {
                    return new NoSavings(trax(0, "savings calculation for "
                            + actionTO.getActionTypeCase().name()));
                }

                // get the cost journal of the entity from the source topology
                // since the entity in the projected topology is in suspended state
                // and does not have cost associated with it
                Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal =
                        topologyCostCalculator.calculateCostForEntity(
                                originalCloudTopology, deactivatingEntity);

                return calculateHorizontalScalingActionSavings(sourceCostJournal,
                        deactivatingEntityTO, deactivatingEntity, true);

            case PROVISION_BY_SUPPLY:
                final ProvisionBySupplyTO provisionBySupply = actionTO.getProvisionBySupply();
                long provisionedEntityOid = provisionBySupply.getModelSeller();
                TraderTO provisionedEntityTO =  oidToProjectedTraderTOMap.get(provisionedEntityOid);

                // get the entity DTO from the original topology
                TopologyEntityDTO provisionedEntity =  originalTopology.get(provisionedEntityOid);

                if (provisionedEntityTO == null || provisionedEntity == null) {
                    return new NoSavings(trax(0, "investment calculation for "
                            + actionTO.getActionTypeCase().name()));
                }
                // get the cost journal of the model seller
                // since the costs for the provisioned (cloned) entity is not computed
                final CostJournal<TopologyEntityDTO> origEntityCostJournal
                                                 = projectedCosts.get(provisionedEntityOid);

                return calculateHorizontalScalingActionSavings(Optional.ofNullable(origEntityCostJournal),
                        provisionedEntityTO, provisionedEntity, false);
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

    @Nonnull
    private CalculatedSavings calculateHorizontalScalingActionSavings(
                                    Optional<CostJournal<TopologyEntityDTO>> entityCostJournal,
                                    TraderTO projectedEntityTO,
                                    @Nonnull TopologyEntityDTO cloudEntityHzScaling,
                                    boolean isSuspend) {
        if (!entityCostJournal.isPresent()) {
            return new NoSavings(trax(0, "no entity cost journal"));
        }
        CostJournal<TopologyEntityDTO> costJournal = entityCostJournal.get();

        // need suppliers to get the market tier
        List<Long> suppliers = projectedEntityTO.getShoppingListsList()
                                            .stream()
                                            .map(sl -> sl.getSupplier())
                                            .collect(Collectors.toList());

        // get cost for the compute tier only
        TraxNumber costs = suppliers.stream()
                .map(cloudTc::getMarketTier)
                .filter(Objects::nonNull)
                .filter(marketTier -> marketTier.getTier() != null
                        && TopologyDTOUtil.isPrimaryTierEntityType(marketTier.getTier().getEntityType()))
                .map(marketTier -> getOnDemandCostForMarketTier(cloudEntityHzScaling, marketTier, costJournal))
                .collect(TraxCollectors.sum("horizontal-scale-vm-in-cloud"));

        final String savingsDescription = String.format("%s for %s \"%s\" (%d)",
                    isSuspend ? "Savings" : "Investment",
                    EntityType.forNumber(cloudEntityHzScaling.getEntityType()).name(),
                    cloudEntityHzScaling.getDisplayName(),
                    cloudEntityHzScaling.getOid());

        // accumulated cost
        final TraxNumber savings = costs.times(isSuspend?1.0:-1.0).compute(savingsDescription);
        return new CalculatedSavings(savings);
    }

    @Nullable
    private TopologyEntityDTO getTopologyEntityMoving(MoveTO move) {
        final ShoppingListInfo shoppingListInfo =
                shoppingListOidToInfos.get(move.getShoppingListToMove());
        long entityMovingId = Optional.ofNullable(shoppingListInfo.getActingId())
                        .orElse(shoppingListInfo.getBuyerId());
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
            TraxNumber spotCost = journal.getHourlyCostForCategory(CostCategory.SPOT);
            if (spotCost == null) {
                spotCost = Trax.trax(0.0d);
            }

            // TODO Roop: remove this condition. OM-61424.
            TraxNumber dbStorageCost = cloudEntityMoving.getEntityType() == EntityType.DATABASE_VALUE ?
                    journal.getHourlyCostFilterEntries(
                            CostCategory.STORAGE,
                            CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER) : Trax.trax(0.0);

            totalOnDemandCost = Stream.of(onDemandComputeCost, licenseCost, reservedLicenseCost, ipCost, dbStorageCost, spotCost)
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
                    shoppingList.getBuyerId(), destination.getDisplayName());
                return Optional.empty();
            }
            List<ChangeProvider> changeProviderList = createChangeProviders(moveTO,
                    projectedTopology, originalCloudTopology, shoppingList.getBuyerId());
            if (!CollectionUtils.isEmpty(changeProviderList)) {
                ActionDTO.Move.Builder builder = ActionDTO.Move.newBuilder()
                        .setTarget(createActionEntity(shoppingList.getBuyerId(), projectedTopology))
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
     * Convert the {@link MoveTO} received from M2 into a {@link ActionDTO.Allocate} action -
     * Create an action entity and set it as the target for the Allocate.
     * Set the WorkloadTier.
     *
     * @param moveTO the input {@link MoveTO}
     * @param projectedTopology a map of entity id to the {@link ProjectedTopologyEntity}.
     * @return {@link ActionDTO.Scale} representing the Scale
     */
    private ActionDTO.Allocate interpretAllocateAction(@Nonnull final MoveTO moveTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final ShoppingListInfo shoppingListInfo =
                shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        // Set action target entity.
        final ActionEntity actionTargetEntity = createActionTargetEntity(shoppingListInfo, projectedTopology);
        ActionDTO.Allocate.Builder builder = ActionDTO.Allocate.newBuilder().setTarget(actionTargetEntity);
        if (moveTO.hasSource()) {
            final Optional<Long> currentTier = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, shoppingListInfo.getBuyerId(), true);
            if (currentTier.isPresent()) {
                builder.setWorkloadTier(createActionEntity(currentTier.get(),
                        projectedTopology));
            } else {
                throw new IllegalStateException(
                        "Market returned invalid source tier for Move: " + moveTO);
            }
        }
        return builder.build();
    }

    /**
     * Find the region corresponding to the market tier which is a source of a move action.
     * @param sourceMarketTier the source market tier.
     * @param originalCloudTopology the original cloud topology.
     * @param targetOid The target of the move.
     * @return the region corresponding to the market tier.
     */
    private TopologyEntityDTO getSourceRegionFromMarketTier(@Nonnull MarketTier sourceMarketTier,
            CloudTopology<TopologyEntityDTO> originalCloudTopology,
            Long targetOid) {
        TopologyEntityDTO sourceRegion = null;
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
        return sourceRegion;
    }

    /**
     * Find the region corresponding to the market tier which is a destination of a move action.
     * @param destMarketTier the destination market tier.
     * @param move the corresponding move action.
     * @return the region corresponding to the market tier.
     */
    private TopologyEntityDTO getDestinationRegionFromMarketTier(@Nonnull MarketTier destMarketTier,
            @Nonnull final MoveTO move){
        TopologyEntityDTO destinationRegion = null;
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
        return destinationRegion;
    }

    /**
     * determine if the move action is a accounting action.
     * @param destinationRegion the destination region.
     * @param sourceRegion the source region.
     * @param sourceMarketTier the source market tier
     * @param move the move action.
     * @param sourceTier the topololgyEntityDTO of source
     * @param destTier the topololgyEntityDTO of destination
     * @param targetOid The target of the move.
     * @return true if the action is a accounting action (RI optimisation)
     */
    private boolean isAccountingAction (TopologyEntityDTO destinationRegion,
            TopologyEntityDTO sourceRegion,
            MarketTier sourceMarketTier,
            @Nonnull final MoveTO move,
            TopologyEntityDTO sourceTier,
            TopologyEntityDTO destTier,
            Long targetOid) {
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
            return !areEqual(originalRICoverage, projectedRICoverage);
        } else {
            return false;
        }
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
    public ActionDTO.Scale interpretScaleAction(@Nonnull final MoveTO moveTO,
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
        } else {
            // only add primary provider if we have it available
            if (shoppingListInfo.getSellerId() != null) {
                final MarketTier currentTier = cloudTc.getMarketTier(shoppingListInfo.getSellerId());
                if (currentTier != null) {
                    builder.setPrimaryProvider(createActionEntity(currentTier.getTier().getOid(), projectedTopology));
                }
            }
        }

        // Interpret commodities change.
        List<ResizeInfo> resizeInfoList = createCommodityResizeInfo(moveTO, actionTargetEntity);
        if (!CollectionUtils.isEmpty(resizeInfoList)) {
            builder.addAllCommodityResizes(resizeInfoList);
        }
        if (moveTO.hasScalingGroupId()) {
            builder.setScalingGroupId(moveTO.getScalingGroupId());
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
        if (reconfigureTO.hasConsumer()
            && reconfigureTO.getConsumer().hasShoppingListToReconfigure()
            && shoppingListOidToInfos.get(reconfigureTO.getConsumer().getShoppingListToReconfigure()) != null) {
            final ShoppingListInfo shoppingListInfo =
                    shoppingListOidToInfos.get(reconfigureTO.getConsumer().getShoppingListToReconfigure());
            final ActionDTO.Reconfigure.Builder builder = ActionDTO.Reconfigure.newBuilder()
                    .setTarget(createActionTargetEntity(shoppingListInfo, projectedTopology));

            if (reconfigureTO.getConsumer().hasSource()) {
                Optional<Long> providerIdOptional = getOriginalProviderId(
                    reconfigureTO.getConsumer().getSource(), shoppingListInfo.getBuyerId(), originalCloudTopology);
                providerIdOptional.ifPresent(providerId ->
                    builder.setSource(createActionEntity(providerId, projectedTopology)));
            }
            if (reconfigureTO.hasScalingGroupId()) {
                builder.setScalingGroupId(reconfigureTO.getScalingGroupId());
            }
            builder.setIsProvider(false);
            return builder.build();
        } else if (reconfigureTO.hasProvider()) {
            final ActionDTO.Reconfigure.Builder builder = ActionDTO.Reconfigure.newBuilder()
                    .setTarget(createActionEntity(reconfigureTO.getProvider().getTargetTrader(), projectedTopology))
                    .setIsProvider(true);
            if (reconfigureTO.getProvider().getAddition()) {
                // add commodity
                builder.setIsAddition(true);
                if (reconfigureTO.getProvider().hasModelTrader()) {
                    builder.setSource(createActionEntity(reconfigureTO.getProvider().getModelTrader(), projectedTopology));
                }
            } else {
                // remove commodity
                builder.setIsAddition(false);
            }
            return builder.build();
        } else {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for RECONFIGURE: " + reconfigureTO);
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
        if (resizeTO.hasReasonCommodity()) {
            final CommodityType reasonCommodityType =
                    commodityConverter.marketToTopologyCommodity(resizeTO.getReasonCommodity())
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "Resize commodity can't be converted to topology commodity format! "
                                            + cs));
            resizeBuilder.setReason(reasonCommodityType);

        }
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
     * This method is not called for accounting action.
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
            destinationRegion = getDestinationRegionFromMarketTier(destMarketTier, move);
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
            Optional<Long> destinationTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(move, target.getOid(), false);
            if (destinationTierId.isPresent()) {
                destTier = originalTopology.get(destinationTierId.get());
            }
        }

        if (sourceMarketTier != null) {
            sourceRegion = getSourceRegionFromMarketTier(sourceMarketTier, originalCloudTopology, targetOid);
            // Soruce AZ or Region is the AZ or Region which the target is connected to.
            List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(target,
                Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                originalTopology);
            if (!connectedEntities.isEmpty())
                sourceAzOrRegion = connectedEntities.get(0);
            else
                logger.error("{} is not connected to any AZ or Region", target.getDisplayName());
            Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(move, target.getOid(), true);
            if (sourceTierId.isPresent()) {
                sourceTier = originalTopology.get(sourceTierId.get());
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

        Set<Long> resourceIds = shoppingListInfo.getResourceIds();
        if (CollectionUtils.isEmpty(resourceIds) && shoppingListInfo.getCollapsedBuyerId().isPresent()) {
            // For cloud->cloud move storage shopping list, use the collapsed buyer.
            resourceIds = Collections.singleton(shoppingListInfo.getCollapsedBuyerId().get());
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
            final boolean generateTierAction = (destTier != sourceTier);
            if (generateTierAction) {
                // Tier change provider
                changeProviders.add(createChangeProvider(sourceTier.getOid(),
                        destTier.getOid(), resourceIds, projectedTopology));
            }
        } else if (sourceMarketTier == null && destMarketTier != null) {
            // On prem to cloud move (with or without source)
            // AZ or Region change provider. We create an AZ or Region change provider
            // because the target is connected to AZ or Region.
            changeProviders.add(createChangeProvider(moveSource,
                destAzOrRegion.getOid(), null, projectedTopology));
            // Tier change provider
            changeProviders.add(createChangeProvider(moveSource,
                    destTier.getOid(), resourceIds, projectedTopology));
        } else if (sourceMarketTier != null && destMarketTier == null) {
            // Cloud to on prem move
            changeProviders.add(createChangeProvider(sourceTier.getOid(),
                    move.getDestination(), resourceIds, projectedTopology));
        } else {
            // On prem to on prem (with or without source)
            // A cloud container pod moving from a cloud VM to another cloud VM is covered by this case.
            changeProviders.add(createChangeProvider(moveSource,
                    move.getDestination(), resourceIds, projectedTopology));
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
        return Math.abs(d1 - d2) <= SMAUtils.BIG_EPSILON;
    }

    @Nonnull
    private ChangeProvider createChangeProvider(@Nullable final Long sourceId,
                            final long destinationId,
                            Set<Long> resourceIds,
                            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final ChangeProvider.Builder changeProviderBuilder = ChangeProvider.newBuilder()
            .setDestination(createActionEntity(destinationId, projectedTopology));
        if (sourceId != null) {
            changeProviderBuilder.setSource(createActionEntity(sourceId, projectedTopology));
        }
        if (resourceIds != null) {
            for (Long resourceId : resourceIds) {
                changeProviderBuilder.addResource(createActionEntity(resourceId, projectedTopology));
            }
        }
        return changeProviderBuilder.build();
    }

    private Explanation interpretExplanation(
            @Nonnull ActionTO actionTO, @Nonnull CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            final ActionType translatedActionType) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                if (translatedActionType == ActionType.SCALE) {
                    expBuilder.setScale(interpretScaleExplanation(actionTO, savings, projectedTopology));
                } else if (translatedActionType == ActionType.ALLOCATE){
                    expBuilder.setAllocate(interpretAllocateExplanation(actionTO));
                } else if (translatedActionType == ActionType.MOVE){
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
                        interpretDeactivateExplanation(actionTO.getDeactivate()));
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
     * Compute the actionType to translate the MoveTO.
     * The move action will be translated to scale if it is within the same region
     * but to different tier. The move action will be translated to allocate if it is an
     * RI optimisation action. If it is no-op action with minor ri coverage change we will covert it
     * to NONE. And if it is none of the above we keep the action as a MOVE action itself.
     *
     * @param actionTO actionTO to check.
     * @return the actiontype to interpret MoveTO to.
     */
    private ActionType findTranslatedActionType(@Nonnull ActionTO actionTO,
            @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final MoveTO moveTO = actionTO.getMove();
        if (moveTO == null) {
            return ActionType.MOVE;
        }

        MarketTier sourceMarketTier = moveTO.hasSource() ? cloudTc.getMarketTier(moveTO.getSource()) : null;
        MarketTier destMarketTier = moveTO.hasDestination() ? cloudTc.getMarketTier(moveTO.getDestination()) : null;
        if (sourceMarketTier == null || destMarketTier == null) {
            return ActionType.MOVE;
        }
        if (sourceMarketTier.getTier() != null
                && TRANSLATE_MOVE_TO_SCALE_PROVIDER_TYPE.contains(sourceMarketTier.getTier().getEntityType())
                && destMarketTier.getTier() != null
                && TRANSLATE_MOVE_TO_SCALE_PROVIDER_TYPE.contains(destMarketTier.getTier().getEntityType())) {
            return ActionType.SCALE;
        }

        if (sourceMarketTier.getTier() != null
                && sourceMarketTier.getTier().getEntityType() == EntityType.COMPUTE_TIER_VALUE
                && destMarketTier.getTier() != null
                && destMarketTier.getTier().getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            final ShoppingListInfo shoppingListInfo =
                    shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
            if (shoppingListInfo == null) {
                throw new IllegalStateException(
                        "Market returned invalid shopping list for MOVE: " + moveTO);
            }
            Long targetOid = shoppingListInfo.getBuyerId();
            TopologyEntityDTO sourceRegion = getSourceRegionFromMarketTier(sourceMarketTier,originalCloudTopology, targetOid);
            TopologyEntityDTO destinationRegion = getDestinationRegionFromMarketTier(destMarketTier, moveTO);
            if (sourceRegion != destinationRegion) {
                return ActionType.MOVE;
            }
            Optional<Long> destinationTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, targetOid, false);
            TopologyEntityDTO sourceTier = null;
            TopologyEntityDTO destTier = null;
            if (destinationTierId.isPresent()) {
                destTier = originalTopology.get(destinationTierId.get());
            }
            Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, targetOid, true);
            if (sourceTierId.isPresent()) {
                sourceTier = originalTopology.get(sourceTierId.get());
            }
            boolean isAccountingAction = isAccountingAction(destinationRegion,
                    sourceRegion,sourceMarketTier,moveTO,sourceTier,destTier,targetOid);
            if (isAccountingAction) {
                return ActionType.ALLOCATE;
            } else if(sourceTier != destTier){
                return ActionType.SCALE;
            } else {
                return ActionType.NONE;
            }
        }
        return ActionType.MOVE;
    }

    /**
     * Generate the explanation for allocate action
     * @param actionTO the M2 move action that is getting converted to allocate.
     * @return the explanation for the allocate action.
     */
    private AllocateExplanation interpretAllocateExplanation(@Nonnull ActionTO actionTO) {
        final MoveTO moveTO = actionTO.getMove();
        final ShoppingListInfo shoppingListInfo =
                shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingListInfo == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for MOVE: " + moveTO);
        }
        Long targetOid = shoppingListInfo.getBuyerId();
        TopologyEntityDTO sourceTier = null;
        Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, targetOid, true);
        if (sourceTierId.isPresent()) {
            sourceTier = originalTopology.get(sourceTierId.get());
        }
        return AllocateExplanation.newBuilder()
                .setInstanceSizeFamily(sourceTier.getTypeSpecificInfo()
                        .getComputeTier().getFamily())
                .build();
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

    /**
     * Generate explanation for Deactivate action.
     *
     * @param deactivateTO - deactivate actionTO
     *
     * @return the explanation.
     */
    private DeactivateExplanation interpretDeactivateExplanation(DeactivateTO deactivateTO) {
        List<CommodityType> reconfigCommTypes = getReconfigurableCommodities(deactivateTO.getTraderToDeactivate(), true);
        return reconfigCommTypes.isEmpty()
            ? DeactivateExplanation.getDefaultInstance()
            : DeactivateExplanation.newBuilder().addAllReconfigCommTypes(reconfigCommTypes).build();
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
                    .newBuilder().setBuyerId(shoppingList.getBuyerId())
                    .addAllCommodityNewCapacityEntry(capacityPerType)
                    .addAllCommodityMaxAmountAvailable(maxAmountPerType).build())
                    .addAllReconfigCommTypes(getReconfigurableCommodities(provisionByDemandTO.getProvisionedSeller(), false))
                    .build();
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
                .addAllReconfigCommTypes(getReconfigurableCommodities(provisionBySupply.getProvisionedSeller(), false))
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
                    if (overrideAndExplanation.getFirst()) {
                        // Need to override, see if there is an explanation available, e.g for vol.
                        if (overrideAndExplanation.getSecond() != null) {
                            changeProviderExplanation = ChangeProviderExplanation
                                    .newBuilder(overrideAndExplanation.getSecond());
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
                        .map(Pair::getFirst)
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
                changeProviderExplanation = ChangeProviderExplanation.newBuilder();
                if (moveExplanation.getEvacuation().getEvacuationExplanation().hasReconfigureRemoval()) {
                    changeProviderExplanation.setEvacuation(ChangeProviderExplanation.Evacuation
                        .newBuilder()
                            .setSuspendedEntity(moveExplanation.getEvacuation().getSuspendedTrader())
                            .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                                .newBuilder()
                                    .setReconfigureRemoval(ChangeProviderExplanation.ReconfigureRemoval.newBuilder().build())
                                .build())
                        .build());
                } else {
                    changeProviderExplanation.setEvacuation(ChangeProviderExplanation.Evacuation
                            .newBuilder()
                                .setSuspendedEntity(moveExplanation.getEvacuation().getSuspendedTrader())
                                .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                                    .newBuilder()
                                        .setSuspension(ChangeProviderExplanation.Suspension.newBuilder().build())
                                    .build())
                            .build());
                }
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
                            .setEvacuationExplanation(ChangeProviderExplanation.EvacuationExplanation
                                .newBuilder()
                                   .setSuspension(ChangeProviderExplanation.Suspension.newBuilder().build())
                               .build())
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

        // First, check if this entity has congested commodities pre-stored.
        Optional<ChangeProviderExplanation.Builder> congestedExplanation =
            getCongestedExplanationFromTracker(actionTargetId, sellerId);
        if (congestedExplanation.isPresent()) {
            return congestedExplanation;
        }

        // Second, check if this entity is cloud and has RI change.
        Optional<ChangeProviderExplanation.Builder> increaseRiUtilExplanation =
            getRIIncreaseExplanation(actionTargetId, moveTO);
        if (increaseRiUtilExplanation.isPresent()) {
            return increaseRiUtilExplanation;
        }

        // Find the commodities which have lower and higher capacity in the projected topology
        Pair<Set<CommodityType>, Set<CommodityType>> lowerAndHigherCapacityPair =
            calculateCommWithChangingProjectedCapacity(moveTO, actionTargetId, projectedTopology, sellerId);
        // Collection of commodities with lower projected capacity in projected topology.
        final Set<CommodityType> lowerProjectedCapacityComms = lowerAndHigherCapacityPair.getFirst();
        // Collection of commodities with higher projected capacity in projected topology.
        final Set<CommodityType> higherProjectedCapacityComms = lowerAndHigherCapacityPair.getSecond();

        // Third, check if there is an under-utilized commodity which became lower capacity
        // in the projected topology.
        Optional<ChangeProviderExplanation.Builder> underUtilizedExplanation =
            getUnderUtilizedExplanationFromTracker(actionTargetId, sellerId, lowerProjectedCapacityComms,
                higherProjectedCapacityComms, savings);
        if (underUtilizedExplanation.isPresent()) {
            return underUtilizedExplanation;
        }

        // Fourth, check if there is savings
        Optional<ChangeProviderExplanation.Builder> savingsExplanation =
            getExplanationFromSaving(savings, higherProjectedCapacityComms);
        if (savingsExplanation.isPresent()) {
            return savingsExplanation;
        }

        // Fifth, check if the action is a CSG action
        Optional<ChangeProviderExplanation.Builder> csgExplanation =
            getConistentScalingExplanationForCloud(moveTO);
        if (csgExplanation.isPresent()) {
            return csgExplanation;
        }

        // Sixth, check if we got a free scale up
        Optional<ChangeProviderExplanation.Builder> freeScaleUpExplanation =
            getFreeScaleUpExplanation(savings, higherProjectedCapacityComms);
        if (freeScaleUpExplanation.isPresent()) {
            return freeScaleUpExplanation;
        }

        // Seventh, check if we got an action with zero savings and the same projected capacities.
        // E.g. it happens when AWS IO1 volume is scaled to IO2. Both IO1 and IO2 provide the same
        // capacities and costs but IO2 is newer and has better durability.
        final Optional<ChangeProviderExplanation.Builder> zeroSavingsExplanation =
                getZeroSavingsAndNoCommodityChangeExplanation(moveTO, savings,
                        lowerProjectedCapacityComms, higherProjectedCapacityComms);
        if (zeroSavingsExplanation.isPresent()) {
            return zeroSavingsExplanation;
        }

        return getDefaultExplanationForCloud(actionTargetId, moveTO);
    }

    private Optional<ChangeProviderExplanation.Builder> getZeroSavingsAndNoCommodityChangeExplanation(
            @Nonnull final MoveTO moveTO,
            @Nonnull final CalculatedSavings savings,
            @Nonnull final Set<CommodityType> lowerProjectedCapacityComms,
            @Nonnull final Set<CommodityType> higherProjectedCapacityComms) {
        // Check if this is a Cloud Move action with zero savings and no commodity changes
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())
                && savings.savingsAmount.getValue() == 0
                && lowerProjectedCapacityComms.isEmpty()
                && higherProjectedCapacityComms.isEmpty()) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.newBuilder()));
        }
        return Optional.empty();
    }

    private Optional<ChangeProviderExplanation.Builder> getFreeScaleUpExplanation(
        CalculatedSavings savings, Set<CommodityType> higherProjectedCapacityComms) {
        // Check if we got a scale up for free. This can happen when the savings is 0 or greater,
        // and there are some commodities which have higher projected capacities.
        if (savings.savingsAmount.getValue() >= 0 && !higherProjectedCapacityComms.isEmpty()) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                Efficiency.newBuilder().addAllScaleUpCommodity(higherProjectedCapacityComms)));
        }
        return Optional.empty();
    }

    private Optional<ChangeProviderExplanation.Builder> getConistentScalingExplanationForCloud(MoveTO moveTO) {
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        TopologyEntityDTO originalTrader = slInfo != null ? originalTopology.get(slInfo.getBuyerId()) : null;
        if (moveTO.hasScalingGroupId() && originalTrader != null
                && originalTrader.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setCompliance(
                Compliance.newBuilder().setIsCsgCompliance(true)));
        }
        return Optional.empty();
    }

    private Optional<ChangeProviderExplanation.Builder> getCongestedExplanationFromTracker(long actionTargetId, long sellerId) {
        Set<CommodityTypeWithLookup> congestedCommodityTypes = commoditiesResizeTracker.getCongestedCommodityTypes(actionTargetId, sellerId);
        if (congestedCommodityTypes != null && !congestedCommodityTypes.isEmpty()) {
            Congestion.Builder congestionBuilder = ChangeProviderExplanation.Congestion.newBuilder()
                    .addAllCongestedCommodities(commTypes2ReasonCommodities(congestedCommodityTypes
                        .stream().map(CommodityTypeWithLookup::commodityType).collect(Collectors.toSet())));
            logger.debug("CongestedCommodities from tracker for buyer: {}, seller: {} : [{}]",
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
            @Nonnull Set<CommodityType> lowerProjectedCapacityComms,
            @Nonnull Set<CommodityType> higherProjectedCapacityComms,
            CalculatedSavings savings) {
        if (!lowerProjectedCapacityComms.isEmpty()) {
            Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder()
                    .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(lowerProjectedCapacityComms));
            logger.debug("Underutilized Commodities from tracker for buyer:{}, seller: {} : [{}]",
                    actionTargetId, sellerId,
                lowerProjectedCapacityComms.stream().map(AbstractMessage::toString).collect(Collectors.joining()));
            if (savings.savingsAmount.getValue() >= 0) {
                // Check if we got a scale up for free. This can happen when the savings is 0 or greater,
                // and there are some commodities which have higher projected capacities.
                efficiencyBuilder
                    .addAllScaleUpCommodity(higherProjectedCapacityComms);
            }
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
        }
        return Optional.empty();
    }

    /**
     * Commodities with either lower projected capacity values or higher projected values compared to original topology.
     *
     * @param moveTO the moveTO from m2
     * @param actionTargetId   current action.
     * @param projectedTopology    projected topology indexed by id.
     * @param sellerId the seller which supplied the shopping list before anlaysis took place
     * @return Pair set of lower projected commodities and higher projected commodity.
     */
    @Nonnull
    private Pair<Set<CommodityType>, Set<CommodityType>> calculateCommWithChangingProjectedCapacity(
            final MoveTO moveTO,
            final long actionTargetId,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            long sellerId) {
        Set<CommodityTypeWithLookup> underutilizedCommTypesWithLookup = commoditiesResizeTracker
            .getUnderutilizedCommodityTypes(actionTargetId, sellerId);
        final Set<CommodityType> lowerProjectedCapacity = Sets.newHashSet();
        final Set<CommodityType> higherProjectedCapacity = Sets.newHashSet();
        if (!underutilizedCommTypesWithLookup.isEmpty()) {
            final Map<CommodityType, CommoditySoldDTO> originalCommoditySoldBySource = Maps.newHashMap();
            final Map<CommodityType, CommoditySoldDTO> originalCommoditySoldByTarget = Maps.newHashMap();
            Map<CommodityType, CommoditySoldDTO> projectedCommoditySoldByDestination = Maps.newHashMap();
            Map<CommodityType, CommoditySoldDTO> projectedCommoditySoldByTarget = Maps.newHashMap();
            // Segregate under-utilized commodities by lookup types - consumer lookup and provider lookup.
            // Some commodities like VMEM/VCPU are sold by the consumer. The projected capacity needs
            // to be compared with original capacity at the consumer.
            // Some commodities like IOThroughput/NetThroughput are bought by the consumer, and sold
            // by the provider. The projected capacity needs to be compared with original capacity
            // at the provider for these commodities.
            Map<CommodityLookupType, List<CommodityTypeWithLookup>> underUtilizedCommoditiesByLookup =
                underutilizedCommTypesWithLookup.stream().collect(Collectors.groupingBy(CommodityTypeWithLookup::lookupType));
            List<CommodityType> underUtilCommsWithConsumerLookup = underUtilizedCommoditiesByLookup.containsKey(CommodityLookupType.CONSUMER)
                ? underUtilizedCommoditiesByLookup.get(CommodityLookupType.CONSUMER).stream()
                .map(CommodityTypeWithLookup::commodityType).collect(Collectors.toList())
                : Collections.emptyList();
            List<CommodityType> underUtilizedCommsWithProviderLookup = underUtilizedCommoditiesByLookup.containsKey(CommodityLookupType.PROVIDER)
                ? underUtilizedCommoditiesByLookup.get(CommodityLookupType.PROVIDER).stream()
                .map(CommodityTypeWithLookup::commodityType).collect(Collectors.toList())
                : Collections.emptyList();

            // populate projectedCommoditySoldByTarget and originalCommoditySoldByTarget
            if (!underUtilCommsWithConsumerLookup.isEmpty()) {
                projectedCommoditySoldByTarget = projectedTopology.containsKey(actionTargetId)
                    ? projectedTopology.get(actionTargetId).getEntity()
                    .getCommoditySoldListList().stream()
                    .filter(commoditySoldDTO -> underUtilCommsWithConsumerLookup.contains(commoditySoldDTO.getCommodityType()))
                    .collect(Collectors.toMap(CommoditySoldDTO::getCommodityType, Function.identity()))
                    : Collections.emptyMap();
                underUtilCommsWithConsumerLookup.forEach(comm -> commodityIndex.getCommSold(actionTargetId, comm)
                    .ifPresent(cSold -> originalCommoditySoldByTarget.put(comm, cSold)));
            }

            // populate projectedCommoditySoldByDestination and originalCommoditySoldBySource
            if (!underUtilizedCommsWithProviderLookup.isEmpty()) {
                long sourceId = moveTO.getSource();
                if (cloudTc.isMarketTier(sourceId)) {
                    Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, actionTargetId, true);
                    if (sourceTierId.isPresent()) {
                        sourceId = sourceTierId.get();
                        populateCommoditiesSoldByCommodityTypeFromCommodityIndex(sourceId, underUtilizedCommsWithProviderLookup, originalCommoditySoldBySource);
                    }
                } else {
                    populateCommoditiesSoldByCommodityTypeFromCommodityIndex(sourceId, underUtilizedCommsWithProviderLookup, originalCommoditySoldBySource);
                }

                long destinationId = moveTO.getDestination();
                if (cloudTc.isMarketTier(destinationId)) {
                    Optional<Long> destinationTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, actionTargetId, false);
                    if (destinationTierId.isPresent()) {
                        destinationId = destinationTierId.get();
                        populateCommoditiesSoldByCommodityTypeFromCommodityIndex(destinationId, underUtilizedCommsWithProviderLookup, projectedCommoditySoldByDestination);
                    }
                } else {
                    if (projectedTopology.get(destinationId) != null) {
                        projectedCommoditySoldByDestination =
                            projectedTopology.get(destinationId).getEntity()
                                .getCommoditySoldListList().stream()
                                .filter(commoditySoldDTO -> underUtilizedCommsWithProviderLookup.contains(commoditySoldDTO.getCommodityType()))
                                .collect(Collectors.toMap(CommoditySoldDTO::getCommodityType, Function.identity()));
                    }
                }
            }

            // Loop through the under utilized commodities, and compare the capacity of the commodity
            // sold of the original with projected.
            for (CommodityTypeWithLookup underutilizedComm : underutilizedCommTypesWithLookup) {
                if (underutilizedComm.lookupType() == CommodityLookupType.CONSUMER) {
                    if (projectedCommoditySoldByTarget.containsKey(underutilizedComm.commodityType())
                        && originalCommoditySoldByTarget.containsKey(underutilizedComm.commodityType())) {
                        double projectedCapacity = projectedCommoditySoldByTarget.get(underutilizedComm.commodityType()).getCapacity();
                        double originalCapacity = originalCommoditySoldByTarget.get(underutilizedComm.commodityType()).getCapacity();
                        if (projectedCapacity < originalCapacity) {
                            lowerProjectedCapacity.add(underutilizedComm.commodityType());
                        } else if (projectedCapacity > originalCapacity) {
                            higherProjectedCapacity.add(underutilizedComm.commodityType());
                        }
                    }
                } else if (underutilizedComm.lookupType() == CommodityLookupType.PROVIDER) {
                    if (projectedCommoditySoldByDestination.containsKey(underutilizedComm.commodityType())
                        && originalCommoditySoldBySource.containsKey(underutilizedComm.commodityType())) {
                        double projectedCapacity = projectedCommoditySoldByDestination.get(underutilizedComm.commodityType()).getCapacity();
                        double originalCapacity = originalCommoditySoldBySource.get(underutilizedComm.commodityType()).getCapacity();
                        if (projectedCapacity < originalCapacity) {
                            lowerProjectedCapacity.add(underutilizedComm.commodityType());
                        } else if (projectedCapacity > originalCapacity) {
                            higherProjectedCapacity.add(underutilizedComm.commodityType());
                        }
                    }
                }
            }
        }
        return new Pair<>(lowerProjectedCapacity, higherProjectedCapacity);
    }

    private void populateCommoditiesSoldByCommodityTypeFromCommodityIndex(long entityId, List<CommodityType> commodityTypeSoldFilter,
                                                                          Map<CommodityType, CommoditySoldDTO> mapToPopulate) {
        for (CommodityType commType : commodityTypeSoldFilter) {
            commodityIndex.getCommSold(entityId, commType)
                .ifPresent(cSold -> mapToPopulate.put(commType, cSold));
        }
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

    private Optional<ChangeProviderExplanation.Builder> getExplanationFromSaving(
        @Nonnull final CalculatedSavings savings,
        @Nonnull Set<CommodityType> higherProjectedCapacityComms) {
        if (savings.savingsAmount.getValue() > 0) {
            Efficiency.Builder efficiencyBuilder = Efficiency.newBuilder().setIsWastedCost(true)
                .addAllScaleUpCommodity(higherProjectedCapacityComms);
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(efficiencyBuilder));
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
        double projectedCouponsCapacity = 0;
        double originalCouponsCapacity = 0;
        double originalCoveragePercentage = 0;
        double projectedCoveragePercentage = 0;
        EntityReservedInstanceCoverage projectedCoverage = projectedRICoverageCalculator
            .getProjectedRICoverageForEntity(entityId);
        if (projectedCoverage != null) {
            projectedCouponsCapacity = projectedCoverage.getEntityCouponCapacity();
            if (!projectedCoverage.getCouponsCoveredByRiMap().isEmpty()) {
                projectedCouponsCovered = projectedCoverage.getCouponsCoveredByRiMap().values()
                    .stream().reduce(0.0, Double::sum);
            }
        }
        if (projectedCouponsCapacity > 0) {
            projectedCoveragePercentage = projectedCouponsCovered / projectedCouponsCapacity;
        }
        Optional<EntityReservedInstanceCoverage> originalCoverage = cloudTc.getRiCoverageForEntity(entityId);
        if (originalCoverage.isPresent()) {
            originalCouponsCapacity = originalCoverage.get().getEntityCouponCapacity();
            if (!originalCoverage.get().getCouponsCoveredByRiMap().isEmpty()) {
                originalCouponsCovered = originalCoverage.get()
                    .getCouponsCoveredByRiMap().values().stream().reduce(0.0, Double::sum);
            }
        }
        if (originalCouponsCapacity > 0) {
            originalCoveragePercentage = originalCouponsCovered / originalCouponsCapacity;
        }
        return projectedCouponsCovered > originalCouponsCovered
                // check if the coverage change is over 1%.
                && projectedCoveragePercentage - originalCoveragePercentage >= TopologyDTOUtil.ONE_PERCENT;
    }

    private ActionEntity createActionTargetEntity(@Nonnull ShoppingListInfo shoppingListInfo,
                                                  @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long targetEntityId = shoppingListInfo.getCollapsedBuyerId().orElse(shoppingListInfo.getBuyerId());
        return createActionEntity(targetEntityId, projectedTopology);
    }

    ActionEntity createActionEntity(final long id,
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
            final CommodityType commType = ct.getFirst();
            final Optional<Integer> slot = ct.getSecond();
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

    /**
     * Collects the reconfigurable commodity types of the trader.
     *
     * @param oid - the oid of the trader.
     * @param forSuspension - true for suspension and false for provision.
     *
     * @return List of the reconfigurable commodity types.
     */
    private List<CommodityType> getReconfigurableCommodities(long oid, boolean forSuspension) {
        TopologyEntityDTO originalTrader = originalTopology.get(oid);
        EconomyDTOs.TraderTO projectedTrader = oidToProjectedTraderTOMap.get(oid);
        if (forSuspension && originalTrader != null) {
            return originalTrader.getCommoditySoldListList().stream()
                .filter(commSold -> MarketAnalysisUtils.RECONFIGURABLE_COMMODITY_TYPES
                    .contains(commSold.getCommodityType().getType()))
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toList());
        } else if (!forSuspension && projectedTrader != null) {
            return projectedTrader.getCommoditiesSoldList().stream()
                .filter(commSold ->  MarketAnalysisUtils.RECONFIGURABLE_COMMODITY_TYPES
                    .contains(commSold.getSpecification().getBaseType()))
                .map(commSold -> {
                    return commodityConverter.marketToTopologyCommodity(commSold.getSpecification())
                        .orElseGet(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        } else {
            return Lists.newArrayList();
        }
    }
}
