package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.TopologyConversionUtils.calculateFactorForCommodityValues;

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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;
import org.springframework.util.CollectionUtils;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.Suffix;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.TimeSlotReasonInformation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.SingleRegionMarketTier;
import com.vmturbo.market.topology.conversions.action.ResizeInterpreter;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
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
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

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
            ImmutableSet.of(EntityType.STORAGE_TIER_VALUE,
                    EntityType.DATABASE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
                    EntityType.COMPUTE_TIER_VALUE);
    private final CommodityIndex commodityIndex;
    private final Map<Long, AtomicInteger> provisionActionTracker = new HashMap<>();

    /**
     * Whether compliance action explanation needs to be overridden with perf/efficiency, needed
     * in certain cases like cloud migration.
     */
    private final BiFunction<MoveTO, Map<Long, ProjectedTopologyEntity>,
            Pair<Boolean, ChangeProviderExplanation>> complianceExplanationOverride;

    private final ResizeInterpreter resizeInterpreter;
    private final FakeEntityCreator fakeEntityCreator;

    /**
     * This map stores all the VMs whose CPU/Mem reservation are larger than real usage. We'll add a "reservation" suffix in their action descriptions.
     */
    private final Map<Long, Map<CommodityBoughtDTO, Long>> commoditiesWithReservationGreaterThanUsed;

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
                              Pair<Boolean, ChangeProviderExplanation>> explanationFunction,
                      @Nonnull final Map<Long, Map<CommodityBoughtDTO, Long>> commoditiesWithReservationGreaterThanUsed,
                      @Nonnull final FakeEntityCreator fakeEntityCreator) {
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.cloudTc = cloudTc;
        this.originalTopology = originalTopology;
        this.oidToProjectedTraderTOMap = oidToTraderTOMap;
        this.commoditiesResizeTracker = commoditiesResizeTracker;
        this.fakeEntityCreator = fakeEntityCreator;
        this.projectedRICoverageCalculator = projectedRICoverageCalculator;
        this.tierExcluder = tierExcluder;
        this.commodityIndex = commodityIndexSupplier.get();
        this.complianceExplanationOverride = explanationFunction;
        this.resizeInterpreter = new ResizeInterpreter(commodityConverter, commodityIndex, oidToProjectedTraderTOMap, originalTopology);
        this.commoditiesWithReservationGreaterThanUsed = commoditiesWithReservationGreaterThanUsed;
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
     * @return The {@link Action} describing the recommendation in a topology-specific way.
     */
    @Nonnull
    List<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                 @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                 @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                 @Nonnull CloudActionSavingsCalculator actionSavingsCalculator) {
        final List<ActionData> actionList = new ArrayList<>();
        try {
            final Action.Builder action;
            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    action = createAction(actionTO);
                    final ActionType translatedActionType = findTranslatedActionType(actionTO, originalCloudTopology);
                    if (translatedActionType == ActionType.SCALE) {
                        action.getInfoBuilder().setScale(interpretScaleAction(actionTO.getMove(),
                                projectedTopology, originalCloudTopology));
                        actionList.add(ActionData.of(actionTO, action));
                        break;
                    } else if (translatedActionType == ActionType.ALLOCATE) {
                        action.getInfoBuilder().setAllocate(interpretAllocateAction(
                                actionTO.getMove(),
                                projectedTopology));
                        actionList.add(ActionData.of(actionTO, action));
                        break;
                    } else if (translatedActionType == ActionType.MOVE) {
                        Optional<ActionDTO.Move> move = interpretMoveAction(actionTO.getMove(),
                                projectedTopology, originalCloudTopology);
                        if (move.isPresent()) {
                            action.getInfoBuilder().setMove(move.get());
                            actionList.add(ActionData.of(actionTO, action));
                        }
                        break;
                    }
                    break;
                case COMPOUND_MOVE:
                    if (isSplitCompoundMove(actionTO)) {
                        actionList.addAll(splitCompoundMove(actionTO, projectedTopology,
                                originalCloudTopology));
                    } else {
                        action = createAction(actionTO);
                        Optional<ActionDTO.Move> compoundMoveOpt = interpretCompoundMoveAction(actionTO.getCompoundMove(),
                                projectedTopology, originalCloudTopology);
                        compoundMoveOpt.ifPresent(compoundMove -> {
                            action.getInfoBuilder().setMove(compoundMove);
                            actionList.add(ActionData.of(actionTO, action));
                        });
                    }
                    break;
                case RECONFIGURE:
                    action = createAction(actionTO);
                    action.getInfoBuilder().setReconfigure(interpretReconfigureAction(
                            actionTO.getReconfigure(), projectedTopology, originalCloudTopology));
                    actionList.add(ActionData.of(actionTO, action));
                    break;
                case PROVISION_BY_SUPPLY:
                    action = createAction(actionTO);
                    action.getInfoBuilder().setProvision(interpretProvisionBySupply(
                            actionTO.getProvisionBySupply(), projectedTopology));
                    actionList.add(ActionData.of(actionTO, action));
                    break;
                case PROVISION_BY_DEMAND:
                    action = createAction(actionTO);
                    action.getInfoBuilder().setProvision(interpretProvisionByDemand(
                            actionTO.getProvisionByDemand(), projectedTopology));
                    actionList.add(ActionData.of(actionTO, action));
                    break;
                case RESIZE:
                    if (isFakeClusterEntity(actionTO.getResize().getSellingTrader())) {
                        break;
                    }
                    action = createAction(actionTO);
                    Optional<ActionDTO.Resize> resize = resizeInterpreter.interpret(
                            actionTO.getResize(), projectedTopology);
                    if (resize.isPresent()) {
                        action.getInfoBuilder().setResize(resize.get());
                        actionList.add(ActionData.of(actionTO, action));
                    }
                    break;
                case ACTIVATE:
                    action = createAction(actionTO);
                    action.getInfoBuilder().setActivate(interpretActivate(
                            actionTO.getActivate(), projectedTopology));
                    actionList.add(ActionData.of(actionTO, action));
                    break;
                case DEACTIVATE:
                    if (isFakeClusterEntity(actionTO.getDeactivate().getTraderToDeactivate())) {
                        break;
                    }
                    action = createAction(actionTO);
                    action.getInfoBuilder().setDeactivate(interpretDeactivate(
                            actionTO.getDeactivate(), projectedTopology));
                    actionList.add(ActionData.of(actionTO, action));
                    break;
            }

            // after creating the set of actions from the actionTO, add action savings and an explanation,
            // which may be based on the action savings
            return actionList.stream()
                    .peek(actionData -> addSavingsAndExplanation(
                            actionData, actionSavingsCalculator, projectedTopology))
                    .map(ActionData::actionBuilder)
                    .map(Action.Builder::build)
                    .collect(ImmutableList.toImmutableList());


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

            return Collections.EMPTY_LIST;
        }
    }

    private void addSavingsAndExplanation(@Nonnull ActionData actionData,
                                          @Nonnull CloudActionSavingsCalculator actionSavingsCalculator,
                                          @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {

        final CalculatedSavings savings = actionSavingsCalculator.calculateSavings(actionData.actionBuilder());
        savings.applyToActionBuilder(actionData.actionBuilder());

        actionData.actionBuilder()
                .setExplanation(interpretExplanation(actionData, savings, projectedTopology));
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
     * @return action builder
     */
    Action.Builder createAction(@Nonnull final ActionTO actionTO) {

        // The action importance should never be infinite, as setImportance() will fail.
        final Action.Builder action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(actionTO.hasId() ? actionTO.getId() : IdentityGenerator.next())
                .setDeprecatedImportance(actionTO.getImportance())
                .setExecutable(!actionTO.getIsNotExecutable());

        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        action.setInfo(infoBuilder);
        return action;
    }

    private List<ActionData> splitCompoundMove(@Nonnull final ActionTO actionTO,
                                               @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                               @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final List<ActionData> actionList = new ArrayList<>();

        List<MoveTO> moveActions = actionTO.getCompoundMove().getMovesList();
        for (MoveTO moveTO : moveActions) {
            // done so that we don't make actions for moves that involve clusters,
            // during ignore constraints plan analysis
            if (isFakeClusterMove(moveTO)) {
                continue;
            }
            // Create ActionTO from MoveTO.
            final ActionTO.Builder actionBuilder = ActionTO.newBuilder();
            actionBuilder.setIsNotExecutable(actionTO.getIsNotExecutable());
            actionBuilder.setMove(moveTO);
            actionBuilder.setImportance(actionTO.getImportance());

            final ActionTO moveActionTO = actionBuilder.build();

            Action.Builder moveActionBuilder = createAction(moveActionTO);
            Optional<ActionDTO.Move> moveAction = interpretMoveAction(
                    actionBuilder.getMove(), projectedTopology, originalCloudTopology);
            if (moveAction.isPresent()) {
                moveActionBuilder.getInfoBuilder().setMove(moveAction.get());
                actionList.add(ActionData.of(moveActionTO, moveActionBuilder));
            }
        }
        return actionList;
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
    @VisibleForTesting
    Optional<ActionDTO.Move> interpretCompoundMoveAction(
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

        List<ChangeProvider> changeProviders = moves.stream()
                .map(move -> createChangeProviders(move, projectedTopology, originalCloudTopology, targetOid))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        if (changeProviders.isEmpty()) {
            logger.warn("Analysis suggested a compound move which translated to an XL move " +
                    "without any change providers. CompoundMoveTO: {}", compoundMoveTO);
            return Optional.empty();
        }
        return Optional.of(ActionDTO.Move.newBuilder()
            .setTarget(createActionEntity(
                targetIds.iterator().next(), projectedTopology))
            .addAllChanges(changeProviders)
                .build());
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
            } else {
                logger.warn("Analysis suggested a move which translated to an XL move " +
                        "without any change providers. MoveTO: {}", moveTO);
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
     * @return {@link ActionDTO.Allocate} representing the Allocate action
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
            if (!connectedEntities.isEmpty()) {
                sourceAzOrRegion = connectedEntities.get(0);
            } else {
                logger.error("{} is not connected to any AZ or Region", target.getDisplayName());
            }
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
            if (ObjectUtils.allNotNull(destinationRegion, sourceRegion, destAzOrRegion, sourceAzOrRegion)
                    && destinationRegion != sourceRegion) {
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
            if (!shouldSkipChangeProvider(move, target, resourceIds, projectedTopology)) {
                // On prem to on prem (with or without source)
                // A cloud container pod moving from a cloud VM to another cloud VM is covered by this case.
                changeProviders.add(createChangeProvider(moveSource,
                    move.getDestination(), resourceIds, projectedTopology));
            } else {
                logger.trace("Skip move action {}", move);
            }
        }
        return changeProviders;
    }

    /**
     * Determine if change provider creation should be skip for given move action.
     * Skips if we are trying to move a virtual volume, then skip change provider creation,
     * or if the move involves a fake cluster
     * @param move a move action
     * @param target target of move action
     * @param resourceIds related resource ids
     * @param projectedTopology projected topology
     * @return should skip change provider creation or not
     */
    private boolean shouldSkipChangeProvider(
            @Nonnull final MoveTO move, @Nullable final TopologyEntityDTO target,
            @Nullable final Set<Long> resourceIds, @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        // If we are trying to move a virtual volume, then skip change provider creation, which means this action will be discarded.
        // TODO: We need a better way to determine configuration volume.
        return isFakeClusterMove(move) || (target != null && target.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
            && projectedTopology.get(move.getDestination()).getEntity().getEntityType() == EntityType.STORAGE_VALUE
            && resourceIds != null && !resourceIds.isEmpty()
            && TopologyDTOUtil.isConfigurationVolume(originalTopology.get(resourceIds.iterator().next())));
    }

    /**
     * If it is a fake cluster than we have no reason to compute its change provider
     * @param move
     * @return if the move is involved with a fake cluster
     */
    private boolean isFakeClusterMove(MoveTO move) {
        return isFakeClusterEntity(move.getDestination())
                || isFakeClusterEntity(move.getSource());
    }

    /**
     * check if the entity is a fake cluster
     * @param entityId
     * @return if the entity is a fake cluster
     */
    private boolean isFakeClusterEntity(long entityId) {
        return this.fakeEntityCreator.isFakeComputeClusterOid(entityId);
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
            @Nonnull ActionData actionData,
            @Nonnull CalculatedSavings savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        final ActionTO actionTO = actionData.actionTO();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                if (actionData.actionBuilder().getInfoBuilder().hasScale()) {
                    expBuilder.setScale(interpretScaleExplanation(
                            actionTO, savings, projectedTopology));
                } else if (actionData.actionBuilder().getInfoBuilder().hasAllocate()){
                    expBuilder.setAllocate(interpretAllocateExplanation(actionTO));
                } else if (actionData.actionBuilder().getInfoBuilder().hasMove()){
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
                expBuilder.setResize(resizeInterpreter.interpretExplanation(actionTO.getResize()));
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

            if (sourceMarketTier.getTier().getEntityType() == EntityType.COMPUTE_TIER_VALUE
                    && destMarketTier.getTier().getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                final ShoppingListInfo shoppingListInfo = shoppingListOidToInfos
                        .get(moveTO.getShoppingListToMove());
                if (shoppingListInfo == null) {
                    throw new IllegalStateException(
                            "Market returned invalid shopping list for MOVE: " + moveTO);
                }
                Long targetOid = shoppingListInfo.getBuyerId();
                TopologyEntityDTO sourceRegion = getSourceRegionFromMarketTier(sourceMarketTier,
                        originalCloudTopology, targetOid);
                TopologyEntityDTO destinationRegion = getDestinationRegionFromMarketTier(
                        destMarketTier, moveTO);
                if (sourceRegion != destinationRegion) {
                    return ActionType.MOVE;
                }
                Optional<Long> destinationTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(
                        moveTO, targetOid, false);
                TopologyEntityDTO sourceTier = null;
                TopologyEntityDTO destTier = null;
                if (destinationTierId.isPresent()) {
                    destTier = originalTopology.get(destinationTierId.get());
                }
                Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO,
                        targetOid, true);
                if (sourceTierId.isPresent()) {
                    sourceTier = originalTopology.get(sourceTierId.get());
                }
                if (sourceTier == destTier) {
                    return ActionType.ALLOCATE;
                } else {
                    return ActionType.SCALE;
                }
            } else {
                return ActionType.SCALE;
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
        DeactivateExplanation.Builder explanationBuilder = DeactivateExplanation.newBuilder();
        List<CommodityType> reconfigCommTypes = getReconfigurableCommodities(deactivateTO.getTraderToDeactivate(), true);
        if (!reconfigCommTypes.isEmpty()) {
            explanationBuilder.addAllReconfigCommTypes(reconfigCommTypes);
        }
        if (deactivateTO.hasReasonEntity()) {
            explanationBuilder.setReasonEntity(deactivateTO.getReasonEntity());
        }
        return explanationBuilder.build();
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
        ProvisionExplanation.Builder provisionExpBuilder = ProvisionExplanation.newBuilder()
                .setProvisionBySupplyExplanation(
                        ProvisionBySupplyExplanation.newBuilder()
                                .setMostExpensiveCommodityInfo(
                                        ReasonCommodity.newBuilder().setCommodityType(commType).build())
                                .build())
                .addAllReconfigCommTypes(getReconfigurableCommodities(provisionBySupply.getProvisionedSeller(), false));

        if (provisionBySupply.hasReasonEntity()) {
            provisionExpBuilder.setReasonEntity(provisionBySupply.getReasonEntity());
        }
        return provisionExpBuilder.build();
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
                changeExplanation(actionTO, moveTO, CalculatedSavings.NO_SAVINGS_USD, projectedTopology,
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
        ChangeExplainer changeExplainer = ChangeExplainerFactory.createChangeExplainer(
                projectedTopology.containsKey(actionTargetId) ? projectedTopology.get(actionTargetId).getEntity() : null,
                commoditiesResizeTracker, cloudTc, projectedRICoverageCalculator, shoppingListOidToInfos,
                commodityIndex, originalTopology);
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
                            changeProviderExplanation = changeExplainer.changeExplanationFromTracker(moveTO, savings, projectedTopology)
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
                Optional<ChangeProviderExplanation.Builder> explanationFromTracker = changeExplainer.changeExplanationFromTracker(moveTO, savings, projectedTopology);
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
                changeProviderExplanation = changeExplainer.changeExplanationFromTracker(moveTO, savings, projectedTopology)
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
            Map<CommodityBoughtDTO, Long> commodityWithReservationGreaterThanUsed = commoditiesWithReservationGreaterThanUsed.get(buyer);
            if (commodityWithReservationGreaterThanUsed != null) {
                commodityWithReservationGreaterThanUsed.entrySet().stream().filter(entry ->
                        entry.getKey().getCommodityType().equals(commType) &&
                        entry.getValue().equals(source)).findAny().ifPresent(
                        entry -> builder.setSuffix(Suffix.RESERVATION)
                );
            }
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
     * Given a VM and topology, return the CPU threads of its projected host.
     *
     * @param topology topologies
     * @param entity the entity resides on PM
     * @return The CPU threads of PM that hosts the entity, if present
     */
    public static Optional<Integer> getCPUThreadsFromPM(
                    @Nonnull final Function<Long, TopologyEntityDTO> topology,
                    @Nonnull final TopologyEntityDTO entity) {
        final Optional<Integer> cpuThreadsOfHost = entity.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(comm -> comm.hasProviderEntityType()
                                        && comm.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                        .map(comm -> topology.apply(comm.getProviderId()))
                        .filter(Objects::nonNull)
                        .filter(TopologyEntityDTO::hasTypeSpecificInfo)
                        .map(TopologyEntityDTO::getTypeSpecificInfo)
                        .filter(TypeSpecificInfo::hasPhysicalMachine)
                        .map(TypeSpecificInfo::getPhysicalMachine)
                        .filter(PhysicalMachineInfo::hasNumCpuThreads)
                        .map(PhysicalMachineInfo::getNumCpuThreads)
                        .findFirst();
        return cpuThreadsOfHost;
    }

    @HiddenImmutableTupleImplementation
    @Immutable
    interface ActionData {

        ActionTO actionTO();

        Action.Builder actionBuilder();

        static ActionData of(@Nonnull ActionTO actionTO, Action.Builder actionBuilder) {
            return ActionDataTuple.of(actionTO, actionBuilder);
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
