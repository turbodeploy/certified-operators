package com.vmturbo.market.topology.conversions;

import static com.vmturbo.trax.Trax.trax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Function;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

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
import com.vmturbo.market.topology.conversions.CloudEntityResizeTracker.CommodityUsageType;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
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
    private final CloudEntityResizeTracker cert;
    private final TierExcluder tierExcluder;
    private final ProjectedRICoverageCalculator projectedRICoverageCalculator;
    private static final Set<EntityState> evacuationEntityState =
        EnumSet.of(EntityState.MAINTENANCE, EntityState.FAILOVER);
    private final CommodityIndex commodityIndex;

    ActionInterpreter(@Nonnull final CommodityConverter commodityConverter,
                      @Nonnull final Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                      @Nonnull final CloudTopologyConverter cloudTc, Map<Long, TopologyEntityDTO> originalTopology,
                      @Nonnull final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap, CloudEntityResizeTracker cert,
                      @Nonnull final ProjectedRICoverageCalculator projectedRICoverageCalculator,
                      @Nonnull final TierExcluder tierExcluder,
                      @Nonnull final CommodityIndex commodityIndex) {
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.cloudTc = cloudTc;
        this.originalTopology = originalTopology;
        this.oidToProjectedTraderTOMap = oidToTraderTOMap;
        this.cert = cert;
        this.projectedRICoverageCalculator = projectedRICoverageCalculator;
        this.tierExcluder = tierExcluder;
        this.commodityIndex = commodityIndex;
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
    Optional<Action> interpretAction(@Nonnull final ActionTO actionTO,
                                     @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                     @Nonnull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                     @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                     @Nonnull TopologyCostCalculator topologyCostCalculator) {
        try {
            final CalculatedSavings savings;
            final Action.Builder action;

            try (TraxContext traxContext = Trax.track("SAVINGS", actionTO.getActionTypeCase().name())) {
                savings = calculateActionSavings(actionTO,
                    originalCloudTopology, projectedCosts, topologyCostCalculator);

                // The action importance should never be infinite, as setImportance() will fail.
                action = Action.newBuilder()
                        // Assign a unique ID to each generated action.
                        .setId(IdentityGenerator.next())
                        .setDeprecatedImportance(actionTO.getImportance())
                        .setExplanation(interpretExplanation(actionTO, savings, projectedTopology))
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

            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    Optional<ActionDTO.Move> move = interpretMoveAction(
                            actionTO.getMove(), projectedTopology, originalCloudTopology);
                    if (move.isPresent()) {
                        infoBuilder.setMove(move.get());
                    } else {
                        return Optional.empty();
                    }
                    break;
                case COMPOUND_MOVE:
                    infoBuilder.setMove(interpretCompoundMoveAction(actionTO.getCompoundMove(),
                            projectedTopology, originalCloudTopology));
                    break;
                case RECONFIGURE:
                    infoBuilder.setReconfigure(interpretReconfigureAction(
                            actionTO.getReconfigure(), projectedTopology, originalCloudTopology));
                    break;
                case PROVISION_BY_SUPPLY:
                    infoBuilder.setProvision(interpretProvisionBySupply(
                            actionTO.getProvisionBySupply(), projectedTopology));
                    break;
                case PROVISION_BY_DEMAND:
                    infoBuilder.setProvision(interpretProvisionByDemand(
                            actionTO.getProvisionByDemand(), projectedTopology));
                    break;
                case RESIZE:
                    Optional<ActionDTO.Resize> resize = interpretResize(
                            actionTO.getResize(), projectedTopology);
                    if (resize.isPresent()) {
                        infoBuilder.setResize(resize.get());
                    } else {
                        return Optional.empty();
                    }
                    break;
                case ACTIVATE:
                    infoBuilder.setActivate(interpretActivate(
                            actionTO.getActivate(), projectedTopology));
                    break;
                case DEACTIVATE:
                    infoBuilder.setDeactivate(interpretDeactivate(
                            actionTO.getDeactivate(), projectedTopology));
                    break;
                default:
                    return Optional.empty();
            }

            action.setInfo(infoBuilder);

            return Optional.of(action.build());
        } catch (RuntimeException e) {
            logger.error("Unable to interpret actionTO " + actionTO + " due to: ", e);
            return Optional.empty();
        }
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
        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                    provisionByDemandTO.getModelSeller(), projectedTopology))
                .setProvisionedSeller(provisionByDemandTO.getProvisionedSeller())
                .build();
    }

    @Nonnull
    private ActionDTO.Provision interpretProvisionBySupply(
            @Nonnull final ProvisionBySupplyTO provisionBySupplyTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        return ActionDTO.Provision.newBuilder()
                .setEntityToClone(createActionEntity(
                        provisionBySupplyTO.getModelSeller(), projectedTopology))
                .setProvisionedSeller(provisionBySupplyTO.getProvisionedSeller())
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
                        // factors other than "limit" that could drive the VM resource
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
                } else {
                    resizeBuilder.setResizeTriggerTrader(createActionEntity(resizeTriggerTraderTO.get()
                            .getTrader(), projectedTopology));
                }
            // Scale Down: Pick related vSan host being suspended that has no reason commodities
            // since it is suspension due to low roi.
            } else if (adjustedResizeTO.getNewCapacity() < adjustedResizeTO.getOldCapacity()) {
                Optional<ResizeTriggerTraderTO> resizeTriggerTraderTO = resizeTO.getResizeTriggerTraderList().stream()
                    .filter(resizeTriggerTrader -> resizeTriggerTrader.getRelatedCommoditiesList()
                        .isEmpty()).findFirst();
                if (!resizeTriggerTraderTO.isPresent()) {
                    return Optional.empty();
                } else {
                    resizeBuilder.setResizeTriggerTrader(createActionEntity(resizeTriggerTraderTO.get()
                            .getTrader(), projectedTopology));
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
            destAzOrRegion = connectedEntities.isEmpty() ?
                destinationRegion : connectedEntities.get(0);
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
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                expBuilder.setMove(interpretMoveExplanation(actionTO, savings, projectedTopology));
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
        ChangeProviderExplanation.Builder changeProviderExplanation;
        switch (moveExplanation.getExplanationTypeCase()) {
            case COMPLIANCE:
                // TODO: For Cloud Migration plans, we need to change compliance to efficiency or
                // performance based on whether the entity resized down or up. This needs to be done
                // once resize and cloud migration stories are done
                Compliance.Builder compliance = ChangeProviderExplanation.Compliance.newBuilder()
                    .addAllMissingCommodities(
                        moveExplanation.getCompliance()
                            .getMissingCommoditiesList().stream()
                            .map(commodityConverter::commodityIdToCommodityType)
                            .filter(commType -> !tierExcluder.isCommodityTypeForTierExclusion(commType))
                            .map(commType2ReasonCommodity())
                            .collect(Collectors.toList())
                    );
                if (isPrimaryChangeExplanation) {
                    tierExcluder.getReasonSettings(actionTO).ifPresent(compliance::addAllReasonSettings);
                }
                changeProviderExplanation = ChangeProviderExplanation.newBuilder()
                    .setCompliance(compliance);
                break;
            case CONGESTION:
                // For cloud entities we explain create either an efficiency or congestion change
                // explanation
                changeProviderExplanation = changeExplanationForCloud(moveTO, savings).orElse(
                    ChangeProviderExplanation.newBuilder().setCongestion(
                        ChangeProviderExplanation.Congestion.newBuilder()
                        .addAllCongestedCommodities(
                                moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                                        .map(commodityConverter::commodityIdToCommodityTypeAndSlot)
                                        .map(commTypeAndSlot2ReasonCommodity(
                                            shoppingListOidToInfos.get(moveTO.getShoppingListToMove())
                                                .getBuyerId(), moveTO.getSource()))
                                        .collect(Collectors.toList()))
                        .build()));
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
                changeProviderExplanation = changeExplanationForCloud(moveTO, savings)
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

    private Optional<ChangeProviderExplanation.Builder> changeExplanationForCloud(
        @Nonnull final MoveTO moveTO, @Nonnull final CalculatedSavings savings) {
        Optional<ChangeProviderExplanation.Builder> explanation = Optional.empty();
        if (cloudTc.isMarketTier(moveTO.getSource()) && cloudTc.isMarketTier(moveTO.getDestination())) {
            ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
            Map<CommodityUsageType, Set<CommodityType>> commsResizedByUsageType =
                cert.getCommoditiesResizedByUsageType(slInfo.getBuyerId());
            Set<CommodityType> congestedComms = commsResizedByUsageType.get(CommodityUsageType.CONGESTED);
            Set<CommodityType> underUtilizedComms = commsResizedByUsageType.get(CommodityUsageType.UNDER_UTILIZED);
            boolean isPrimaryTierChange = TopologyDTOUtil.isPrimaryTierEntityType(
                cloudTc.getMarketTier(moveTO.getDestination()).getTier().getEntityType());
            // First, check if there was a congested commodity. If there was, then that is the one driving the action
            if (!congestedComms.isEmpty()) {
                Congestion.Builder congestionBuilder = ChangeProviderExplanation.Congestion.newBuilder()
                    .addAllCongestedCommodities(commTypes2ReasonCommodities(congestedComms));
                explanation =  Optional.of(ChangeProviderExplanation.newBuilder().setCongestion(
                    congestionBuilder));
            } else if (isPrimaryTierChange && didRiCoverageIncrease(slInfo.getBuyerId())) {
                // Next, check if RI Coverage increased. If yes, that is the reason of the action.
                Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder()
                        .setIsRiCoverageIncreased(true);
                explanation = Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
            } else if (!underUtilizedComms.isEmpty()) {
                // Next, check if there are any underUtilized commodities. If yes, then that is the reason of the action.
                Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder()
                    .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(underUtilizedComms));
                explanation = Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
            } else if (savings.savingsAmount.getValue() > 0) {
                // There were no under-utilized commodities. Action is purely because the same
                // requirements fit in a cheaper template.
                explanation = Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.newBuilder().setIsWastedCost(true)));
            } else {
                logger.error("Could not explain cloud scale action. MoveTO = {} .Entity oid = {}",
                    moveTO, slInfo.getBuyerId());
                explanation = Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.getDefaultInstance()));
            }
        }
        return explanation;
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
        public boolean hasSavings() {
            return false;
        }
    }
}
