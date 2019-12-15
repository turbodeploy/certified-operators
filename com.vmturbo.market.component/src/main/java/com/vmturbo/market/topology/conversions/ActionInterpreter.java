package com.vmturbo.market.topology.conversions;

import static com.vmturbo.trax.Trax.trax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
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
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
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
    private final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage;
    private static final Set<EntityState> evacuationEntityState =
        EnumSet.of(EntityState.MAINTENANCE, EntityState.FAILOVER);

    ActionInterpreter(@Nonnull final CommodityConverter commodityConverter,
                      @Nonnull final Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                      @Nonnull final CloudTopologyConverter cloudTc, Map<Long, TopologyEntityDTO> originalTopology,
                      @Nonnull final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap, CloudEntityResizeTracker cert,
                      @Nonnull final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage,
                      @NonNull final TierExcluder tierExcluder) {
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.cloudTc = cloudTc;
        this.originalTopology = originalTopology;
        this.oidToProjectedTraderTOMap = oidToTraderTOMap;
        this.cert = cert;
        this.projectedRiCoverage = projectedRiCoverage;
        this.tierExcluder = tierExcluder;
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
                                     @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                     @NonNull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                     @NonNull TopologyCostCalculator topologyCostCalculator) {
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
                    infoBuilder.setMove(interpretMoveAction(
                            actionTO.getMove(), projectedTopology, originalCloudTopology));
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
                    infoBuilder.setResize(interpretResize(
                            actionTO.getResize(), projectedTopology));
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
    @NonNull
    CalculatedSavings calculateActionSavings(@Nonnull final ActionTO actionTO,
            @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology,
            @NonNull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
            @NonNull TopologyCostCalculator topologyCostCalculator) {
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

    @NonNull
    private CalculatedSavings calculateMoveSavings(@NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology,
                                                   @NonNull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
                                                   @NonNull TopologyCostCalculator topologyCostCalculator,
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
    @NonNull
    private TraxNumber getOnDemandCostForMarketTier(TopologyEntityDTO cloudEntityMoving,
                                                    MarketTier marketTier,
                                                    CostJournal<TopologyEntityDTO> journal) {
        final TraxNumber totalOnDemandCost;
        if (TopologyDTOUtil.isPrimaryTierEntityType(marketTier.getTier().getEntityType())) {
            TraxNumber onDemandComputeCost = journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE);
            TraxNumber licenseCost = journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE);
            TraxNumber ipCost = journal.getHourlyCostForCategory(CostCategory.IP);
            totalOnDemandCost = Stream.of(onDemandComputeCost, licenseCost, ipCost)
                .collect(TraxCollectors.sum(marketTier.getTier().getDisplayName() + " total cost"));
            logger.debug("Costs for {} on {} are -> on demand compute cost = {}, licenseCost = {}, ipCost = {}",
                    cloudEntityMoving.getDisplayName(), marketTier.getDisplayName(),
                    onDemandComputeCost, licenseCost, ipCost);
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
                        .map(commodityConverter::economyToTopologyCommodity)
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
            @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
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
     * Convert the {@link MoveTO} recieved from M2 into a {@link ActionDTO.Move} action.
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
    private ActionDTO.Move interpretMoveAction(@Nonnull final MoveTO moveTO,
                           @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
                                               @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final ShoppingListInfo shoppingList =
                shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        if (shoppingList == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for MOVE: " + moveTO);
        } else {
            return ActionDTO.Move.newBuilder()
                    .setTarget(
                            createActionEntity(shoppingList.buyerId, projectedTopology))
                    .addAllChanges(createChangeProviders(moveTO,
                            projectedTopology, originalCloudTopology, shoppingList.buyerId))
                    .build();
        }
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

            return builder.build();
        }
    }

    @Nonnull
    private ActionDTO.Resize interpretResize(@Nonnull final ResizeTO resizeTO,
                     @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long entityId = resizeTO.getSellingTrader();
        final CommodityType topologyCommodityType =
                commodityConverter.economyToTopologyCommodity(resizeTO.getSpecification())
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Resize commodity can't be converted to topology commodity format! "
                                        + resizeTO.getSpecification()));
        // Determine if this is a remove limit or a regular resize.
        final TopologyEntityDTO projectedEntity = projectedTopology.get(entityId).getEntity();

        // Find the CommoditySoldDTO for the resize commodity.
        final Optional<CommoditySoldDTO> resizeCommSold =
            projectedEntity.getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().equals(topologyCommodityType))
                .findFirst();
        if (projectedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE.getNumber()) {
            // If this is a VM and has a restricted capacity, we are going to assume it's a limit
            // removal. This logic seems like it could be fragile, in that limit may not be the
            // only way VM capacity could be restricted in the future, but this is consistent
            // with how classic makes the same decision.
            TraderTO traderTO = oidToProjectedTraderTOMap.get(entityId);
            // find the commodity on the trader and see if there is a limit?
            for (CommoditySoldTO commoditySold : traderTO.getCommoditiesSoldList()) {
                if (commoditySold.getSpecification().equals(resizeTO.getSpecification())) {
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
                        return resizeBuilder.build();
                    }
                    break;
                }
            }
        }
        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
                .setTarget(createActionEntity(entityId, projectedTopology))
                .setNewCapacity(resizeTO.getNewCapacity())
                .setOldCapacity(resizeTO.getOldCapacity())
                .setCommodityType(topologyCommodityType);
        setHotAddRemove(resizeBuilder, resizeCommSold);
        return resizeBuilder.build();
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
                        .map(commodityConverter::economyToTopologyCommodity)
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
    List<ChangeProvider> createChangeProviders(
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
        TopologyEntityDTO target = originalTopology.get(targetOid);
        Long moveSource = move.hasSource() ? move.getSource() : null;
        // Update moveSource if the original supplier is in MAINTENANCE or FAILOVER state.
        final ShoppingListInfo shoppingListInfo = shoppingListOidToInfos.get(move.getShoppingListToMove());
        TopologyEntityDTO destinationRegion;
        if (destMarketTier instanceof SingleRegionMarketTier) {
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
        TopologyEntityDTO sourceRegion = null;
        if (sourceMarketTier instanceof SingleRegionMarketTier) {
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
        if (!move.hasSource() &&
            shoppingListInfo.getSellerId() != null &&
            projectedTopology.containsKey(shoppingListInfo.getSellerId()) &&
            evacuationEntityState.contains(
                projectedTopology.get(shoppingListInfo.getSellerId()).getEntity().getEntityState()))
            moveSource = shoppingListInfo.getSellerId();
        if (destMarketTier != null ) {
            // TODO: We are considering the destination AZ as the first AZ of the destination
            // region. In case of zonal RIs, we need to get the zone of the RI.
            List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(
                    destinationRegion, EntityType.AVAILABILITY_ZONE_VALUE, originalTopology);
            destAzOrRegion = connectedEntities.isEmpty() ?
                    destinationRegion : connectedEntities.get(0);
            destTier = destMarketTier.getTier();
        }
        if (sourceMarketTier != null) {
            // Soruce AZ or Region is the AZ or Region which the target is connected to.
            List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(target,
                Sets.newHashSet(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE),
                originalTopology);
            if (!connectedEntities.isEmpty())
                sourceAzOrRegion = connectedEntities.get(0);
            else
                logger.error("{} is not connected to any AZ or Region", target.getDisplayName());
            sourceTier = sourceMarketTier.getTier();
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
            final boolean isAccountingAction = destinationRegion == sourceRegion
                    && destTier == sourceTier
                    && move.hasCouponDiscount()
                    && move.hasCouponId();
            if (destTier != sourceTier || isAccountingAction) {
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
            changeProviders.add(createChangeProvider(moveSource,
                    move.getDestination(), resourceId, projectedTopology));
        }
        return changeProviders;
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
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        // For simple moves, set the sole change provider explanation to be the primary one
        ChangeProviderExplanation.Builder changeProviderExplanation =
            changeExplanation(actionTO, actionTO.getMove(), savings, projectedTopology, true);
        moveExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
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
        CommodityType commType = commodityConverter.economyToTopologyCommodity(
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
        return ResizeExplanation.newBuilder().setStartUtilization(resizeTO.getStartUtilization())
            .setEndUtilization(resizeTO.getEndUtilization()).build();
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
            @NonNull ActionTO actionTO,
            @NonNull MoveTO moveTO,
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
                // explanation based on the savings
                changeProviderExplanation = changeExplanationBasedOnSavings(moveTO, savings).orElse(
                    ChangeProviderExplanation.newBuilder().setCongestion(
                        ChangeProviderExplanation.Congestion.newBuilder()
                        .addAllCongestedCommodities(
                                moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                                        .map(commodityConverter::commodityIdToCommodityType)
                                        .map(commType2ReasonCommodity())
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
                // explanation based on the savings
                changeProviderExplanation = changeExplanationBasedOnSavings(moveTO, savings)
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
     * For cloud entities, we calculate the change provider explanation of the action based on
     * savings. If there are savings, then the explanation is efficiency, otherwise we say
     * performance.
     *
     * @param moveTO the move for which we are computing the category
     * @param savings the savings
     * @return An optional {@link ChangeProviderExplanation.Builder}.
     */
    private Optional<ChangeProviderExplanation.Builder>
    changeExplanationBasedOnSavings(@Nonnull final MoveTO moveTO, @Nonnull final CalculatedSavings savings) {
        Optional<ChangeProviderExplanation.Builder> explanation = Optional.empty();
        if (cloudTc.isMarketTier(moveTO.getSource()) && cloudTc.isMarketTier(moveTO.getDestination())) {
            ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
            if (savings.hasSavings()) {
                boolean isPrimaryTierChange = TopologyDTOUtil.isPrimaryTierEntityType(
                        cloudTc.getMarketTier(moveTO.getDestination()).getTier().getEntityType());
                Map<CommodityUsageType, Set<CommodityType>> commsResizedByUsageType =
                        cert.getCommoditiesResizedByUsageType(slInfo.getBuyerId());
                Set<CommodityType> congestedComms = commsResizedByUsageType.get(CommodityUsageType.CONGESTED);
                Set<CommodityType> underUtilizedComms = commsResizedByUsageType.get(CommodityUsageType.UNDER_UTILIZED);
                if (savings.savingsAmount.getValue() >= 0) {
                    Efficiency.Builder efficiencyBuilder = ChangeProviderExplanation.Efficiency.newBuilder();
                    if (isPrimaryTierChange && cert.didCommoditiesOfEntityResize(slInfo.buyerId)) {
                        if (congestedComms != null && !congestedComms.isEmpty()) {
                            efficiencyBuilder
                                .addAllCongestedCommodities(commTypes2ReasonCommodities(congestedComms));
                        }
                        if (underUtilizedComms != null && !underUtilizedComms.isEmpty()) {
                            efficiencyBuilder
                                .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(underUtilizedComms));
                        }
                    } else if (isPrimaryTierChange && projectedRiCoverage.get(slInfo.getBuyerId()) != null) {
                        efficiencyBuilder.setIsRiCoverageIncreased(true);
                    }
                    explanation = Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                            efficiencyBuilder));
                } else {
                    Congestion.Builder congestionBuilder = ChangeProviderExplanation.Congestion.newBuilder();
                    if (isPrimaryTierChange && cert.didCommoditiesOfEntityResize(slInfo.getBuyerId())) {
                        if (congestedComms != null && !congestedComms.isEmpty()) {
                            congestionBuilder
                                .addAllCongestedCommodities(commTypes2ReasonCommodities(congestedComms));
                        }
                        if (underUtilizedComms != null && !underUtilizedComms.isEmpty()) {
                            congestionBuilder
                                .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(underUtilizedComms));
                        }
                    } else if (isPrimaryTierChange && projectedRiCoverage.get(slInfo.getBuyerId()) != null) {
                        congestionBuilder.setIsRiCoverageIncreased(true);
                    }
                    explanation =  Optional.of(ChangeProviderExplanation.newBuilder().setCongestion(
                            congestionBuilder));
                }
            } else {
                logger.error("No savings present while trying to explain move for {}",
                        originalTopology.get(slInfo.getBuyerId()).getDisplayName());
            }
        }
        return explanation;
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

    private static Collection<ReasonCommodity>
            commTypes2ReasonCommodities(Collection<CommodityType> types) {
        return types.stream().map(commType2ReasonCommodity()).collect(Collectors.toList());
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
