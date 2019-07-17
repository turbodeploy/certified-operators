package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
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
    private final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage;
    private static final Set<EntityState> evacuationEntityState =
        EnumSet.of(EntityState.MAINTENANCE, EntityState.FAILOVER);

    ActionInterpreter(@Nonnull final CommodityConverter commodityConverter,
                      @Nonnull final Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                      @Nonnull final CloudTopologyConverter cloudTc, Map<Long, TopologyEntityDTO> originalTopology,
                      @Nonnull final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap, CloudEntityResizeTracker cert,
                      @Nonnull final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage) {
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.cloudTc = cloudTc;
        this.originalTopology = originalTopology;
        this.oidToProjectedTraderTOMap = oidToTraderTOMap;
        this.cert = cert;
        this.projectedRiCoverage = projectedRiCoverage;
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
            Optional<CurrencyAmount> savings = calculateActionSavings(actionTO,
                    originalCloudTopology, projectedCosts, topologyCostCalculator);
            // The action importance should never be infinite, as setImportance() will fail.
            final Action.Builder action = Action.newBuilder()
                    // Assign a unique ID to each generated action.
                    .setId(IdentityGenerator.next())
                    .setDeprecatedImportance(actionTO.getImportance())
                    .setExplanation(interpretExplanation(actionTO, savings, projectedTopology))
                    .setExecutable(!actionTO.getIsNotExecutable());
            savings.ifPresent(action::setSavingsPerHour);

            final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();

            switch (actionTO.getActionTypeCase()) {
                case MOVE:
                    Move move = interpretMoveAction(actionTO.getMove(), projectedTopology);
                    if (move.getChangesList().isEmpty()) {
                        // There needs to be at least one change provider. It will currently hit
                        // this block for Accounting actions on the cloud.
                        return Optional.empty();
                    }
                    infoBuilder.setMove(move);
                    break;
                case COMPOUND_MOVE:
                    infoBuilder.setMove(interpretCompoundMoveAction(actionTO.getCompoundMove(),
                            projectedTopology));
                    break;
                case RECONFIGURE:
                    infoBuilder.setReconfigure(interpretReconfigureAction(
                            actionTO.getReconfigure(), projectedTopology));
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
     * @return
     */
    @NonNull
    Optional<CurrencyAmount> calculateActionSavings(@Nonnull final ActionTO actionTO,
            @NonNull CloudTopology<TopologyEntityDTO> originalCloudTopology,
            @NonNull Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts,
            @NonNull TopologyCostCalculator topologyCostCalculator) {
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                MoveTO move = actionTO.getMove();
                MarketTier destMarketTier = cloudTc.getMarketTier(move.getDestination());
                MarketTier sourceMarketTier = cloudTc.getMarketTier(move.getSource());
                // Savings can be calculated if the destination is cloud
                if (destMarketTier != null) {
                    TopologyEntityDTO cloudEntityMoving = getTopologyEntityMoving(move);
                    if (cloudEntityMoving != null) {
                        CostJournal<TopologyEntityDTO> destCostJournal = projectedCosts
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
                            double onDemandDestCost = getOnDemandCostForMarketTier(
                                    cloudEntityMoving, destMarketTier, destCostJournal);
                            // Now get the source cost. Assume on prem source cost 0.
                            double onDemandSourceCost = 0;
                            if (sourceMarketTier != null) {
                                Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal =
                                        topologyCostCalculator.calculateCostForEntity(
                                                originalCloudTopology, cloudEntityMoving);
                                if (!sourceCostJournal.isPresent()) {
                                    return Optional.empty();
                                }
                                onDemandSourceCost = getOnDemandCostForMarketTier(cloudEntityMoving,
                                        sourceMarketTier, sourceCostJournal.get());
                            }
                            double savings = onDemandSourceCost - onDemandDestCost;
                            logger.debug("Savings of action for {} moving from {} to {} is {}",
                                    cloudEntityMoving.getDisplayName(),
                                    sourceMarketTier.getDisplayName(),
                                    destMarketTier.getDisplayName(), savings);
                            return Optional.of(CurrencyAmount.newBuilder().setAmount(savings).build());
                        }
                    }
                }
                break;
            case COMPOUND_MOVE:
                List<MoveTO> moves = actionTO.getCompoundMove().getMovesList();
                // Savings can be calculated if the destination is cloud
                boolean isDestCloud = moves.stream().anyMatch(m -> cloudTc.isMarketTier(m.getDestination()));
                boolean isSourceCloud = moves.stream().anyMatch(m -> cloudTc.isMarketTier(m.getSource()));
                if (isDestCloud) {
                    TopologyEntityDTO cloudEntityMoving = getTopologyEntityMoving(moves.get(0));
                    if (cloudEntityMoving != null) {
                        CostJournal<TopologyEntityDTO> destCostJournal = projectedCosts
                                .get(cloudEntityMoving.getOid());
                        if (destCostJournal != null) {
                            double totalOnDemandDestCost = destCostJournal
                                    .getTotalHourlyCostExcluding(Sets.immutableEnumSet(CostCategory.RI_COMPUTE));
                            // Now get the source cost. Assume on prem source cost 0.
                            double totalOnDemandSourceCost = 0;
                            if (isSourceCloud) {
                                Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal =
                                        topologyCostCalculator.calculateCostForEntity(
                                                originalCloudTopology, cloudEntityMoving);
                                if (!sourceCostJournal.isPresent()) {
                                    return Optional.empty();
                                }
                                totalOnDemandSourceCost = sourceCostJournal.get()
                                        .getTotalHourlyCostExcluding(Sets.immutableEnumSet(CostCategory.RI_COMPUTE));
                            }
                            double savings = totalOnDemandSourceCost - totalOnDemandDestCost;
                            logger.debug("Savings of compound move for {} = {}",
                                    cloudEntityMoving.getDisplayName(), savings);
                            return Optional.of(CurrencyAmount.newBuilder().setAmount(savings).build());
                        }
                    }
                }
                break;
            default:
                return Optional.empty();
        }
        return Optional.empty();
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
    private double getOnDemandCostForMarketTier(TopologyEntityDTO cloudEntityMoving,
                                            MarketTier marketTier,
                                            CostJournal<TopologyEntityDTO> journal) {
        double totalOnDemandCost = 0;
        if (TopologyDTOUtil.isPrimaryTierEntityType(marketTier.getTier().getEntityType())) {
            double onDemandComputeCost = journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE);
            double licenseCost = journal.getHourlyCostForCategory(CostCategory.LICENSE);
            double ipCost = journal.getHourlyCostForCategory(CostCategory.IP);
            totalOnDemandCost = onDemandComputeCost + licenseCost + ipCost;
            logger.debug("Costs for {} on {} are -> on demand compute cost = {}, licenseCost = {}, ipCost = {}",
                    cloudEntityMoving.getDisplayName(), marketTier.getDisplayName(),
                    onDemandComputeCost, licenseCost, ipCost);
        } else {
            totalOnDemandCost = journal.getHourlyCostForCategory(CostCategory.STORAGE);
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
     * @return {@link ActionDTO.Move} representing the compound move
     */
    @Nonnull
    private ActionDTO.Move interpretCompoundMoveAction(
            @Nonnull final CompoundMoveTO compoundMoveTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
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
                        .map(move -> createChangeProviders(move, projectedTopology, targetOid))
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
     * @return {@link ActionDTO.Move} representing the move
     */
    @Nonnull
    private ActionDTO.Move interpretMoveAction(@Nonnull final MoveTO moveTO,
                           @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
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
                            projectedTopology, shoppingList.buyerId))
                    .build();
        }
    }

    @Nonnull
    private ActionDTO.Reconfigure interpretReconfigureAction(
            @Nonnull final ReconfigureTO reconfigureTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final ShoppingListInfo shoppingList =
                shoppingListOidToInfos.get(reconfigureTO.getShoppingListToReconfigure());
        if (shoppingList == null) {
            throw new IllegalStateException(
                    "Market returned invalid shopping list for RECONFIGURE: " + reconfigureTO);
        } else {
            final ActionDTO.Reconfigure.Builder builder = ActionDTO.Reconfigure.newBuilder()
                    .setTarget(createActionEntity(shoppingList.buyerId, projectedTopology));

            if (reconfigureTO.hasSource()) {
                builder.setSource(createActionEntity(reconfigureTO.getSource(), projectedTopology));
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
     * @return A list of change providers representing the move
     */
    @Nonnull
    List<ChangeProvider> createChangeProviders(
            @Nonnull final MoveTO move, @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            Long targetOid) {
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
                    destMarketTier.getRegion(), EntityType.AVAILABILITY_ZONE_VALUE, originalTopology);
            destAzOrRegion = connectedEntities.isEmpty() ?
                    destMarketTier.getRegion() : connectedEntities.get(0);
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
            if ((destMarketTier.getRegion() == sourceMarketTier.getRegion()) && (destTier == sourceTier)
                    && (move.hasCouponDiscount()&& move.hasCouponId())) {
                logger.warn("ACCOUNTING action generated for {}. We do not handle accounting " +
                        "actions YET. Dropping action.", target.getDisplayName());
            }
            // Cloud to cloud move
            if (destMarketTier.getRegion() != sourceMarketTier.getRegion()) {
                // AZ or Region change provider. We create an AZ or Region change provider
                // because the target is connected to AZ or Region.
                changeProviders.add(createChangeProvider(sourceAzOrRegion.getOid(),
                    destAzOrRegion.getOid(), null, projectedTopology));
            }
            if (destTier != sourceTier) {
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
            @Nonnull ActionTO actionTO, @Nonnull Optional<CurrencyAmount> savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        Explanation.Builder expBuilder = Explanation.newBuilder();
        switch (actionTO.getActionTypeCase()) {
            case MOVE:
                expBuilder.setMove(interpretMoveExplanation(actionTO.getMove(), savings, projectedTopology));
                break;
            case RECONFIGURE:
                expBuilder.setReconfigure(
                        interpretReconfigureExplanation(actionTO.getReconfigure()));
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
                    actionTO.getCompoundMove().getMovesList(), projectedTopology));
                break;
            default:
                throw new IllegalArgumentException("Market returned invalid action type "
                        + actionTO.getActionTypeCase());
        }
        return expBuilder.build();
    }

    private MoveExplanation interpretMoveExplanation(
            @Nonnull MoveTO moveTO, @Nonnull Optional<CurrencyAmount> savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        // For simple moves, set the sole change provider explanation to be the primary one
        ChangeProviderExplanation.Builder changeProviderExplanation =
            changeExplanation(moveTO, savings, projectedTopology)
                .setIsPrimaryChangeProviderExplanation(true);
        moveExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
        return moveExpBuilder.build();
    }

    private ReconfigureExplanation
    interpretReconfigureExplanation(ReconfigureTO reconfTO) {
        return ReconfigureExplanation.newBuilder()
                .addAllReconfigureCommodity(reconfTO.getCommodityToReconfigureList().stream()
                        .map(commodityConverter::commodityIdToCommodityType)
                        .map(commType2ReasonCommodity())
                        .collect(Collectors.toList()))
                .build();
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
            @Nonnull List<MoveTO> moveTOs,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        MoveExplanation.Builder moveExpBuilder = MoveExplanation.newBuilder();
        for (MoveTO moveTO : moveTOs) {
            TraderTO destinationTrader = oidToProjectedTraderTOMap.get(moveTO.getDestination());
            boolean isPrimaryChangeExplanation = destinationTrader != null
                    && ActionDTOUtil.isPrimaryEntityType(destinationTrader.getType());
            ChangeProviderExplanation.Builder changeProviderExplanation =
                changeExplanation(moveTO, Optional.empty(), projectedTopology)
                    .setIsPrimaryChangeProviderExplanation(isPrimaryChangeExplanation);
            moveExpBuilder.addChangeProviderExplanation(changeProviderExplanation);
        }
        return moveExpBuilder.build();
    }

    private ChangeProviderExplanation.Builder changeExplanation(
            MoveTO moveTO, Optional<CurrencyAmount> savings,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        ActionDTOs.MoveExplanation moveExplanation = moveTO.getMoveExplanation();
        switch (moveExplanation.getExplanationTypeCase()) {
            case COMPLIANCE:
                // TODO: For Cloud Migration plans, we need to change compliance to efficiency or
                // performance based on whether the entity resized down or up. This needs to be done
                // once resize and cloud migration stories are done
                return ChangeProviderExplanation.newBuilder()
                        .setCompliance(ChangeProviderExplanation.Compliance.newBuilder()
                                .addAllMissingCommodities(
                                        moveExplanation.getCompliance()
                                                .getMissingCommoditiesList().stream()
                                                .map(commodityConverter::commodityIdToCommodityType)
                                                .map(commType2ReasonCommodity())
                                                .collect(Collectors.toList())
                                )
                                .build());
            case CONGESTION:
                // For cloud entities we explain create either an efficiency or congestion change
                // explanation based on the savings
                return changeExplanationBasedOnSavings(moveTO, savings).orElse(
                        ChangeProviderExplanation.newBuilder().setCongestion(
                                ChangeProviderExplanation.Congestion.newBuilder()
                                .addAllCongestedCommodities(
                                        moveExplanation.getCongestion().getCongestedCommoditiesList().stream()
                                                .map(commodityConverter::commodityIdToCommodityType)
                                                .map(commType2ReasonCommodity())
                                                .collect(Collectors.toList()))
                                .build()));
            case EVACUATION:
                return ChangeProviderExplanation.newBuilder()
                        .setEvacuation(ChangeProviderExplanation.Evacuation.newBuilder()
                                .setSuspendedEntity(moveExplanation.getEvacuation().getSuspendedTrader())
                                .build());
            case INITIALPLACEMENT:
                final ShoppingListInfo shoppingListInfo =
                    shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
                // Create Evacuation instead of InitialPlacement
                // if the original supplier is in MAINTENANCE or FAILOVER state.
                if (shoppingListInfo.getSellerId() != null &&
                    projectedTopology.containsKey(shoppingListInfo.getSellerId()) &&
                    evacuationEntityState.contains(
                        projectedTopology.get(shoppingListInfo.getSellerId()).getEntity().getEntityState()))
                    return ChangeProviderExplanation.newBuilder()
                        .setEvacuation(ChangeProviderExplanation.Evacuation.newBuilder()
                            .setSuspendedEntity(shoppingListInfo.getSellerId())
                            .setIsAvailable(false)
                            .build());
                else
                    return ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(ChangeProviderExplanation.InitialPlacement.getDefaultInstance());
            case PERFORMANCE:
                // For cloud entities we explain create either an efficiency or congestion change
                // explanation based on the savings
                return changeExplanationBasedOnSavings(moveTO, savings).orElse(ChangeProviderExplanation.newBuilder()
                        .setPerformance(ChangeProviderExplanation.Performance.getDefaultInstance()));
            default:
                logger.error("Unknown explanation case for move action: "
                        + moveExplanation.getExplanationTypeCase());
                return ChangeProviderExplanation.getDefaultInstance().toBuilder();
        }
    }

    /**
     * For cloud entities, we calculate the change provider explanation of the action based on
     * savings. If there are savings, then the explanation is efficiency, otherwise we say
     * performance.
     *
     * @param moveTO the move for which we are computing the category
     * @param savings the savings
     * @return
     */
    private Optional<ChangeProviderExplanation.Builder> changeExplanationBasedOnSavings(MoveTO moveTO,
                                                                                Optional<CurrencyAmount> savings) {
        Optional<ChangeProviderExplanation.Builder> explanation = Optional.empty();
        if (cloudTc.isMarketTier(moveTO.getSource()) && cloudTc.isMarketTier(moveTO.getDestination())) {
            ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
            if (savings.isPresent()) {
                boolean isPrimaryTierChange = TopologyDTOUtil.isPrimaryTierEntityType(
                        cloudTc.getMarketTier(moveTO.getDestination()).getTier().getEntityType());
                Map<CommodityUsageType, Set<CommodityType>> commsResizedByUsageType =
                        cert.getCommoditiesResizedByUsageType(slInfo.getBuyerId());
                Set<CommodityType> congestedComms = commsResizedByUsageType.get(CommodityUsageType.CONGESTED);
                Set<CommodityType> underUtilizedComms = commsResizedByUsageType.get(CommodityUsageType.UNDER_UTILIZED);
                if (savings.get().getAmount() >= 0) {
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

}
