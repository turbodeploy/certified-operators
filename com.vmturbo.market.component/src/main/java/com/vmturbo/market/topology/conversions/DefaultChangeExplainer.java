package com.vmturbo.market.topology.conversions;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The default change explainer. It is used to explain actions which needed resizing before analysis - like Cloud VMS, VDI entities etc.
 * To explain Merged actions like in the case of RDS, we use {@link MergedActionChangeExplainer}
 */
public class DefaultChangeExplainer extends ChangeExplainer {

    private static final Logger logger = LogManager.getLogger();
    private static final Set<Integer> FREE_SCALE_UP_EXPLANATION_ENTITY_TYPES =
            ImmutableSet.of(CommonDTO.EntityDTO.EntityType.DATABASE_VALUE);

    private final ProjectedRICoverageCalculator projectedRICoverageCalculator;
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos;
    private final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology;

    /**
     * Default change explainer constructor.
     * @param commoditiesResizeTracker the commoditiesResizeTracker stores resizes done before analysis
     * @param cloudTc the cloud topology converter
     * @param projectedRICoverageCalculator projected RI coverage calculator
     * @param shoppingListOidToInfos map of shopping list oid to shopping list infos
     * @param commodityIndex the commodity index
     * @param originalTopology original topology which came into market component
     */
    public DefaultChangeExplainer(CommoditiesResizeTracker commoditiesResizeTracker,
                                  CloudTopologyConverter cloudTc,
                                  ProjectedRICoverageCalculator projectedRICoverageCalculator,
                                  Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                                  CommodityIndex commodityIndex,
                                  Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology) {
        super(commoditiesResizeTracker, cloudTc, commodityIndex);
        this.projectedRICoverageCalculator = projectedRICoverageCalculator;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
        this.originalTopology = originalTopology;
    }

    /**
     * Get the change explanations based on the tracker.
     * @param moveTO the move to explain
     * @param savings the savings
     * @param projectedTopology the projected topology - topology outputted by analysis
     * @return Optional change explanation
     */
    @Override
    public Optional<ChangeProviderExplanation.Builder> changeExplanationFromTracker(
            @NotNull ActionDTOs.MoveTO moveTO,
            @NotNull CloudActionSavingsCalculator.CalculatedSavings savings,
            @NotNull Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopology) {
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        final long actionTargetId = slInfo.getCollapsedBuyerId().orElse(slInfo.getBuyerId());
        int actionTargetEntityType = projectedTopology.containsKey(actionTargetId)
                ? projectedTopology.get(actionTargetId).getEntity().getEntityType() : -1;
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
        Pair<Set<TopologyDTO.CommodityType>, Set<TopologyDTO.CommodityType>> lowerAndHigherCapacityPair =
                calculateCommWithChangingProjectedCapacity(moveTO, actionTargetId, projectedTopology, sellerId);
        // Collection of commodities with lower projected capacity in projected topology.
        final Set<TopologyDTO.CommodityType> lowerProjectedCapacityComms = lowerAndHigherCapacityPair.getFirst();
        // Collection of commodities with higher projected capacity in projected topology.
        final Set<TopologyDTO.CommodityType> higherProjectedCapacityComms = lowerAndHigherCapacityPair.getSecond();

        // Third, check if there is an under-utilized commodity which became lower capacity
        // in the projected topology.
        Optional<ChangeProviderExplanation.Builder> underUtilizedExplanation =
                getUnderUtilizedExplanationFromTracker(actionTargetId, sellerId, lowerProjectedCapacityComms,
                        higherProjectedCapacityComms, savings, actionTargetEntityType);
        if (underUtilizedExplanation.isPresent()) {
            return underUtilizedExplanation;
        }

        // Fourth, check if there is savings
        Optional<ChangeProviderExplanation.Builder> savingsExplanation =
                getExplanationFromSaving(savings, higherProjectedCapacityComms, actionTargetEntityType);
        if (savingsExplanation.isPresent()) {
            return savingsExplanation;
        }

        // Fifth, check if the action is a CSG action
        Optional<ChangeProviderExplanation.Builder> csgExplanation =
                getConistentScalingExplanationForCloud(moveTO);
        if (csgExplanation.isPresent()) {
            return csgExplanation;
        }

        // Sixth, check if we got an action with zero savings and the same projected capacities.
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

    @Nonnull
    private Optional<ChangeProviderExplanation.Builder> getUnderUtilizedExplanationFromTracker(
            long actionTargetId,
            long sellerId,
            @Nonnull Set<TopologyDTO.CommodityType> lowerProjectedCapacityComms,
            @Nonnull Set<TopologyDTO.CommodityType> higherProjectedCapacityComms,
            CloudActionSavingsCalculator.CalculatedSavings savings,
            int actionTargetEntityType) {
        if (lowerProjectedCapacityComms.isEmpty()) {
            return Optional.empty();
        }
        Efficiency.Builder efficiencyBuilder = Efficiency.newBuilder()
                .addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(lowerProjectedCapacityComms));
        logger.debug("Underutilized Commodities from tracker for buyer:{}, seller: {} : [{}]",
                actionTargetId, sellerId,
                lowerProjectedCapacityComms.stream().map(AbstractMessage::toString).collect(Collectors.joining()));
        if ((savings.isSavings() || savings.isZeroSavings())
                && FREE_SCALE_UP_EXPLANATION_ENTITY_TYPES.contains(actionTargetEntityType)) {
            // Check if we got a scale up for free. This can happen when the savings is 0 or greater,
            // and there are some commodities which have higher projected capacities.
            efficiencyBuilder
                    .addAllScaleUpCommodity(higherProjectedCapacityComms);
        }
        return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                efficiencyBuilder));
    }

    private Optional<ChangeProviderExplanation.Builder> getRIIncreaseExplanation(long id, @Nonnull final ActionDTOs.MoveTO moveTO) {
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())
                && TopologyDTOUtil.isPrimaryTierEntityType(cloudTc.getMarketTier(moveTO.getDestination()).getTier().getEntityType())
                && didRiCoverageIncrease(id)) {
            Efficiency.Builder efficiencyBuilder = Efficiency.newBuilder()
                    .setIsRiCoverageIncreased(true);
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    efficiencyBuilder));
        }
        return Optional.empty();
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
        Cost.EntityReservedInstanceCoverage projectedCoverage = projectedRICoverageCalculator
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
        Optional<Cost.EntityReservedInstanceCoverage> originalCoverage = cloudTc.getRiCoverageForEntity(entityId);
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

    private Optional<ChangeProviderExplanation.Builder> getExplanationFromSaving(
            @Nonnull final CloudActionSavingsCalculator.CalculatedSavings savings,
            @Nonnull Set<TopologyDTO.CommodityType> higherProjectedCapacityComms,
            @Nonnull int actionTargetEntityType) {
        if (!savings.isSavings()) {
            return Optional.empty();
        }
        Efficiency.Builder efficiencyBuilder = Efficiency.newBuilder().setIsWastedCost(true);
        if (!higherProjectedCapacityComms.isEmpty()
                && FREE_SCALE_UP_EXPLANATION_ENTITY_TYPES.contains(actionTargetEntityType)) {
            efficiencyBuilder.addAllScaleUpCommodity(higherProjectedCapacityComms);
        }
        return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(efficiencyBuilder));
    }

    protected Optional<ChangeProviderExplanation.Builder> getConistentScalingExplanationForCloud(ActionDTOs.MoveTO moveTO) {
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        TopologyDTO.TopologyEntityDTO originalTrader = slInfo != null ? originalTopology.get(slInfo.getBuyerId()) : null;
        if (moveTO.hasScalingGroupId() && originalTrader != null
                && originalTrader.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setCompliance(
                    ChangeProviderExplanation.Compliance.newBuilder().setIsCsgCompliance(true)));
        }
        return Optional.empty();
    }

    protected Optional<ChangeProviderExplanation.Builder> getZeroSavingsAndNoCommodityChangeExplanation(
            @Nonnull final ActionDTOs.MoveTO moveTO,
            @Nonnull final CloudActionSavingsCalculator.CalculatedSavings savings,
            @Nonnull final Set<TopologyDTO.CommodityType> lowerProjectedCapacityComms,
            @Nonnull final Set<TopologyDTO.CommodityType> higherProjectedCapacityComms) {
        // Check if this is a Cloud Move action with zero savings and no commodity changes
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())
                && savings.isZeroSavings()
                && lowerProjectedCapacityComms.isEmpty()
                && higherProjectedCapacityComms.isEmpty()) {
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.newBuilder()));
        }
        return Optional.empty();
    }

    protected Optional<ChangeProviderExplanation.Builder> getDefaultExplanationForCloud(
            long id, @Nonnull final ActionDTOs.MoveTO moveTO) {
        if (cloudTc.isMarketTier(moveTO.getSource())
                && cloudTc.isMarketTier(moveTO.getDestination())) {
            logger.error("Could not explain cloud scale action. MoveTO = {} .Entity oid = {}", moveTO, id);
            return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(
                    Efficiency.getDefaultInstance()));
        }
        return Optional.empty();
    }
}
