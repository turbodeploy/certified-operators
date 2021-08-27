package com.vmturbo.market.topology.conversions;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.WastedCostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.TraxSavingsDetails;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.trax.TraxNumber;

/**
 * Some entities like RDS Database servers have merged compute and storage actions. This class is used to explain
 * actions on these entities which needed resize before analysis.
 */
public class MergedActionChangeExplainer extends ChangeExplainer {

    private Map<Integer, Set<Integer>> computeCommoditiesByEntityType = ImmutableMap.of(EntityType.DATABASE_SERVER_VALUE,
            ImmutableSet.of(CommodityType.VMEM_VALUE, CommodityType.VCPU_VALUE));
    private Map<Integer, Set<Integer>> storageCommoditiesByEntityType = ImmutableMap.of(EntityType.DATABASE_SERVER_VALUE,
            ImmutableSet.of(CommodityType.STORAGE_ACCESS_VALUE, CommodityType.STORAGE_AMOUNT_VALUE));

    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos;

    /**
     * The public constructor.
     * @param commoditiesResizeTracker Resize tracker.
     * @param cloudTc cloud topology converter
     * @param shoppingListOidToInfos map of shopping list oids to shopping list infos
     * @param commodityIndex the commodity index
     */
    public MergedActionChangeExplainer(CommoditiesResizeTracker commoditiesResizeTracker,
                                       CloudTopologyConverter cloudTc,
                                       Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                                       CommodityIndex commodityIndex) {
        super(commoditiesResizeTracker, cloudTc, commodityIndex);
        this.shoppingListOidToInfos = shoppingListOidToInfos;
    }

    @Override
    public Optional<ChangeProviderExplanation.Builder> changeExplanationFromTracker(
            @NotNull ActionDTOs.MoveTO moveTO,
            @NotNull CalculatedSavings savings,
            @NotNull Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopology) {
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(moveTO.getShoppingListToMove());
        final long actionTargetId = slInfo.getCollapsedBuyerId().orElse(slInfo.getBuyerId());
        int actionTargetEntityType = projectedTopology.containsKey(actionTargetId)
                ? projectedTopology.get(actionTargetId).getEntity().getEntityType() : -1;
        long sellerId = slInfo.getSellerId();
        // Find the commodities which have lower and higher capacity in the projected topology
        Pair<Set<TopologyDTO.CommodityType>, Set<TopologyDTO.CommodityType>> lowerAndHigherCapacityPair =
                calculateCommWithChangingProjectedCapacity(moveTO, actionTargetId, projectedTopology, sellerId);
        // Collection of commodities with lower projected capacity in projected topology.
        final Set<TopologyDTO.CommodityType> lowerProjectedCapacityComms = lowerAndHigherCapacityPair.getFirst();
        Optional<ChangeProviderExplanation.Builder> congestedExplanation =
                getCongestedExplanationFromTracker(actionTargetId, sellerId);
        Set<Integer> computeCommodities = computeCommoditiesByEntityType.getOrDefault(actionTargetEntityType, Sets.newHashSet());
        Set<Integer> storageCommodities = storageCommoditiesByEntityType.getOrDefault(actionTargetEntityType, Sets.newHashSet());
        if (congestedExplanation.isPresent()) {
            Congestion.Builder congestionBuilder = congestedExplanation.get().getCongestionBuilder();
            congestionBuilder.addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(lowerProjectedCapacityComms));
            Set<ActionDTO.Explanation.ReasonCommodity> explainedCommodities = Sets.newHashSet(congestionBuilder.getCongestedCommoditiesList());
            explainedCommodities.addAll(congestionBuilder.getUnderUtilizedCommoditiesList());
            boolean isComputeExplained = explainedCommodities.stream().map(c -> c.getCommodityType().getType()).anyMatch(computeCommodities::contains);
            boolean isStorageExplained = explainedCommodities.stream().map(c -> c.getCommodityType().getType()).anyMatch(storageCommodities::contains);
            if (!isComputeExplained && isCostForCategoryGoingDown(savings, CostCategory.ON_DEMAND_COMPUTE)) {
                congestionBuilder.setIsWastedCost(true);
                congestionBuilder.setWastedCostCategory(WastedCostCategory.COMPUTE);
            }
            if (!isStorageExplained && isCostForCategoryGoingDown(savings, CostCategory.STORAGE)) {
                congestionBuilder.setIsWastedCost(true);
                if (congestionBuilder.hasWastedCostCategory()) {
                    congestionBuilder.clearWastedCostCategory();
                } else {
                    congestionBuilder.setWastedCostCategory(WastedCostCategory.STORAGE);
                }
            }
            return congestedExplanation;
        }
        return buildEfficiencyExplanation(lowerProjectedCapacityComms, savings, computeCommodities, storageCommodities);
    }

    private Optional<ChangeProviderExplanation.Builder> buildEfficiencyExplanation(
            Set<TopologyDTO.CommodityType> lowerProjectedCapacityComms,
            CalculatedSavings savings,
            Set<Integer> computeCommodities,
            Set<Integer> storageCommodities) {
        Efficiency.Builder efficiencyExplanation = Efficiency.newBuilder();
        if (!lowerProjectedCapacityComms.isEmpty()) {
            efficiencyExplanation.addAllUnderUtilizedCommodities(commTypes2ReasonCommodities(lowerProjectedCapacityComms));
        }
        boolean isComputeExplained = lowerProjectedCapacityComms.stream().map(c -> c.getType()).anyMatch(computeCommodities::contains);
        boolean isStorageExplained = lowerProjectedCapacityComms.stream().map(c -> c.getType()).anyMatch(storageCommodities::contains);
        if (!isComputeExplained && isCostForCategoryGoingDown(savings, CostCategory.ON_DEMAND_COMPUTE)) {
            efficiencyExplanation.setIsWastedCost(true);
            efficiencyExplanation.setWastedCostCategory(WastedCostCategory.COMPUTE);
        }
        if (!isStorageExplained && isCostForCategoryGoingDown(savings, CostCategory.STORAGE)) {
            efficiencyExplanation.setIsWastedCost(true);
            if (efficiencyExplanation.hasWastedCostCategory()) {
                efficiencyExplanation.clearWastedCostCategory();
            } else {
                efficiencyExplanation.setWastedCostCategory(WastedCostCategory.STORAGE);
            }
        }
        return Optional.of(ChangeProviderExplanation.newBuilder().setEfficiency(efficiencyExplanation));
    }

    private boolean isCostForCategoryGoingDown(CalculatedSavings savings, CostCategory costCategory) {
        if (savings.cloudSavingsDetails().isPresent()) {
            TraxSavingsDetails cloudSavings = savings.cloudSavingsDetails().get();
            Optional<CostJournal<TopologyDTO.TopologyEntityDTO>> sourceCostJournal = cloudSavings.sourceTierCostDetails().costJournal();
            Optional<CostJournal<TopologyDTO.TopologyEntityDTO>> projectedCostJournal = cloudSavings.projectedTierCostDetails().costJournal();
            if (sourceCostJournal.isPresent() && projectedCostJournal.isPresent()) {
                TraxNumber sourceCost = sourceCostJournal.get().getHourlyCostForCategory(costCategory);
                TraxNumber projectedCost = projectedCostJournal.get().getHourlyCostForCategory(costCategory);
                return projectedCost.compareTo(sourceCost) < 0;
            }
        }
        return false;
    }
}