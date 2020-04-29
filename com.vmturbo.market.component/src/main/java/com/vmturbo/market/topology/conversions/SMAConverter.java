package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Compliance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Congestion;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

/**
 * This class has methods to convert the smaOutput to projectedTraderDTOs and "M2 like" ActionTOs.
 */
public class SMAConverter {

    private final TopologyConverter converter;

    private static final Logger logger = LogManager.getLogger();

    /**
     * constructor.
     *
     * @param converter the current topology converter.
     */
    public SMAConverter(@Nonnull final TopologyConverter converter) {
        this.converter = Objects.requireNonNull(converter);
    }

    // Stable Marriage related actions.
    private SMAOutput smaOutput = new SMAOutput(new ArrayList<>());

    public SMAOutput getSmaOutput() {
        return smaOutput;
    }

    public void setSmaOutput(final SMAOutput smaOutput) {
        this.smaOutput = smaOutput;
    }

    private List<TraderTO> projectedTraderDTOsWithSMA = new ArrayList<>();

    public List<TraderTO> getProjectedTraderDTOsWithSMA() {
        return projectedTraderDTOsWithSMA;
    }

    private List<ActionTO> smaActions = new ArrayList<>();

    public List<ActionTO> getSmaActions() {
        return smaActions;
    }

    // map from the oid of projected trader to the projected trader.
    private Map<Long, TraderTO> oidToProjectedTraderMap = new HashMap<>();

    /**
     * Populate projectedTraderDTOsWithSMA and smaActions using smaOutput and projectedTraderDTOs.
     * The SMA algorithm has already populated the smaOutput.
     *
     * @param projectedTraderDTOs the projectedTraderDTOs from market2.
     */
    public void updateWithSMAOutput(List<TraderTO> projectedTraderDTOs) {
        // create a map of the projectedTrader to OID so tht it can be replaced with updated ones.
        for (TraderTO traderTO : projectedTraderDTOs) {
            oidToProjectedTraderMap.put(traderTO.getOid(), traderTO);
        }
        // update projectedTraderDTOsWithSMA.
        updateProjectTraderWithSMA();
        // update smaActions.
        updateActionsWithSMA();
    }

    /**
     * Update the projectedTraderDTOs with the results of SMA (stable marriage algorithm).
     * Update the supplier of the compute shopping list of cloud vms. Add coupon
     * commodity to the compute shopping list and update quantity appropriately.
     */
    private void updateProjectTraderWithSMA() {
        for (SMAOutputContext outputContext : getSmaOutput().getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                long vmOid = smaMatch.getVirtualMachine().getOid();
                TraderTO projectedVmTraderTO =
                        oidToProjectedTraderMap.get(vmOid);
                Long destinationOnDemandMarketTierOid = converter.getCloudTc().getTraderTOOid(
                        new OnDemandMarketTier(converter.getUnmodifiableEntityOidToDtoMap()
                                .get(smaMatch.getTemplate().getOid())));
                if (destinationOnDemandMarketTierOid == null) {
                    logger.error("TraderTO for topology entity: {} not found in scope.",
                            smaMatch.getTemplate().getOid());
                    continue;
                }
                Long destinationRIDiscountedMarketTierOid = destinationOnDemandMarketTierOid;
                // If SMAMatch uses an RI, then create a corresponding
                // RiDiscountedMarketTier and use it instead of the OnDemandMarketTier.
                if (smaMatch.getDiscountedCoupons() > 0) {
                    TopologyEntityDTO region = converter.getCloudTc()
                            .getRegionOfCloudConsumer(
                                    converter.getUnmodifiableEntityOidToDtoMap().get(vmOid));
                    if (converter.getUnmodifiableEntityOidToDtoMap().get(vmOid) == null ||
                            region == null) {
                        logger.error("Region for topology entity: {} not found in scope.",
                                outputContext.getContext().getRegionId());
                        continue;
                    }
                    final ReservedInstanceData riData = converter.getCloudTc().getRiDataById(
                            smaMatch.getReservedInstance().getOid());
                    if (riData == null) {
                        logger.error("Reserved Instance for topology entity: {} " +
                                        "not found in scope.",
                                smaMatch.getReservedInstance().getOid());
                        continue;
                    }

                    destinationRIDiscountedMarketTierOid = converter.getCloudTc()
                        .getRIDiscountedMarketTierIDFromRIData(riData);

                }
                // Update the supplier of the compute shopping list of cloud VMs. Also add coupon
                // commodity to the compute shopping list and update quantity appropriately.
                // For each VM, generate a new shopping list according to the SMA (slWithSMA) and
                // update oidToProjectedTraderMap with the
                // updated projected VM (projectedVmTraderTOWithSMA).
                int indexOfComputeShoppingList =
                        converter.getCloudTc().getIndexOfSlSuppliedByPrimaryTier(projectedVmTraderTO);
                if (indexOfComputeShoppingList < 0) {
                    logger.error("No compute shopping list for topology entity: {} ",
                            vmOid);
                    continue;
                }
                ShoppingListTO sl =
                        projectedVmTraderTO.getShoppingListsList()
                                .get(indexOfComputeShoppingList);
                ShoppingListTO.Builder slWithSMA =
                        ShoppingListTO.newBuilder(sl)
                                .setSupplier(destinationOnDemandMarketTierOid)
                                .clearCouponId();
                if (smaMatch.getDiscountedCoupons() > 0) {
                    // add the coupon commodity to the compute shopping list.
                    Optional<CommodityBoughtTO> coupon =
                            converter.createCouponCommodityBoughtForCloudEntity(
                                    destinationRIDiscountedMarketTierOid,
                                    vmOid);
                    if (coupon.isPresent()) {
                        CommodityBoughtTO.Builder couponCommodity =
                                CommodityBoughtTO.newBuilder(coupon.get());
                        couponCommodity.setQuantity(smaMatch.getDiscountedCoupons());
                        slWithSMA.addCommoditiesBought(couponCommodity);
                        slWithSMA.setCouponId(destinationRIDiscountedMarketTierOid);
                    }
                }
                TraderTO projectedVmTraderTOWithSMA = TraderTO.newBuilder(projectedVmTraderTO)
                        .removeShoppingLists(indexOfComputeShoppingList)
                        .addShoppingLists(slWithSMA).build();
                oidToProjectedTraderMap.put(
                        projectedVmTraderTOWithSMA.getOid(),
                        projectedVmTraderTOWithSMA);

            }
        }
        projectedTraderDTOsWithSMA.clear();
        projectedTraderDTOsWithSMA.addAll(oidToProjectedTraderMap.values());
    }


    /**
     * Generate "market2 like" actions based on the matching obtained from sma ie smaOutput.
     */
    private void updateActionsWithSMA() {
        smaActions = new ArrayList<>();
        for (SMAOutputContext outputContext : getSmaOutput().getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                long vmOid = smaMatch.getVirtualMachine().getOid();
                TraderTO projectedVmTraderTO =
                        oidToProjectedTraderMap.get(vmOid);
                Long destinationOnDemandMarketTierOid = converter.getCloudTc().getTraderTOOid(
                        new OnDemandMarketTier(converter.getUnmodifiableEntityOidToDtoMap()
                                .get(smaMatch.getTemplate().getOid())));
                if (destinationOnDemandMarketTierOid == null) {
                    logger.error("TraderTO for topology entity: {} not found in scope.",
                            smaMatch.getTemplate().getOid());
                    continue;
                }
                Long sourceOnDemandMarketTierOid = converter.getCloudTc().getTraderTOOid(
                        new OnDemandMarketTier(converter.getUnmodifiableEntityOidToDtoMap()
                                .get(smaMatch.getVirtualMachine()
                                        .getCurrentTemplate().getOid())));
                if (sourceOnDemandMarketTierOid == null) {
                    logger.error("TraderTO for topology entity: {} not found in scope.",
                            smaMatch.getVirtualMachine()
                                    .getCurrentTemplate().getOid());
                    continue;
                }
                int indexOfComputeShoppingList =
                        converter.getCloudTc().getIndexOfSlSuppliedByPrimaryTier(projectedVmTraderTO);
                if (indexOfComputeShoppingList < 0) {
                    continue;
                }
                ShoppingListTO sl =
                        projectedVmTraderTO.getShoppingListsList()
                                .get(indexOfComputeShoppingList);
                TraderTO sourceTraderTO = converter.getUnmodifiableOidToOriginalTraderTOMap()
                        .get(sourceOnDemandMarketTierOid);

                // Compare the comm sold by the current supplier and comm bought by the VM and
                // find the missing commodities.
                List<Integer> missingCommodities = new ArrayList<>();
                for (int i = 0; i < sl.getCommoditiesBoughtList().size(); i++) {
                    CommodityBoughtTO commBought = sl.getCommoditiesBoughtList().get(i);
                    boolean commditySatisfied = false;
                    for (CommoditySoldTO commSold : sourceTraderTO.getCommoditiesSoldList()) {
                        if (commBought.getSpecification().getType() ==
                                commSold.getSpecification().getType()) {
                            commditySatisfied = true;
                            break;
                        }
                    }
                    if (!commditySatisfied) {
                        missingCommodities.add(commBought.getSpecification().getType());
                    }
                }
                if (smaMatch.getVirtualMachine().getGroupProviders().size() == 0) {
                    // Reconfigure action. There is no provider that can provides all the
                    // commodities the VM is shopping.
                    ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();
                    reconfigureTO.setShoppingListToReconfigure(sl.getOid())
                            .setSource(sourceOnDemandMarketTierOid)
                            .addAllCommodityToReconfigure(missingCommodities);
                    if (!smaMatch.getVirtualMachine().getGroupName().equals(SMAUtils.NO_GROUP_ID)) {
                        reconfigureTO.setScalingGroupId(smaMatch.getVirtualMachine()
                                .getGroupName());
                    }
                    ActionTO actionTO = ActionTO.newBuilder().setReconfigure(reconfigureTO)
                            .setImportance(0)
                            .setIsNotExecutable(false).build();
                    smaActions.add(actionTO);
                } else {
                    MoveExplanation.Builder moveExplanation = MoveExplanation.newBuilder();
                    if (missingCommodities.isEmpty()) {
                        moveExplanation.setCongestion(Congestion.getDefaultInstance());
                    } else {
                        // if there is a missing commodity then it is compliance action.
                        moveExplanation.setCompliance(Compliance.newBuilder()
                                .addAllMissingCommodities(missingCommodities));
                    }
                    MoveTO.Builder moveTO = MoveTO.newBuilder()
                            .setDestination(destinationOnDemandMarketTierOid)
                            .setSource(sourceOnDemandMarketTierOid)
                            .setShoppingListToMove(sl.getOid())
                            .setMoveExplanation(moveExplanation)
                            .setMoveContext(Context.newBuilder()
                                    .setRegionId(outputContext.getContext().getRegionId())
                                    .setZoneId(smaMatch.getVirtualMachine().getZoneId()));
                    if (smaMatch.getDiscountedCoupons() > 0) {
                        // This is just a dummy piece of code because this is used in
                        // one place to figure out if the vm is moving to a RI.
                        // ActionInterpreter.createChangeProviders while computing isAccountingAction.
                        moveTO.setCouponId(destinationOnDemandMarketTierOid)
                                .setCouponDiscount(1.0);
                    }
                    if (!smaMatch.getVirtualMachine().getGroupName().equals(SMAUtils.NO_GROUP_ID)) {
                        moveTO.setScalingGroupId(smaMatch.getVirtualMachine().getGroupName());
                    }
                    ActionTO actionTO = ActionTO.newBuilder().setMove(moveTO)
                            .setImportance(0)
                            .setIsNotExecutable(false).build();
                    smaActions.add(actionTO);
                }
            }
        }
    }

}
