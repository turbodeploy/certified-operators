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

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMACloudCostCalculator;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Compliance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Congestion;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureConsumerTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
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

    private SMACloudCostCalculator smaCloudCostCalculator = null;

    public SMAOutput getSmaOutput() {
        return smaOutput;
    }

    public SMACloudCostCalculator getSmaCloudCostCalculator() {
        return smaCloudCostCalculator;
    }

    public void setSmaCloudCostCalculator(SMACloudCostCalculator smaCloudCostCalculator) {
        this.smaCloudCostCalculator = smaCloudCostCalculator;
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
        updateProjectedTraderWithSMA();
        // update smaActions.
        updateActionsWithSMA();
    }

    /**
     * Update the projectedTraderDTOs with the results of SMA (stable marriage algorithm).
     * Update the supplier of the compute shopping list of cloud vms. Add coupon
     * commodity to the compute shopping list and update quantity appropriately.
     */
    private void updateProjectedTraderWithSMA() {
        for (SMAOutputContext outputContext : getSmaOutput().getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                long vmOid = smaMatch.getVirtualMachine().getOid();
                TraderTO projectedVmTraderTO =
                        oidToProjectedTraderMap.get(vmOid);
                if (projectedVmTraderTO == null) {
                    // do not try updating an entity that was removed
                    continue;
                }
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
                if (smaMatch.getDiscountedCoupons().getValueCase() == ValueCase.COUPONS &&
                        CommitmentAmountCalculator.isPositive(smaMatch.getDiscountedCoupons(), SMAUtils.EPSILON)) {
                    final ReservedInstanceData riData = converter.getCloudTc().getRiDataById(
                            smaMatch.getReservedInstance().getOid());
                    if (riData == null) {
                        logger.error("Reserved Instance: {} " +
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
                if (smaMatch.getDiscountedCoupons().getValueCase() == ValueCase.COUPONS &&
                        CommitmentAmountCalculator.isPositive(smaMatch.getDiscountedCoupons(), SMAUtils.EPSILON)) {
                    // add the coupon commodity to the compute shopping list.
                    Optional<CommodityBoughtTO> coupon =
                            converter.createCouponCommodityBoughtForCloudEntity(
                                    destinationRIDiscountedMarketTierOid,
                                    vmOid);
                    if (coupon.isPresent()) {
                        CommodityBoughtTO.Builder couponCommodity =
                                CommodityBoughtTO.newBuilder(coupon.get());
                        couponCommodity.setQuantity((float)smaMatch.getDiscountedCoupons().getCoupons());
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
        Map<Long, MinimalOriginalTrader> originalTraderTOMap = converter.getUnmodifiableOidToOriginalTraderTOMap();
        for (SMAOutputContext outputContext : getSmaOutput().getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                long vmOid = smaMatch.getVirtualMachine().getOid();
                TraderTO projectedVmTraderTO =
                        oidToProjectedTraderMap.get(vmOid);
                if (projectedVmTraderTO == null) {
                    // do not try updating an entity that was removed
                    continue;
                }
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
                //sourceRIDiscountedMarketTierOid can be ondemand if source wasnt RI to begin with.
                Long sourceRIDiscountedMarketTierOid = sourceOnDemandMarketTierOid;
                // If SMAMatch is already in an RI, then create a corresponding
                // RiDiscountedMarketTier and use it instead of the OnDemandMarketTier.
                if (smaMatch.getVirtualMachine().getCurrentRI() != null &&
                        smaMatch.getVirtualMachine().getCurrentRICoverage().getValueCase() == ValueCase.COUPONS) {
                    final ReservedInstanceData riData = converter.getCloudTc().getRiDataById(
                            smaMatch.getVirtualMachine().getCurrentRI().getOid());
                    if (riData != null) {
                        sourceRIDiscountedMarketTierOid = converter.getCloudTc()
                                .getRIDiscountedMarketTierIDFromRIData(riData);
                    } else {
                        logger.error("Reserved Instance: {} " +
                                        "not found in scope.",
                                smaMatch.getVirtualMachine().getCurrentRI().getOid());
                    }
                }


                int indexOfComputeShoppingList =
                        converter.getCloudTc().getIndexOfSlSuppliedByPrimaryTier(projectedVmTraderTO);
                if (indexOfComputeShoppingList < 0) {
                    continue;
                }
                ShoppingListTO sl =
                        projectedVmTraderTO.getShoppingListsList()
                                .get(indexOfComputeShoppingList);

                // If the shopping list is not part of CloudVmComputeShoppingListIDs because
                // the vm is uncontrollable dont generate any action for it.
                if (!converter.getCloudVmComputeShoppingListIDs().contains(sl.getOid())) {
                    continue;
                }

                MinimalOriginalTrader sourceTraderTO = originalTraderTOMap.get(sourceOnDemandMarketTierOid);

                // Compare the comm sold by the current supplier and comm bought by the VM and
                // find the missing commodities.
                List<Integer> missingCommodities = new ArrayList<>();
                for (int i = 0; i < sl.getCommoditiesBoughtList().size(); i++) {
                    CommodityBoughtTO commBought = sl.getCommoditiesBoughtList().get(i);
                    boolean commditySatisfied = false;
                    for (int commSoldType : sourceTraderTO.getSoldCommTypes()) {
                        if (commBought.getSpecification().getType() == commSoldType) {
                            commditySatisfied = true;
                            break;
                        }
                    }
                    if (!commditySatisfied) {
                        missingCommodities.add(commBought.getSpecification().getType());
                    }
                }
                if (!missingCommodities.isEmpty()
                        && smaMatch.getVirtualMachine().isEmptyProviderList()) {
                    // Reconfigure action. There is no provider that can provides all the
                    // commodities the VM is shopping.
                    ReconfigureTO.Builder reconfigureTO = ReconfigureTO.newBuilder();
                    reconfigureTO.setConsumer(ReconfigureConsumerTO.newBuilder()
                            .setShoppingListToReconfigure(sl.getOid())
                            .setSource(sourceOnDemandMarketTierOid))
                            .addAllCommodityToReconfigure(missingCommodities);
                    if (!smaMatch.getVirtualMachine().getGroupName().equals(SMAUtils.NO_GROUP_ID)) {
                        reconfigureTO.setScalingGroupId(smaMatch.getVirtualMachine()
                                .getGroupName());
                    }
                    ActionTO actionTO = ActionTO.newBuilder().setReconfigure(reconfigureTO)
                            .setImportance(0)
                            .setIsNotExecutable(false).build();
                    smaActions.add(actionTO);
                }
                if (smaMatch.getVirtualMachine()
                        .getCurrentTemplate().getOid() == smaMatch.getTemplate().getOid()
                        && CommitmentAmountCalculator.isSame(smaMatch.getVirtualMachine().getCurrentRICoverage(),
                        smaMatch.getDiscountedCoupons(), 0.01d)) {
                    continue;
                }
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
                        .setSource(sourceRIDiscountedMarketTierOid)
                        .setShoppingListToMove(sl.getOid())
                        .setMoveExplanation(moveExplanation)
                        .setMoveContext(Context.newBuilder()
                                .setRegionId(outputContext.getContext().getRegionId())
                                .setZoneId(smaMatch.getVirtualMachine().getZoneId()));
                if (CommitmentAmountCalculator.isPositive(smaMatch.getDiscountedCoupons(), SMAUtils.EPSILON)) {
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
