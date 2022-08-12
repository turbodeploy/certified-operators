package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * OverProvisionedPriceFunction.
 */
public class OverProvisionedPriceFunction implements PriceFunction {

    /*
     * Weight assigned to the priceFunction.
     */
    double weight_;

    /*
     * Constant price to be returned as part of the function below the first step.
     */
    double constant_;

    /*
     * First junction point where the first step occurs separating different pricing used above and
     * below.
     */
    double stepOne_;

    /*
     * Second junction point where the second step occurs separating different pricing used above
     * and below.
     */
    double stepTwo_;

    /**
     * Creates OverProvisionedPriceFunction used for risk related commodities where pricing can be
     * dependent on the consumer or provider.
     *
     * @param weight - price weight.
     * @param constant - constant to be returned under stepOne.
     * @param stepOne - first utilization point where price function switches from using some
     *                  pricing behavior to another.
     * @param stepTwo - second utilization point where price function switches from using some
     *                  pricing behavior to another.
     */
    OverProvisionedPriceFunction(double weight, double constant, double stepOne, double stepTwo) {
        weight_ = weight;
        constant_ = constant;
        stepOne_ = stepOne;
        stepTwo_ = stepTwo;
    }

    /**
     * OverProvisionedPriceFunction used to account for risk related over-provisioning commodities
     * behavior.
     *
     * <p>For placed entities:
     * a: Under stepOne, we return the constant.
     * b: Above stepOne and below stepTwo, we return a price based on increasing pricing compared to
     *    util similar to SWPF.
     * c: Above stepTwo we return infinite.</p>
     *
     * <p>For unplaced entities:
     * We try to get the resold commodity from the seller's provider and make sure the pricing is
     * finite.
     * If finite:
     * a: Under stepOne, we return the constant.
     * b: Above stepOne and below stepTwo, we return a price based on increasing pricing compared to
     *    util similar to SWPF.
     * c: Above stepTwo we return infinite.</p>
     *
     * <p>For seller ROI:
     * a: Under stepOne, we return the constant.
     * b: Above stepOne, we return the price from the seller's provider or infinite if we can't
     *     find the reseller or resold commodity.</p>
     *
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     *
     * @return the price that will be charged.
     */
    @Override
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller,
        CommoditySold cs, UnmodifiableEconomy e) {
        // seller's ROI (provision/suspension).
        if (shoppingList == null) {
            return sellerPricing(normalizedUtilization, shoppingList, seller, cs, e);
        // Placement entities with no supplier for (new workloads such as add workload plans and
        // reservations).
        } else if (shoppingList.getSupplier() == null) {
            return unplacedConsumerPricing(normalizedUtilization, shoppingList, seller, cs, e);
        // Placement entities with supplier (existing workloads).
        } else {
            return placedConsumerPricing(normalizedUtilization, shoppingList, seller, cs, e);
        }
    }

    /**
     * The mechanism to update a {@link PriceFunction} with any new weight specified.
     * @param weight is the weight on the new PriceFunction.
     * @return this {@link PriceFunction}.
     */
    @Override
    public PriceFunction updatePriceFunctionWithWeight(double weight) {
        return PriceFunctionFactory
                .createOverProvisionedPriceFunction(weight, constant_, stepOne_, stepTwo_);
    }

    @Override
    public double[] getParams() {
        return new double[] { weight_, constant_, stepOne_, stepTwo_ };
    }

    /**
     * Pricing behavior for when util is in between the 2 specified steps.
     * This pricing behavior is similar to SWPF, however, it is normalized based on the
     * distance between the two steps.
     * Example 1:
     * stepOne_ - 1.0
     * stepTwo_ - 2.0
     * util - 1.5
     * normalizedUtil is now 0.5 and price is 4, similar to passing 0.5 to SWPF.
     * Example 2:
     * stepOne_ - 1.0
     * stepTwo_ - 3.0
     * util - 1.5
     * normalizedUtil is now 0.25 and price is 1.8, similar to passing 0.25 to SWPF.
     *
     * @param util - the current util on the seller.
     * @return the price
     */
    private double pricingBetweenSteps(double util) {
        double normalizedUtil = (util - stepOne_) / (stepTwo_ - stepOne_);
        return Math.min(weight_ / ((1.0f - normalizedUtil) * (1.0f - normalizedUtil)),
                    PriceFunctionFactory.MAX_UNIT_PRICE);
    }

    /**
     * Pricing for seller ROI.
     * Under stepOne, we return the constant.
     * Above stepOne, we return the price from the seller's provider or infinite if we can't
     * find the reseller or resold commodity. If we don't find the seller's supplier then return
     * infinite.
     *
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     *
     * @return the price that will be charged.
     */
    private double sellerPricing(double normalizedUtilization, ShoppingList shoppingList,
        Trader seller, CommoditySold cs, UnmodifiableEconomy e) {
        if (normalizedUtilization >= stepOne_) {
            CommoditySpecification commSpec =
                seller.getBasketSold().get(seller.getCommoditiesSold().indexOf(cs));
            // Obtain commodity sold by seller's supplier.
            Pair<CommoditySold, Trader> rawMaterial =
                RawMaterials.findResoldRawMaterialOnSeller((Economy)e, seller, commSpec);
            CommoditySold commSoldBySeller = rawMaterial.first;
            // Return infinite price for seller if no supplier is found since
            // this seller's util is above step one.
            if (commSoldBySeller == null || rawMaterial.second == seller) {
                return Double.POSITIVE_INFINITY;
            }
            PriceFunction pf = commSoldBySeller.getSettings().getPriceFunction();
            // Return price from seller's supplier.
            return pf.unitPrice(commSoldBySeller.getQuantity()
                / commSoldBySeller.getEffectiveCapacity(), shoppingList, rawMaterial.second,
                commSoldBySeller, e);
        } else {
            return constant_ * weight_;
        }
    }

    /**
     * Pricing for unplaced consumer.
     * We try to get the resold commodity from the seller's provider and make sure the pricing is
     * finite so that we can fit the consumer on the seller's supplier as well.
     * If we don't find the seller's supplier, then we return infinite above stepOne else constant.
     * If we do find the seller's supplier then:
     *   If seller's supplier's price is finite:
     *   a: seller util is under stepOne, we return the constant.
     *   b: seller util is above stepOne and below stepTwo, we return a price based on increasing
     *      pricing compared to util similar to SWPF.
     *   c: seller util is above stepTwo we return infinite.
     *   If seller's supplier's price is infinite then we return infinite since the consumer can't
     *   fit on the seller's supplier.
     *
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     *
     * @return the price that will be charged.
     */
    private double unplacedConsumerPricing(double normalizedUtilization, ShoppingList shoppingList,
        Trader seller, CommoditySold cs, UnmodifiableEconomy e) {
        CommoditySpecification commSpec =
            seller.getBasketSold().get(seller.getCommoditiesSold().indexOf(cs));
        // Obtain commodity sold by seller's supplier.
        Pair<CommoditySold, Trader> rawMaterial =
            RawMaterials.findResoldRawMaterialOnSeller((Economy)e, seller, commSpec);
        CommoditySold commSoldBySeller = rawMaterial.first;
        // Return infinite price for this unplaced consumer if no supplier is found for the
        // seller if this seller's util is above step one or constant if below step one.
        if (commSoldBySeller == null || rawMaterial.second == seller) {
            if (normalizedUtilization > stepOne_) {
                return Double.POSITIVE_INFINITY;
            } else {
                return constant_ * weight_;
            }
        }
        PriceFunction pf = commSoldBySeller.getSettings().getPriceFunction();
        double resellerPrice = pf.unitPrice(commSoldBySeller.getQuantity()
            / commSoldBySeller.getEffectiveCapacity(), shoppingList, rawMaterial.second,
            commSoldBySeller, e);
        // If seller's supplier price is finite, then we can fit this consumer on this seller
        // provided that the util is below step two.
        if (Double.isFinite(resellerPrice)) {
            if (normalizedUtilization <= stepOne_) {
                return constant_ * weight_;
            } else {
                return normalizedUtilization > stepTwo_
                    ? Double.POSITIVE_INFINITY
                    : pricingBetweenSteps(normalizedUtilization);
            }
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    /**
     * Pricing for placed consumer.
     * Since it's an existing consumer, it doesn't check the seller's supplier pricing so it won't
     * prevent optimal placement if seller's supplier is over utilized.
     * a: Under stepOne, we return the constant.
     * b: Above stepOne and below stepTwo, we return a price based on increasing pricing compared to
     *    util similar to SWPF.
     * c: Above stepTwo we return infinite.
     *
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     *
     * @return the price that will be charged.
     */
    private double placedConsumerPricing(double normalizedUtilization, ShoppingList shoppingList,
        Trader seller, CommoditySold cs, UnmodifiableEconomy e) {
        if (normalizedUtilization <= stepOne_) {
            return constant_ * weight_;
        } else {
            return normalizedUtilization > stepTwo_
                ? Double.POSITIVE_INFINITY
                : pricingBetweenSteps(normalizedUtilization);
        }
    }
}
