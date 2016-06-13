package com.vmturbo.platform.analysis.ede;

import java.util.function.DoubleBinaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * EdeCommon contains a number of methods that are common across decisions algorithms in the engine.
 *
 */
public final class EdeCommon {

    /**
     * Calculate the quote of a seller for a basket bought by a buyer.
     *
     * @param economy - economy in which the market and traders are a part of.
     * @param shoppingList - shopping list containing specific quantities of the basket commodities
     * @param seller - the seller that will give the quote
     */
    @Pure
    public static double quote(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList,
            @NonNull Trader seller) {
        //TODO (Apostolos): we have not dealt with equivalent commodities
        double quote = 0.0;
        Basket basket = shoppingList.getBasket();
        // go over all commodities in basket
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size(); boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }

            // calculate quote
            // TODO: decide what to do if peakQuantity is less than quantity
            quote += computeCommodityCost(economy, shoppingList, seller, soldIndex, boughtIndex);
        }
        return quote;
    }

    /**
     * Calculate the cost of a commodity bought by a consumer in a market that sells a particular basket.
     *
     * @param economy - economy in which the market and traders are a part of.
     * @param shoppingList - shopping list containing specific quantities of the basket commodities
     * @param seller - the seller that will give the quote
     * @param soldIndex - this is the position of the commoditySold in the list of soldCommodities
     * @param boughtIndex - this is the position of the commodityBought in the list of boughtCommodities
     */
    @Pure
    public static double computeCommodityCost(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList,
                                              @NonNull Trader seller, int soldIndex, int boughtIndex) {

        boolean isCurrentSupplier = seller == shoppingList.getSupplier();

        // get the quantity and peak quantity to buy for each commodity of the basket
        final double[] quantities = shoppingList.getQuantities();
        final double[] peakQuantities = shoppingList.getPeakQuantities();

        final CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);
        final DoubleBinaryOperator addition = (sold, bought) -> sold + bought;

        // add quantities bought by buyer, to quantities already used at seller
        final double effectiveCapacity = commSold.getEffectiveCapacity();
        final double[] newQuantities = Move.updatedQuantities(economy, addition, quantities[boughtIndex],
            peakQuantities[boughtIndex], seller, soldIndex, true);
        final double newQuantity = isCurrentSupplier ? commSold.getQuantity() : newQuantities[0];
        final double newPeakQuantity = isCurrentSupplier ? commSold.getPeakQuantity() : newQuantities[1];
        final double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();
        final double excessQuantity = peakQuantities[boughtIndex] - quantities[boughtIndex];

        if (newQuantity > effectiveCapacity || newPeakQuantity > effectiveCapacity) {
            return Double.POSITIVE_INFINITY;
        }

        // calculate the price per unit for quantity and peak quantity
        final PriceFunction pf = commSold.getSettings().getPriceFunction();
        final double priceUsed = pf.unitPrice(newQuantity/effectiveCapacity);
        final double pricePeak = pf.unitPrice(Math.max(0, newPeakQuantity-newQuantity)
                               / (effectiveCapacity - utilUpperBound*newQuantity));

        // calculate quote
        // TODO: decide what to do if peakQuantity is less than quantity
        return (quantities[boughtIndex]*priceUsed + (excessQuantity > 0 ? excessQuantity*pricePeak : 0))
               / commSold.getCapacity();

    }

} // end EdeCommon class
