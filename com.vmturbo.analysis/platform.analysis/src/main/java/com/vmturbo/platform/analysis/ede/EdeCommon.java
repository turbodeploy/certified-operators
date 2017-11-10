package com.vmturbo.platform.analysis.ede;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;

/**
 * EdeCommon contains a number of methods that are common across decisions algorithms in the engine.
 *
 */
public final class EdeCommon {

    private static final Logger logger = LogManager.getLogger(EdeCommon.class);

    /**
     * Returns the quote offered by a seller for a shopping list bought by a buyer.
     *
     * @param economy - economy containing the shopping list and seller. It will not be modified.
     * @param shoppingList - shopping list containing specific quantities requested.
     * @param seller - the seller that will be queried for a quote.
     * @param bestQuoteSoFar - an optional best quote I may have gotten from other sellers.
     *                         Pass {@link Double#POSITIVE_INFINITY} to get the exact quote.
     * @param forTraderIncomeStmt - is true when we want to compute the min(max)desiredExp(rev)
     *
     * @return an array containing the quote offered by the seller for the given shopping list,
     *         or a part of it greater than or equal to bestQuoteSoFar iff the actual quote would
     *         exceed that value or if any commodity returns an INFINITE price, the minQuote and
     *         the maxQuote.
     */
    @Pure
    public static double[] quote(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList,
            @NonNull Trader seller, final double bestQuoteSoFar, boolean forTraderIncomeStmt) {
        // TODO (Apostolos): we have not dealt with equivalent commodities
        double quote[] = {0.0, 0.0, 0.0};
        Basket basket = shoppingList.getBasket();
        // go over all commodities in basket
        int boughtIndex = 0;
        for (int soldIndex = 0; boughtIndex < basket.size()
                && quote[0] < bestQuoteSoFar && Double.isFinite(quote[0]); boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }

            double[] tempQuote = computeCommodityCost(economy, shoppingList, seller, soldIndex, boughtIndex, forTraderIncomeStmt);
            quote[0] += tempQuote[0];
            if (forTraderIncomeStmt) {
                quote[1] += tempQuote[1];
                quote[2] += tempQuote[2];
            }
        }
        if (seller.isDebugEnabled()) {
            if(shoppingList.getSupplier() == seller) {
                if(Double.isInfinite(quote[0])) {
                    CommoditySpecification basketCommSpec = basket.get(boughtIndex-1);
                    logger.info("{" + shoppingList.getBuyer().getDebugInfoNeverUseInCode()
                                    + "} The commodity causing the infinite quote: " + basketCommSpec.getDebugInfoNeverUseInCode());
                }
            }
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
    public static double[] computeCommodityCost(@NonNull UnmodifiableEconomy economy,
                                    @NonNull ShoppingList shoppingList,
                                    @NonNull Trader seller, int soldIndex,
                                    int boughtIndex, boolean forTraderIncomeStmt) {

        // get the quantity and peak quantity to buy for each commodity of the basket
        final double[] quantities = shoppingList.getQuantities();
        final double[] peakQuantities = shoppingList.getPeakQuantities();
        double boughtQnty = quantities[boughtIndex];
        if (boughtQnty == 0) {
            return new double[] {0, 0, 0};
        }

        double[] costCurrentMinMax = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY};
        boolean isCurrentSupplier = seller == shoppingList.getSupplier();
        final CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);
        final FunctionalOperator addition = FunctionalOperatorUtil.ADD_COMM;

        // add quantities bought by buyer, to quantities already used at seller
        final double effectiveCapacity = commSold.getEffectiveCapacity();
        final double[] newQuantities = Move.updatedQuantities(economy, addition, shoppingList,
                        boughtIndex, seller, soldIndex, true, false);
        final double newQuantity = isCurrentSupplier ? commSold.getQuantity() : newQuantities[0];
        final double newPeakQuantity = isCurrentSupplier ? commSold.getPeakQuantity() : newQuantities[1];
        final double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();
        final double excessQuantity = peakQuantities[boughtIndex] - boughtQnty;

        // calculate the price per unit for quantity and peak quantity
        final PriceFunction pf = commSold.getSettings().getPriceFunction();
        final double priceUsed = pf.unitPrice(newQuantity/effectiveCapacity, seller, commSold, economy);
        final double pricePeak = pf.unitPrice(Math.max(0, newPeakQuantity-newQuantity)
                               / (effectiveCapacity - utilUpperBound*newQuantity), seller, commSold, economy);

        // calculate quote
        // TODO: decide what to do if peakQuantity is less than quantity
        costCurrentMinMax[0] = ((boughtQnty*priceUsed) + (excessQuantity > 0 ?
                        excessQuantity*pricePeak : 0)) / effectiveCapacity;

        if (forTraderIncomeStmt && costCurrentMinMax[0] != 0) {
            costCurrentMinMax[1] = pf.unitPrice(seller.getSettings().getMinDesiredUtil(), seller
                                                , commSold, economy)*boughtQnty / effectiveCapacity;
            costCurrentMinMax[2] = pf.unitPrice(seller.getSettings().getMaxDesiredUtil(), seller
                                                , commSold, economy)*boughtQnty / effectiveCapacity;
        } else {
            costCurrentMinMax[1] = costCurrentMinMax[2] = 0;
        }

        return costCurrentMinMax;

    }

} // end EdeCommon class
