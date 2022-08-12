package com.vmturbo.platform.analysis.ede;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunction;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * EdeCommon contains a number of methods that are common across decisions algorithms in the engine.
 *
 */
public final class EdeCommon {

    @SuppressWarnings("unused")
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
     * @return A quote offered by the seller for the given shopping list.
     */
    @Pure
    public static MutableQuote quote(@NonNull UnmodifiableEconomy economy, @NonNull ShoppingList shoppingList,
                    @NonNull Trader seller, final double bestQuoteSoFar, boolean forTraderIncomeStmt) {
        return seller.getSettings().getQuoteFunction().calculateQuote(shoppingList, seller, bestQuoteSoFar,
                        forTraderIncomeStmt, (Economy)economy);
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
            return new double[] {0, 0, 0, 0};
        }

        double[] costCurrentMinMax = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                        Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
        boolean isCurrentSupplier = seller == shoppingList.getSupplier();
        final CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);
        final UpdatingFunction addition = UpdatingFunctionFactory.ADD_COMM;
        final double effectiveCapacity = commSold.getEffectiveCapacity();
        double newQuantity = commSold.getQuantity();
        double newPeakQuantity = commSold.getPeakQuantity();
        if (!isCurrentSupplier) {
            // add quantities bought by buyer, to quantities already used at seller
            final double[] newQuantities = Move.updatedQuantities(economy, addition, shoppingList,
                    boughtIndex, seller, soldIndex, false, null, true);
            newQuantity = newQuantities[0];
            newPeakQuantity = newQuantities[1];
        }
        final double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();
        final double excessQuantity = peakQuantities[boughtIndex] - boughtQnty;
        if (logger.isTraceEnabled()) {
            logger.trace("Seller {} commodity {} quantity would change from {} to {}, capacity {}",
                    seller.getDebugInfoNeverUseInCode(),
                    shoppingList.getBasket().getCommodityDebugInfoAt(boughtIndex),
                    commSold.getQuantity(),
                    newQuantity,
                    commSold.getCapacity());
        }

        // calculate the price per unit for quantity and peak quantity
        final PriceFunction pf = commSold.getSettings().getPriceFunction();
        final double priceUsed = pf.unitPrice(newQuantity / effectiveCapacity, shoppingList, seller,
                        commSold, economy);
        final double pricePeak = pf.unitPrice(
                        Math.max(0, newPeakQuantity - newQuantity)
                                        / (effectiveCapacity - utilUpperBound * newQuantity),
                        shoppingList, seller, commSold, economy);
        if (logger.isTraceEnabled()) {
            logger.trace("Seller {} commodity {} prices would now be (used) {} and (peak) {}",
                    seller.getDebugInfoNeverUseInCode(),
                    shoppingList.getBasket().getCommodityDebugInfoAt(boughtIndex),
                    priceUsed,
                    pricePeak);
        }

        double quoteUsed;
        double quotePeak;
        if (economy.getSettings().isFullPriceForQuote() && !forTraderIncomeStmt) {
            // In some cases we may want to consider the quote that an entity gets for a commodity
            // from its provider solely on the price for that commodity on the provider without considering
            // the consumption of the consumer as a percentage of the provider.
            // This is the case when there is a heterogeneous environment with providers of different
            // capacities for commodities such as having a Super Cluster where hosts in some clusters
            // can be be 5x bigger than hosts in other clusters. This would mean that the bigger
            // providers can give cheaper quotes for consumers even if they are more highly utilized
            // and lead to a discrepancy across providers' utilizations, making some highly utilized and some
            // low utilized.
            quoteUsed = priceUsed;
            quotePeak = excessQuantity > 0 ? pricePeak : 0;
            costCurrentMinMax[0] = quoteUsed + quotePeak;
        } else {
            // calculate quote
            // TODO: decide what to do if peakQuantity is less than quantity
            quoteUsed = (boughtQnty / effectiveCapacity) * priceUsed;
            quotePeak = excessQuantity > 0
                ? (excessQuantity / effectiveCapacity) * pricePeak : 0;
            costCurrentMinMax[0] = quoteUsed + quotePeak;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Buyer {} would pay (used) {} (peak) {} for commodity {}, bought qty {}",
                    shoppingList.getBuyer(), quoteUsed, quotePeak,
                    shoppingList.getBasket().getCommodityDebugInfoAt(boughtIndex),
                    boughtQnty);
        }

        if (forTraderIncomeStmt && costCurrentMinMax[0] != 0) {
            costCurrentMinMax[1] = pf.unitPrice(seller.getSettings().getMinDesiredUtil(), shoppingList, seller
                                                , commSold, economy)*(boughtQnty / effectiveCapacity);
            costCurrentMinMax[2] = pf.unitPrice(seller.getSettings().getMaxDesiredUtil(), shoppingList, seller
                                                , commSold, economy)*(boughtQnty / effectiveCapacity);
            costCurrentMinMax[3] = pf.unitPrice((seller.getSettings().getMinDesiredUtil()
                + seller.getSettings().getMaxDesiredUtil()) / 2, shoppingList, seller,
                    commSold, economy) * (boughtQnty / effectiveCapacity);
        } else {
            costCurrentMinMax[1] = costCurrentMinMax[2] = costCurrentMinMax[3] = 0;
        }
        return costCurrentMinMax;

    }

} // end EdeCommon class
