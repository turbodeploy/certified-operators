package com.vmturbo.platform.analysis.ese;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * EdeCommon contains a number of methods that are common across decisions algorithms in the engine.
 *
 */
public final class EdeCommon {

    /**
     * Calculate the quote of a seller for a basket bought by a buyer.
     *
     * @param buyerParticipation - participation buying specific quantities of the basket commodities
     * @param basket - the basket bought by the buyer participation
     * @param currentSupplier - the current supplier for the buyerParticipation. Can be null.
     * @param seller - the seller that will give the quote
     */
    @Pure
    public static double quote(@NonNull BuyerParticipation buyerParticipation, @NonNull Basket basket,
                    Trader currentSupplier, @NonNull Trader seller) {
        //TODO (Apostolos): we have not dealt with equivalent commodities
        double quote = 0.0;
        boolean isCurrentSupplier = seller == currentSupplier;

        // get the quantity and peak quantity to buy for each commodity of the basket
        final double[] quantities = buyerParticipation.getQuantities();
        final double[] peakQuantities = buyerParticipation.getPeakQuantities();

        // go over all commodities in basket
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size(); boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);

            // add quantities bought by buyer, to quantities already used at seller
            final double effectiveCapacity = commSold.getEffectiveCapacity();
            final double newQuantity = isCurrentSupplier ? commSold.getQuantity()
                                     : quantities[boughtIndex] + commSold.getQuantity();
            final double newPeakQuantity = isCurrentSupplier ? commSold.getPeakQuantity()
                                         : peakQuantities[boughtIndex] + commSold.getPeakQuantity();
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
            quote += (quantities[boughtIndex]*priceUsed + (excessQuantity > 0 ? excessQuantity*pricePeak : 0))
                   / commSold.getCapacity();
        }
        return quote;
    }

}
