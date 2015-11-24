package com.vmturbo.platform.analysis.ese;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

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
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size();
                        boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);

            // add quantities bought by buyer, to quantities already used at seller
            final double capacity = commSold.getCapacity();
            final double utilization = isCurrentSupplier ? commSold.getUtilization()
                            : (quantities[boughtIndex] + commSold.getQuantity()) / capacity;
            final double peakUtilization = isCurrentSupplier ? commSold.getPeakUtilization()
                            : (peakQuantities[boughtIndex] + commSold.getPeakQuantity()) / capacity;
            final double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();

            // calculate the price per unit for quantity and peak quantity
            final PriceFunction pf = commSold.getSettings().getPriceFunction();
            final double priceUsed = pf.unitPrice(utilization, utilUpperBound);
            final double pricePeak = pf.unitPeakPrice(utilization, peakUtilization, utilUpperBound);

            // calculate quote
            // TODO: consider removing 100 from everywhere in the code. It is redundant
            // TODO: decide what to do if peakQuantity is less than quantity
            quote += 100.0 * (quantities[boughtIndex] * priceUsed
                              + Math.max(0.0, peakQuantities[boughtIndex] - quantities[boughtIndex]) * pricePeak)
                            / capacity;

        }
        return quote;
    }

    /**
     * Create and return a placement {@link RecommendationItem recommendation item}.
     *
     * @param buyerParticipation - the buyer participation to be moved
     * @param economy - the economy where the Buyer belongs
     * @param newSupplier - the new Seller where we are going to move the Buyer
     */
    public static RecommendationItem recommendPlace(BuyerParticipation buyerParticipation,
                                                    Economy economy, Market market, Trader newSupplier) {
        Trader buyer = economy.getBuyer(buyerParticipation);
        Trader currentSupplier = economy.getSupplier(buyerParticipation);
        String reason = "";

        // if there is no current supplier, recommend to start buyer in new supplier
        if (currentSupplier == null) {
            reason = "Buyer is currently not placed in any supplier selling " +
                     market.getBasket().toString();
            return new RecommendationItem("Start ", reason, buyer, currentSupplier, newSupplier);
        }

        if (market.getSellers().contains(currentSupplier)) {
            reason = "bla bla bla";
        } else { // current supplier is not part of the market, i.e. not selling what the buyer wants
            reason = "Current supplier is not selling one or more of the following: "
                            + market.getBasket().toString();
        }
        return new RecommendationItem("Move ", reason, buyer, currentSupplier, newSupplier);
    }

    /**
     * Create and return a reconfigure {@link RecommendationItem recommendation item}.
     *
     * @param buyerParticipation - the buyer participation to be moved
     * @param market - the market where the buyer participation is shopping
     * @param economy - the economy where the Buyer belongs
     */
    public static RecommendationItem recommendReconfigure(BuyerParticipation buyerParticipation,
                                                          Market market, Economy economy) {
        Trader buyer = economy.getBuyer(buyerParticipation);
        String reason = "There are no suppliers selling: " + market.getBasket().toString();
        return (new RecommendationItem("Reconfigure ", reason, buyer, null, null));
    }
}
