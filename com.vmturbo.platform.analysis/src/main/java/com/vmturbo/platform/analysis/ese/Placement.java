package com.vmturbo.platform.analysis.ese;


import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ese.EdeCommon.QuoteMinimizer;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

public class Placement {

    /**
     * Return a list of recommendations to optimize the placement of all traders in the economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param state - the state capturing all previous decisions of the economic decisions engine
     * @param timeMiliSec - time in Mili Seconds the placement decision algorithm is invoked
     *                      (typically since Jan. 1, 1970)
     */
    public static @NonNull List<@NonNull RecommendationItem> placementDecisions(@NonNull Economy economy,
                    @NonNull List<@NonNull StateItem> state, long timeMiliSec) {

        @NonNull List<RecommendationItem> recommendations = new ArrayList<>();

        // iterate over all markets, i.e., all sets of providers selling a specific basket
        for (Market market : economy.getMarkets()) {

            // iterate over all buyers in this market
            for (@NonNull BuyerParticipation buyerParticipation : market.getBuyers()) {

                // if there are no sellers in the market, the buyer is misconfigured
                final @NonNull Trader buyer = buyerParticipation.getBuyer();
                if (market.getSellers().isEmpty()) {
                    recommendations.add(new RecommendationItem(buyer, null, null, market));
                    continue;
                }
                final @Nullable Trader currentSupplier = buyerParticipation.getSupplier();
                final int buyerIndex = economy.getIndex(buyer);
                // check if the buyer cannot move, or cannot move out of current supplier
                if (timeMiliSec < state.get(buyerIndex).getMoveOnlyAfterThisTime()
                    || (currentSupplier != null
                        && timeMiliSec < state.get(economy.getIndex(currentSupplier)).getMoveFromOnlyAfterThisTime())) {
                    continue;
                }

                // get cheapest quote
                final EdeCommon.QuoteMinimizer minimizer = market.getSellers().stream().collect(
                    ()->new QuoteMinimizer(economy,state,timeMiliSec,buyerParticipation,
                                           market.getBasket(), currentSupplier),
                    QuoteMinimizer::accept, QuoteMinimizer::combine);

                final double cheapestQuote = minimizer.bestQuote();
                final Trader cheapestSeller = minimizer.bestSeller();
                final double currentQuote = minimizer.currentQuote();

                // move, and update economy and state
                // TODO: decide how much cheaper the new supplier should be to decide to move
                if (currentQuote > cheapestQuote) { // + market.getBasket().size() * 2.0) {
                    //TODO (Apostolos): use economy.getSettings().getQuoteFactor() above
                    // create recommendation and add it to the result list
                    recommendations.add(new RecommendationItem(buyer, currentSupplier, cheapestSeller, market));
                    // update the economy to reflect the decision
                    moveTraderAndUpdateQuantitiesSold(buyerParticipation, cheapestSeller,
                                    market.getBasket(), economy);
                    // update the state
                    int newSellerIndex = economy.getIndex(cheapestSeller);
                    // TODO (Apostolos): use economy.getSettings().getPlacementInterval() below
                    long newTime = timeMiliSec + 1200000; // wait two 10 min intervals
                    state.get(newSellerIndex).setSuspendOnlyAfterThisTime(newTime);
                    state.get(buyerIndex).setMoveOnlyAfterThisTime(newTime);
                }
            }
        }
        return recommendations;
    }

    /**
     * Change a buyer to become a customer of a newSupplier, and update the quantities sold by
     * the new and the current supplier.
     *
     * <p>
     *  The method assumes that the new supplier can satisfy the basket bought by the buyer
     *  participation, and is not checking for it. No such assumption is made for the current
     *  supplier.
     *  </p>
     *
     * <p>
     *  Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     *  actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param buyerParticipation - the buyer participation to be moved to the new supplier
     * @param newSupplier - the new supplier of the buyer participation
     * @param basket - the basket bought by the buyer participation
     * @param economy - the economy where the buyer and the suppliers belong
     */
    private static void moveTraderAndUpdateQuantitiesSold(BuyerParticipation buyerParticipation,
            @NonNull Trader newSupplier, Basket basket, Economy economy) {

        // Add the buyer to the customers of newSupplier, and remove it from the customers of
        // currentSupplier
        economy.moveTrader(buyerParticipation, newSupplier);

        // Go over all commodities in the basket and update quantities in old and new Supplier
        int currCommSoldIndex = 0;
        int newCommSoldIndex = 0;
        final double[] quantities = buyerParticipation.getQuantities();
        final double[] peakQuantities = buyerParticipation.getPeakQuantities();
        final Trader currSupplier = buyerParticipation.getSupplier();
        final Basket currBasketSold = currSupplier.getBasketSold();
        final int currSupplierBasketSize = currSupplier.getBasketSold().size();
        for (int index = 0; index < basket.size(); index++) {
            CommoditySpecification basketCommSpec = basket.get(index);

            // Update current supplier
            if (currSupplier != null) {
                // Find the corresponding commodity sold in the current supplier.
                int startIndex = currCommSoldIndex; // keep the start index for misconfigured current supplier
                while (currCommSoldIndex < currSupplierBasketSize
                                && !basketCommSpec.isSatisfiedBy(currBasketSold.get(currCommSoldIndex))) {
                    currCommSoldIndex++;
                }
                // handle a misconfigured current supplier that is not selling the commodity
                if (currCommSoldIndex == currSupplierBasketSize) {
                    currCommSoldIndex = startIndex;
                }
                final CommoditySold currCommSold = currSupplier.getCommoditiesSold().get(currCommSoldIndex);

                // adjust quantities at current supplier
                // TODO: the following is problematic for commodities such as latency, peaks, etc.
                // TODO: this is the reason for the Math.max, but it is not a good solution
                currCommSold.setQuantity(Math.max(0.0, currCommSold.getQuantity() - quantities[index]));
                currCommSold.setPeakQuantity(Math.max(0.0, currCommSold.getPeakQuantity() - peakQuantities[index]));
            }

            // Update new supplier
            // Find the corresponding commodity sold in the new supplier.
            while (!basketCommSpec.isSatisfiedBy(newSupplier.getBasketSold().get(newCommSoldIndex))) {
                newCommSoldIndex++;
            }
            final CommoditySold newCommSold = newSupplier.getCommoditiesSold().get(newCommSoldIndex);

            // adjust quantities at new supplier
            newCommSold.setQuantity(newCommSold.getQuantity() + quantities[index]);
            newCommSold.setPeakQuantity(newCommSold.getPeakQuantity() + peakQuantities[index]);
        }
    }
}
