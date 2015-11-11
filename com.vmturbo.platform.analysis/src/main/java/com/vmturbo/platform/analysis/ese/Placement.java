package com.vmturbo.platform.analysis.ese;


import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

public class Placement {

    /**
     * Return a list of recommendations to optimize the placement of all traders in the economy.
     *
     * @param economy - the economy whose traders placement we want to optimize
     * @param time - the time the placement decision algorithm is invoked
     */
    public static List<RecommendationItem> placementDecisions(Economy economy, List<StateItem> state,
                                                              long time) {

        List<RecommendationItem> recommendationList = new ArrayList<>();

        // iterate over all markets, i.e., all sets of providers selling a specific basket
        for (Market market : economy.getMarkets()) {

            // iterate over all buyers in this market
            for (@NonNull BuyerParticipation buyerParticipation : market.getBuyers()) {

                double quote;
                Trader buyer = economy.getBuyer(buyerParticipation);
                Trader currentSupplier = economy.getSupplier(buyerParticipation);
                int buyerIndex = economy.getTraders().indexOf(buyer);
                int currentSupplierIndex = currentSupplier != null
                                ? economy.getTraders().indexOf(currentSupplier) : -1;

                // check if the buyer can move, and can move out of current supplier
                if (currentSupplierIndex >= 0
                    && (time < state.get(currentSupplierIndex).getMoveFromOnlyAfterThisTime()
                        || time < state.get(buyerIndex).getMoveOnlyAfterThisTime())) {
                    continue;
                }

                // get cheapest quote
                double cheapestQuote = Double.MAX_VALUE;
                Trader cheapestSeller = null;
                double currentQuote = Double.MAX_VALUE;
                List<Trader> sellers = market.getSellers();
                if (sellers == null) {
                    EseCommon.recommendReconfigure(buyerParticipation, market, economy);
                    continue;
                }
                for (Trader seller : sellers) {
                    // if we cannot move to this seller and it is not the current supplier, skip it
                    int sellerIndex = economy.getTraders().indexOf(seller);
                    if (!seller.equals(currentSupplier)
                        && time < state.get(sellerIndex).getMoveToOnlyAfterThisTime()) {
                        continue;
                    }
                    quote = EseCommon.calcQuote(buyerParticipation, market, economy, seller);
                    if (seller.equals(currentSupplier)) {
                        currentQuote = quote;
                        if (cheapestQuote == Double.MAX_VALUE) {
                            cheapestQuote = quote;
                            cheapestSeller = seller;
                        }
                    }
                    // compare quote with minQuote
                    if (quote < cheapestQuote) {
                        cheapestQuote = quote;
                        cheapestSeller = seller;
                    }
                }

                // move, and update economy and state
                // TODO: decide how much cheaper the new supplier should be to decide to move
                if (currentQuote > cheapestQuote) { // + market.getBasket().size() * 2.0) {
                    //TODO (Apostolos): use economy.getSettings().getQuoteFactor() above
                    // create recommendation and add it to the result list
                    recommendationList.add(EseCommon.recommendPlace(buyerParticipation, economy,
                                                                    market, cheapestSeller));
                    // update the economy to reflect the decision
                    moveTraderAndUpdateQuantitiesSold(buyerParticipation, currentSupplier,
                                                      cheapestSeller, market.getBasket(), economy);
                    // update the state
                    int newSellerIndex = economy.getTraders().indexOf(cheapestSeller);
                    // TODO (Apostolos): use economy.getSettings().getPlacementInterval() below
                    long newTime = time + 1200000; // wait two 10 min intervals
                    state.get(newSellerIndex).setSuspendOnlyAfterThisTime(newTime);
                    state.get(buyerIndex).setMoveOnlyAfterThisTime(newTime);
                }
            }
        }
        return recommendationList;
    }

    /**
     * Change a buyer to become a customer of a newSupplier, and update the quantities sold by
     * the new and the current supplier.
     *
     * @param buyerParticipation - the buyer participation to be moved to the new supplier
     * @param currentSupplier - the current supplier of the buyer participation
     * @param newSupplier - the new supplier of the buyer participation
     * @param basket - the basket bought by the buyer participation
     * @param economy - the economy where the buyer and the suppliers belong
     */
    private static void moveTraderAndUpdateQuantitiesSold(BuyerParticipation buyerParticipation,
            Trader currentSupplier, Trader newSupplier, Basket basket, Economy economy) {

        // Add the buyer to the customers of newSupplier, and remove it from the customers of
        // currentSupplier
        economy.moveTrader(buyerParticipation, newSupplier);

        // Go over all commodities in the basket and update quantities in old and new Supplier
        int currCommSoldIndex = 0;
        int newCommSoldIndex = 0;
        double[] quantities = buyerParticipation.getQuantities();
        double[] peakQuantities = buyerParticipation.getPeakQuantities();
        for (int index = 0; index < basket.size(); index++) {
            CommoditySpecification basketCommSpec = basket.get(index);

            // Update current supplier
            if (currentSupplier != null) {
                // Find the corresponding commodity sold in the current supplier.
                while (!basketCommSpec.isSatisfiedBy(currentSupplier.getBasketSold().get(currCommSoldIndex))) {
                    currCommSoldIndex++;
                }
                CommoditySold currCommSold = currentSupplier.getCommoditiesSold().get(currCommSoldIndex);

                // adjust quantities at current supplier
                // TODO: the following is problematic for commodities such as latency, peaks, etc.
                // TODO: this is the reason for the Math.min, but it is not a good solution
                currCommSold.setQuantity(Math.max(0.0, currCommSold.getQuantity() - quantities[index]));
                currCommSold.setPeakQuantity(Math.max(0.0, currCommSold.getPeakQuantity() - peakQuantities[index]));
            }

            // Update new supplier
            // Find the corresponding commodity sold in the new supplier.
            while (!basketCommSpec.isSatisfiedBy(newSupplier.getBasketSold().get(newCommSoldIndex))) {
                newCommSoldIndex++;
            }
            CommoditySold newCommSold = newSupplier.getCommoditiesSold().get(newCommSoldIndex);

            // adjust quantities at new supplier
            newCommSold.setQuantity(newCommSold.getQuantity() + quantities[index]);
            newCommSold.setPeakQuantity(newCommSold.getPeakQuantity() + peakQuantities[index]);
        }
    }
}
