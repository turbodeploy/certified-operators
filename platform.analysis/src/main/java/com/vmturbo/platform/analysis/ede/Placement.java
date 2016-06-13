package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

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
     */
    public static @NonNull List<@NonNull Action> placementDecisions(@NonNull Economy economy) {

        @NonNull List<Action> actions = new ArrayList<>();

        // iterate over all markets, i.e., all sets of providers selling a specific basket
        for (Market market : economy.getMarkets()) {

            // iterate over all buyers in this market
            for (@NonNull ShoppingList shoppingList : market.getBuyers()) {

                // if there are no sellers in the market, the buyer is misconfigured
                final @NonNull List<@NonNull Trader> sellers = market.getActiveSellers();
                if (!shoppingList.isMovable())
                    continue;
                if (sellers.isEmpty()) {
                    actions.add(new Reconfigure(economy, shoppingList).take());
                    continue;
                }

                // get cheapest quote
                final QuoteMinimizer minimizer =
                    (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                        ? sellers.stream() : sellers.parallelStream())
                    .collect(()->new QuoteMinimizer(economy,shoppingList),
                        QuoteMinimizer::accept, QuoteMinimizer::combine);

                final double cheapestQuote = minimizer.getBestQuote();
                final Trader cheapestSeller = minimizer.getBestSeller();
                final double currentQuote = minimizer.getCurrentQuote();

                // move, and update economy and state
                // TODO: decide how much cheaper the new supplier should be to decide to move
                if (currentQuote > cheapestQuote) { // + market.getBasket().size() * 2.0) {
                    //TODO (Apostolos): use economy.getSettings().getQuoteFactor() above
                    // create recommendation, add it to the result list and  update the economy to
                    // reflect the decision
                    actions.add(new Move(economy,shoppingList,cheapestSeller).take());
                }
            }
        }
        return actions;
    }

}
