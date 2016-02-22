package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.EdeCommon.QuoteMinimizer;

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
    public static @NonNull List<@NonNull Action> placementDecisions(@NonNull Economy economy,
                    @NonNull List<@NonNull StateItem> state, long timeMiliSec) {

        @NonNull List<Action> actions = new ArrayList<>();

        // iterate over all markets, i.e., all sets of providers selling a specific basket
        for (Market market : economy.getMarkets()) {

            // iterate over all buyers in this market
            for (@NonNull BuyerParticipation buyerParticipation : market.getBuyers()) {

                // if there are no sellers in the market, the buyer is misconfigured
                final @NonNull List<@NonNull Trader> sellers = market.getActiveSellers();
                final @NonNull Trader buyer = buyerParticipation.getBuyer();
                if (!buyer.getSettings().isMovable())
                    continue;
                if (sellers.isEmpty()) {
                    actions.add(new Reconfigure(economy, buyerParticipation).take());
                    continue;
                }
                final @Nullable Trader currentSupplier = buyerParticipation.getSupplier();
                final int buyerIndex = buyer.getEconomyIndex();
                // check if the buyer cannot move, or cannot move out of current supplier
                if (timeMiliSec < state.get(buyerIndex).getMoveOnlyAfterThisTime()
                    || (currentSupplier != null
                        && timeMiliSec < state.get(currentSupplier.getEconomyIndex()).getMoveFromOnlyAfterThisTime())) {
                    continue;
                }

                // get cheapest quote
                final EdeCommon.QuoteMinimizer minimizer =
                    // TODO (Vaptistis): use economy.getSettings().parallelismThreshold().
                    // '256' was selected after finding the time-as-a-function-of-size curves for
                    // parallel and sequential execution and finding their intersection.
                    (sellers.size() < 256 ? sellers.stream() : sellers.parallelStream()).collect(
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
                    // create recommendation, add it to the result list and  update the economy to
                    // reflect the decision
                    actions.add(new Move(economy,buyerParticipation,cheapestSeller).take());
                    // update the state
                    int newSellerIndex = cheapestSeller.getEconomyIndex();
                    // TODO (Apostolos): use economy.getSettings().getPlacementInterval() below
                    long newTime = timeMiliSec + 1200000; // wait two 10 min intervals
                    state.get(newSellerIndex).setSuspendOnlyAfterThisTime(newTime);
                    state.get(buyerIndex).setMoveOnlyAfterThisTime(newTime);
                }
            }
        }
        return actions;
    }

}
