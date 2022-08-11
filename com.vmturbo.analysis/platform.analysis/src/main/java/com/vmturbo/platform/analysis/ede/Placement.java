package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Sets;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * Contains static methods related to optimizing the placement of {@link Trader}s in an
 * {@link Economy}.
 */
public class Placement {

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     * With a preference given to the shoppingLists in the list "sls"
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param sls - list of shoppingLists that denotes buyers that are to shop before the others
     */
    public static @NonNull List<@NonNull Action> placementDecisions(@NonNull Economy economy,
                    List<ShoppingList> sls) {

        @NonNull List<Action> actions = new ArrayList<>();

        // run placement on specific buyers in a selected market
        for (ShoppingList sl : sls) {
            actions.addAll(generatePlacementDecisions(economy, sl));
        }
        // iterate over all markets, i.e., all sets of providers selling a specific basket
        for (Market market : economy.getMarkets()) {
            // iterate over all buyers in this market that havnt already shopped
            for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                if (sls.contains(shoppingList)) {
                    continue;
                }
                actions.addAll(generatePlacementDecisions(economy, shoppingList));
            }
        }
        return actions;
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     */
    public static @NonNull List<@NonNull Action> placementDecisions(@NonNull Economy economy) {
        return Placement.placementDecisions(economy, new ArrayList<>());
    }

    /**
     * Returns a list of recommendations to optimize the placement of a trader with a
     * particular shoppingList
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param shoppingList - The {@link ShoppingList} for which we try to find the best destination
     */
    public static @NonNull List<@NonNull Action> generatePlacementDecisions(@NonNull Economy economy
                                , ShoppingList shoppingList) {
        @NonNull List<Action> actions = new ArrayList<>();
        // if there are no sellers in the market, the buyer is misconfigured
        final @NonNull List<@NonNull Trader> sellers = economy.getMarket(shoppingList).getActiveSellers();
        if (!shoppingList.isMovable())
            return actions;
        if (sellers.isEmpty()) {
            actions.add(new Reconfigure(economy, shoppingList).take());
            return actions;
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
        if (cheapestQuote < currentQuote * economy.getSettings().getQuoteFactor()) {
            // create recommendation, add it to the result list and  update the economy to
            // reflect the decision
            actions.add(new Move(economy,shoppingList,cheapestSeller).take().setImportance(
                            currentQuote - cheapestQuote));
        }
        return actions;
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     */
    public static @NonNull List<@NonNull Action> shopTogetherDecisions(@NonNull Economy economy) {
        return Placement.shopTogetherDecisions(economy, new ArrayList<>());
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     * allowing shoppingLists in "specialShoppingLists" to shop before the rest of the shoppingLists
     * in the economy
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param specialShoppingLists - list of shoppingLists that denotes buyers that
     * are to shop before the others
     */
    public static @NonNull List<@NonNull Action> shopTogetherDecisions(@NonNull Economy economy,
                                List<ShoppingList> specialShoppingLists) {
        @NonNull List<@NonNull Action> output = new ArrayList<>();

        List<Trader> specialTraders = new ArrayList<>();
        if (!specialShoppingLists.isEmpty()) {
            specialShoppingLists.forEach(sl -> specialTraders.add(sl.getBuyer()));
            // place selected list of buyers
            output.addAll(generateShopTogetherDecisions(economy, specialTraders));
            output.addAll(generateShopTogetherDecisions(economy, economy.getTraders().stream()
                            .filter(trader -> !specialTraders.contains(trader)).collect(Collectors.toList())));
        } else {
            output.addAll(generateShopTogetherDecisions(economy, economy.getTraders()));
        }
        return output;
    }

    /**
     * Returns a list of recommendations to optimize the placement of a trader buying
     * shoppingLists in specific markets
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param traders - list of {@link Trader} that are to be placed before the rest present in the {@link Economy}
     */
    public static @NonNull List<@NonNull Action> generateShopTogetherDecisions(@NonNull Economy
                    economy, List<Trader> traders) {
        @NonNull List<@NonNull Action> output = new ArrayList<>();

        for (@NonNull @ReadOnly Trader buyer : traders) {
            final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>> entries =
                economy.getMarketsAsBuyer(buyer).entrySet();
            final @NonNull @ReadOnly List<Entry<@NonNull ShoppingList, @NonNull Market>> movableEntries =
                entries.stream().filter(entry->entry.getKey().isMovable()).collect(Collectors.toList());

            // Only optimize active traders that are at least partially movable.
            if (buyer.getState().isActive() && !movableEntries.isEmpty()) {
                // Find the set of k-partite cliques where the trader can potentially be placed.
                Set<Integer> commonCliques = entries.stream()
                    .map(entry->entry.getKey().isMovable() // if shopping list is movable
                        ? entry.getValue().getCliques().keySet() // use the cliques of the market
                        : (new TreeSet<>(entry.getKey().getSupplier() != null // else if shopping list is placed
                                        ? entry.getKey().getSupplier().getCliques() // use clique that contain supplier
                                        : Arrays.asList())) // else there is no valid placement.
                    ).reduce(Sets::intersection).get();

                // Compute current total quote.
                final double currentTotalQuote = movableEntries.stream().mapToDouble(entry ->
                    entry.getKey().getSupplier() == null // if unplaced or incorrectly placed
                        || !entry.getValue().getBasket().isSatisfiedBy(entry.getKey().getSupplier().getBasketSold())
                    ? Double.POSITIVE_INFINITY // current total is infinite
                    : EdeCommon.quote(economy, entry.getKey(), entry.getKey().getSupplier(),
                                      Double.POSITIVE_INFINITY, false)[0]
                ).sum(); // TODO: break early...

                // Compute the best total quote.
                CliqueMinimizer minimizer = commonCliques.stream().collect(
                    ()->new CliqueMinimizer(economy,movableEntries), CliqueMinimizer::accept, CliqueMinimizer::combine);

                // If buyer can improve its position, output action.
                if (minimizer.getBestTotalQuote() < currentTotalQuote * economy.getSettings().getQuoteFactor()) {
                    List<ShoppingList> shoppingLists = movableEntries.stream()
                        .map(Entry::getKey).collect(Collectors.toList());
                    output.add(new CompoundMove(economy, shoppingLists, minimizer.getBestSellers())
                                   .take().setImportance(currentTotalQuote - minimizer.getBestTotalQuote()));
                }
            }
        }
        return output;
    }

} // end Placement class
