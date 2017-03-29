package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.Sets;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;

/**
 * Contains static methods related to optimizing the placement of {@link Trader}s in an
 * {@link Economy}.
 */
public class Placement {

	static final Logger logger = Logger.getLogger(Placement.class);

    // the maximum number of placements to be 1000, when reaching this limit, we force stop
    // the placements. 1000 is a random number, it does not have any significant meaning.
    public static int MAX_NUM_PLACEMENT = 1000;
    public static int globalCounter = 0;

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
     * @param placeJustPassedBuyers - boolean to run placements only on the buyers passed
     */
    public static @NonNull List<@NonNull Action> placementDecisions(@NonNull Economy economy,
                    List<ShoppingList> sls, boolean placeJustPassedBuyers) {

        @NonNull List<Action> actions = prefPlacementDecisions(economy, sls);
        if (!placeJustPassedBuyers) {
            // iterate over all markets, i.e., all sets of providers selling a specific basket
            for (Market market : economy.getMarketsForPlacement()) {
                // iterate over all buyers in this market that havnt already shopped
                for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                    if (sls.contains(shoppingList)) {
                        continue;
                    }
                    actions.addAll(generatePlacementDecisions(economy, shoppingList));
                }
            }
        }
        return actions;
    }

    /**
     * Returns a list of recommendations to optimize placement of a set of shopping lists
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param sls - list of shoppingLists that denotes buyers that are to shop before the others
     */
    public static @NonNull List<@NonNull Action> prefPlacementDecisions(@NonNull Economy economy,
                                                                        List<ShoppingList> sls) {
        @NonNull List<Action> actions = new ArrayList<>();
        // iterate over all buyers passed
        for (@NonNull ShoppingList shoppingList : sls) {
            actions.addAll(generatePlacementDecisions(economy, shoppingList));
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
        return Placement.placementDecisions(economy, new ArrayList<>(), false);
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
    public static @NonNull List<@NonNull Action> generatePlacementDecisions(@NonNull Economy economy,
                                ShoppingList shoppingList) {
        @NonNull List<Action> actions = new ArrayList<>();
        if (economy.getForceStop()) {
            return actions;
        }
        // if there are no sellers in the market, the buyer is misconfigured
        final @NonNull List<@NonNull Trader> sellers = economy.getMarket(shoppingList).getActiveSellers();
        if (!shoppingList.isMovable())
            return actions;
        if (sellers.isEmpty()) {
            actions.add(new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
            // To prevent regeneration of duplicate reconfigure actions
            shoppingList.setMovable(false);
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
            double savings = currentQuote - cheapestQuote;
            if (Double.isInfinite(savings)) {
                savings = 0;
            }
            // create recommendation, add it to the result list and  update the economy to
            // reflect the decision
            actions.add(new Move(economy,shoppingList,cheapestSeller).take().setImportance(
                            savings));
            if (economy.getSettings().isUseExpenseMetricForTermination()) {
                Market myMarket = economy.getMarket(shoppingList);
                myMarket.setPlacementSavings(myMarket.getPlacementSavings() + savings);
                if (myMarket.getExpenseBaseline() < myMarket.getPlacementSavings()) {
                    logger.info("Total savings exceeds base expenses for buyer while shopping " +
                        shoppingList.getBuyer().getDebugInfoNeverUseInCode() + " Basket " +
                        shoppingList.getBasket());
                }
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
    public static @NonNull List<@NonNull Action> shopTogetherDecisions(@NonNull Economy economy) {
        return Placement.shopTogetherDecisions(economy, new ArrayList<>(), false);
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     * allowing shoppingLists in "shopFirstShoppingLists" to shop before the rest of the shoppingLists
     * in the economy
     *
     * <p>
     *  As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param shopFirstShoppingLists - list of shoppingLists that denotes buyers that
     *                                 are to shop before the others
     * @param placeJustPassedBuyers - boolean to run placements only on the buyers passed
     */
    public static @NonNull List<@NonNull Action> shopTogetherDecisions(@NonNull Economy economy,
                                List<ShoppingList> shopFirstShoppingLists, boolean placeJustPassedBuyers) {
        @NonNull List<@NonNull Action> output = new ArrayList<>();

        List<Trader> specialTraders = new ArrayList<>();
        if (!shopFirstShoppingLists.isEmpty()) {
            shopFirstShoppingLists.forEach(sl -> specialTraders.add(sl.getBuyer()));
            // place selected list of buyers
            output.addAll(generateShopTogetherDecisions(economy, specialTraders));
            if (!placeJustPassedBuyers) {
                output.addAll(generateShopTogetherDecisions(economy, economy.getTraders().stream()
                                .filter(trader -> !specialTraders.contains(trader)).collect(Collectors.toList())));
            }
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
     * @param traders - list of {@link Trader} that are to be placed before the rest present in the
     * {@link Economy}
     */
    public static @NonNull List<@NonNull Action> generateShopTogetherDecisions(@NonNull Economy
                    economy, List<Trader> traders) {
        @NonNull List<@NonNull Action> output = new ArrayList<>();

        for (@NonNull @ReadOnly Trader buyer : traders) {
            if (economy.getForceStop()) {
                return output;
            }
            final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>> entries =
                economy.getMarketsAsBuyer(buyer).entrySet();
            final @NonNull @ReadOnly List<Entry<@NonNull ShoppingList, @NonNull Market>> movableEntries =
                entries.stream().filter(entry->entry.getKey().isMovable()).collect(Collectors.toList());

            // Only optimize active traders that are at least partially movable.
            if (buyer.getState().isActive() && !movableEntries.isEmpty()) {
                // Find the set of k-partite cliques where the trader can potentially be placed.
                Set<Long> commonCliques = entries.stream()
                    .map(entry->entry.getKey().isMovable() // if shopping list is movable
                        ? entry.getValue().getCliques().keySet() // use the cliques of the market
                        : (new TreeSet<>(entry.getKey().getSupplier() != null // else if shopping list is placed
                                        ? entry.getKey().getSupplier().getCliques() // use clique that contain supplier
                                        : Arrays.asList())) // else there is no valid placement.
                    ).reduce(Sets::intersection).get();

                // Compute current total quote.
                final double currentTotalQuote = movableEntries.stream().mapToDouble(entry ->
                    entry.getKey().getSupplier() == null // if unplaced or incorrectly placed
                        || !entry.getValue().getBasket().isSatisfiedBy(entry.getKey().getSupplier()
                                .getBasketSold())
                    ? Double.POSITIVE_INFINITY // current total is infinite
                    : EdeCommon.quote(economy, entry.getKey(), entry.getKey().getSupplier(),
                                      Double.POSITIVE_INFINITY, false)[0]
                ).sum(); // TODO: break early...

                // Compute the best total quote.
                CliqueMinimizer minimizer = commonCliques.stream().collect(
                    ()->new CliqueMinimizer(economy,movableEntries), CliqueMinimizer::accept,
                    CliqueMinimizer::combine);

                // If buyer can improve its position, output action.
                if (minimizer.getBestTotalQuote() < currentTotalQuote * economy.getSettings().getQuoteFactor()) {
                    List<ShoppingList> shoppingLists = movableEntries.stream()
                        .map(Entry::getKey).collect(Collectors.toList());
                    output.add(new CompoundMove(economy, shoppingLists, minimizer.getBestSellers())
                                   .take().setImportance(currentTotalQuote - minimizer
                                   .getBestTotalQuote()));
                }
            }
        }
        return output;
    }

    /**
     * Run placement algorithm until the convergence criteria are satisfied.
     * If the placement has been running for more than MAX_NUM_PLACEMENT, force placement to stop.
     * @param economy
     * @param ledger - the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param isShopTogether - flag specifies if we want to use SNM or normal placement
     * @param callerPhase - tag to identify phase it is being called from
     * @return a list of recommendations about trader placement
     */
    public static @NonNull List<@NonNull Action> runPlacementsTillConverge(Economy economy,
                    Ledger ledger, boolean isShopTogether, String callerPhase) {
        return runPlacementsTillConverge(economy, new ArrayList<ShoppingList>(), ledger,
                                         isShopTogether, false, callerPhase);
    }

    /**
     * Run placement algorithm until the convergence criteria are satisfied.
     * If the placement has been running for more than MAX_NUM_PLACEMENT, force placement to stop.
     * @param economy
     * @param shoppingLists - list of shoppingLists that denotes buyers that are to shop before the others
     * @param ledger - the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param isShopTogether - flag specifies if we want to use SNM or normal placement
     * @param placeJustPassedBuyers - run placements on just the {@link ShoppingList}s passed if
     *                                true and on all {@link ShoppingList}s if false
     * @param callerPhase - tag to identify phase it is being called from
     * @return a list of recommendations about trader placement
     */
    public static @NonNull List<@NonNull Action> runPlacementsTillConverge(Economy economy,
                    List<ShoppingList> shoppingLists, Ledger ledger, boolean isShopTogether,
                    boolean placeJustPassedBuyers, String callerPhase) {
        @NonNull
        List<Action> actions = new ArrayList<@NonNull Action>();
        // generate placement actions
        boolean keepRunning = true;
        boolean useExpenseMetric = economy.getSettings().isUseExpenseMetricForTermination();
        if (useExpenseMetric) {
            initializeMarketExpenses(economy, ledger);
        }
        int counter = 0;
        while (keepRunning) {
            // in certain edge cases, we may have placement keep generating move actions
            // while they don't really improve the performance. We force the placement to
            // stop when reaching the max number allowed.
            if (counter >= Placement.MAX_NUM_PLACEMENT) {
                keepRunning = false;
                logger.warn("The placement has been running for " + Placement.MAX_NUM_PLACEMENT
                                + " rounds, forcing placement to stop now!");
                break;
            }
            List<Action> placeActions = isShopTogether
                            ? breakDownCompoundMove(Placement.shopTogetherDecisions(economy,
                                            shoppingLists, placeJustPassedBuyers))
                            : placementDecisions(economy, shoppingLists, placeJustPassedBuyers);
            counter++;
            globalCounter++;
            keepRunning = !(placeActions.isEmpty()
                            || placeActions.stream().allMatch(a -> a instanceof Reconfigure)
                            || (useExpenseMetric && areSavingsLessThanThreshold(economy)));
            actions.addAll(placeActions);
            if (useExpenseMetric) {
                adjustMarketBaselineExpenses(economy, ledger);
            }

        }
        logger.info(callerPhase + " Total Placement Iterations: " + counter + " " + globalCounter);
        return actions;
    }

    /**
     * Initialize the total expenses of buyers in the market
     *
     * @param economy - the {@link Economy}
     * @param ledger - the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     */
    private static void initializeMarketExpenses(@NonNull Economy economy,
                                                 @NonNull Ledger ledger) {
        for (Market market : economy.getMarkets()) {
            calculateTotalExpensesForBuyersInMarket(economy, ledger, market);
        }
    }

    /**
     * Initialize the total expenses of buyers in the market
     *
     * @param economy - the {@link Economy}
     * @param ledger - the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param market - the {@link Market} for which expenses are to be calculated
     */
    private static void calculateTotalExpensesForBuyersInMarket(@NonNull Economy economy,
                                                                @NonNull Ledger ledger,
                                                                @NonNull Market market) {
        double totalExpenseForMarket =
                  ledger.calculateTotalExpensesForBuyers(economy, market);
        if (Double.isInfinite(totalExpenseForMarket)) {
            totalExpenseForMarket = Double.MAX_VALUE;
        }
        market.setExpenseBaseline(totalExpenseForMarket);
    }

    /**
     * Adjust the expenses of each Market for use in next iteration
     *
     * @param economy The {@link Economy}
     * @param ledger - the {@link Ledger} of the {@link Economy}
     */
    private static void adjustMarketBaselineExpenses(@NonNull Economy economy,
                                                     @NonNull Ledger ledger) {
        for (Market market : economy.getMarkets()) {
            double newBaseLine = market.getExpenseBaseline() - market.getPlacementSavings();
            if (newBaseLine < 0 || Double.isInfinite(newBaseLine)) {
                calculateTotalExpensesForBuyersInMarket(economy, ledger, market);
                logger.info("Total savings exceeds base expenses, recalculating market " +
                            market.getBasket());
            } else {
                market.setExpenseBaseline(newBaseLine);
            }
            market.setPlacementSavings(0);
        }
    }

    /**
     * Returns true if the expenses in any market have not changed by more than epsilon
     *
     * @param economy The {@link Economy}
     * @return true if the expenses are at minimum
     */
    public static boolean areSavingsLessThanThreshold(@NonNull Economy economy) {
        double factor = economy.getSettings().getExpenseMetricFactor();
        for (Market market : economy.getMarkets()) {
            if (market.getPlacementSavings() > factor * market.getExpenseBaseline()) {
                return false;
            }
        }
        return true;
    }

    /**
     * A helper method to break down the compoundMove to individual move so that legacy UI can
     * assimilate it. This method should be called only when legacy UI is used!
     *
     * @param compoundMoves a list of CompoundMove actions to be broken down into individual
     * Move actions
     * @return a list of moves that constitute the compoundMove
     */
    public static List<Action> breakDownCompoundMove(List<Action> compoundMoves) {
        // break down the compound move to individual moves so that legacy UI can assimilate it.
        // TODO: if new UI can support compoundMove, we do not need this break down
        List<Action> moveActions = new ArrayList<Action>();
        compoundMoves.forEach(a -> {
            if (a instanceof CompoundMove) {
                moveActions.addAll(((CompoundMove)a).getConstituentMoves());
            }
        });
        return moveActions;
    }
} // end Placement class
