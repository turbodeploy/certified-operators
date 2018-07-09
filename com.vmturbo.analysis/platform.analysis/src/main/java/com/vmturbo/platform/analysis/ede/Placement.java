package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;


import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

/**
 * Contains static methods related to optimizing the placement of {@link Trader}s in an
 * {@link Economy}.
 */
public class Placement {

    private Placement() {}

    static final Logger logger = LogManager.getLogger(Placement.class);

    public static int globalCounter = 0;
    private static boolean printMaxPlacementIterations = true;

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     * With a preference given to the shoppingLists in the list "sls"
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param sls - list of shoppingLists that denotes buyers that are to shop before the others
     * @param preferentialPlacementOnly - boolean to run placements only on the buyers passed
     * @return the placement decisions
     */
    public static @NonNull PlacementResults placementDecisions(@NonNull Economy economy,
                    List<ShoppingList> sls, boolean preferentialPlacementOnly) {
        @NonNull PlacementResults placementResults = prefPlacementDecisions(economy, sls);
        @NonNull Set<@NonNull ShoppingList> slsToPlace = new LinkedHashSet<>();
        if (!preferentialPlacementOnly) {
            // iterate over all markets, i.e., all sets of providers selling a specific basket
            for (Market market : economy.getMarketsForPlacement()) {
                // iterate over all buyers in this market that havn't already shopped
                for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                    if (sls.contains(shoppingList)) {
                        continue;
                    }
                    slsToPlace.add(shoppingList);
                }
            }
            placementResults.combine(generatePlacementDecisions(economy, slsToPlace));
        }
        return placementResults;
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @return placement decisions
     */
    public static @NonNull PlacementResults placementDecisions(@NonNull Economy economy) {
        return Placement.placementDecisions(economy, new ArrayList<>(), false);
    }

    /**
     * Returns a list of recommendations to optimize placement of a set of shopping lists
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param sls - list of shoppingLists that denotes buyers that are to shop before the others
     * @return placement decisions due to performance
     */
    public static @NonNull PlacementResults prefPlacementDecisions(@NonNull Economy economy,
                                                                        List<ShoppingList> sls) {
        return generatePlacementDecisions(economy, sls);
    }
    
    /**
     * Returns a list of recommendations to optimize the placement of a trader either using shop
     * together or shop alone algorithm depending on the trader's setting.
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param shoppingLists - The {@link ShoppingList} for which we try to find the best destination
     * @return the placement actions
     */
    public static @NonNull PlacementResults generatePlacementDecisions(
                    @NonNull Economy economy, @NonNull Collection<@NonNull ShoppingList> shoppingLists) {
        @NonNull PlacementResults placementResults = new PlacementResults();
        // Use a LinkedHashSet to prevent duplicates but allow ordered traversal.
        @NonNull Set<@NonNull Trader> shopTogetherTraders = new LinkedHashSet<>();
        for (ShoppingList sl : shoppingLists) {
            // if the sl is shop together, put it into a list, else, run non shop alone placement
            Trader buyer = sl.getBuyer();
            if (buyer.getSettings().isShopTogether()) {
                shopTogetherTraders.add(buyer);
            } else {
                placementResults.combine(generateShopAlonePlacementDecisions(economy, sl));
            }
        }
        placementResults.combine(generateShopTogetherDecisions(economy, shopTogetherTraders));
        return placementResults;
    }

    /**
     * Returns a list of recommendations to optimize the placement of a trader with a
     * particular shoppingList
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param shoppingList - The {@link ShoppingList} for which we try to find the best destination
     * @return the placement actions
     */
    public static PlacementResults generateShopAlonePlacementDecisions(
                    @NonNull Economy economy, ShoppingList shoppingList) {
        if (economy.getForceStop()) {
            return PlacementResults.empty();
        }
        // if there are no sellers in the market, the buyer is misconfigured
        final @NonNull List<@NonNull Trader> sellers =
                        economy.getMarket(shoppingList).getActiveSellersAvailableForPlacement();
        // sl can be immovable when the underlying provider is not availableForPlacement
        if (!shoppingList.isMovable())
            return PlacementResults.empty();
        if (economy.getMarket(shoppingList).getActiveSellers().isEmpty()) {
            final PlacementResults results = PlacementResults.forSingleAction(
                new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
            // To prevent regeneration of duplicate reconfigure actions
            shoppingList.setMovable(false);
            return results;
        }

        // get cheapest quote
        final QuoteMinimizer minimizer =
            (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                ? sellers.stream() : sellers.parallelStream())
                                                        .collect(() -> new QuoteMinimizer(economy,
                                                                        shoppingList),
                QuoteMinimizer::accept, QuoteMinimizer::combine);

        final double cheapestQuote = minimizer.getBestQuote();
        final Trader cheapestSeller = minimizer.getBestSeller();
        Trader buyer = shoppingList.getBuyer();
        boolean isDebugTrader = buyer.isDebugEnabled() || logger.isTraceEnabled();
        boolean isSellersInfoPrinted = buyer.isSellersInfoPrinted();
        String buyerDebugInfo = shoppingList.getBuyer().getDebugInfoNeverUseInCode();
        if (logger.isTraceEnabled() || isDebugTrader) {
            if (!isSellersInfoPrinted) {
                logger.info("{" + buyerDebugInfo + "} Print debug info for all sellers in placement: ");
                for (Trader trader : sellers) {
                    logger.info("Possible seller debug info: " + trader.getDebugInfoNeverUseInCode());
                }
                shoppingList.getBuyer().setSellersInfoPrinted(true);
            }
            if (shoppingList.getSupplier() == null) {
                logger.info("{" + buyerDebugInfo + "} Supplier is null.");
            } else {
                logger.info("{" + buyerDebugInfo
                                + "} current supplier: " + shoppingList.getSupplier().getDebugInfoNeverUseInCode());
            }
            if (!shoppingList.isMovable()) {
                logger.info("{" + buyerDebugInfo + "} Shopping list of " + shoppingList.getSupplier() + " is not movable.");
            }
            if (cheapestSeller == null) {
                logger.info("{" + buyerDebugInfo + "} The cheapest supplier is null.");
            } else {
                logger.info("{" + buyerDebugInfo + "} The cheapest quote: "
                                + cheapestQuote + " from the cheapest supplier: " + cheapestSeller.getDebugInfoNeverUseInCode());
            }
        }
        final double currentQuote = minimizer.getCurrentQuote();

        // move, and update economy and state
        PlacementResults placementResults = PlacementResults.empty();
        if (cheapestQuote < currentQuote * buyer.getSettings().getQuoteFactor()) {
            double savings = currentQuote - cheapestQuote;
            if (Double.isInfinite(savings)) {
                savings = 0;
                if (isDebugTrader) {
                    if (shoppingList.getSupplier() != null) {
                        logger.info("{" + buyerDebugInfo + "} The infinite quote is from supplier: "
                                        + shoppingList.getSupplier().getDebugInfoNeverUseInCode());
                    }
                }
            }
            // create recommendation, add it to the result list and  update the economy to
            // reflect the decision
            placementResults = PlacementResults.forSingleAction(
                new Move(economy, shoppingList, cheapestSeller).take().setImportance(savings));
            if (economy.getSettings().isUseExpenseMetricForTermination()) {
                Market myMarket = economy.getMarket(shoppingList);
                myMarket.setPlacementSavings(myMarket.getPlacementSavings() + savings);
                if (logger.isDebugEnabled()
                             && myMarket.getExpenseBaseline() < myMarket.getPlacementSavings()) {
                    logger.debug("Total savings exceeds base expenses for buyer while shopping " +
                                    buyer.getDebugInfoNeverUseInCode()
                                    + " Basket " + shoppingList.getBasket());
                }
            }
        }

        return placementResults;
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @return shop-together decisions
     */
    public static @NonNull PlacementResults shopTogetherDecisions(@NonNull Economy economy) {
        return Placement.shopTogetherDecisions(economy, new ArrayList<>(), false);
    }

    /**
     * Returns a list of recommendations to optimize the placement of all traders in the economy.
     * allowing shoppingLists in "shopFirstShoppingLists" to shop before the rest of the shoppingLists
     * in the economy
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param shopFirstShoppingLists - list of shoppingLists that denotes buyers that
     *                                 are to shop before the others
     * @param preferentialPlacementOnly - boolean to run placements only on the buyers passed
     * @return shop-together decisions
     */
    public static @NonNull PlacementResults shopTogetherDecisions(@NonNull Economy economy,
                                List<ShoppingList> shopFirstShoppingLists, boolean preferentialPlacementOnly) {
        @NonNull final PlacementResults placementResults = new PlacementResults();

        // Use a linkedHashSet to preserve order, permit fast #contains lookups, and ensure only
        // unique elements in the collection.
        Set<Trader> specialTraders = new LinkedHashSet<>();
        if (!shopFirstShoppingLists.isEmpty()) {
            shopFirstShoppingLists.forEach(sl -> specialTraders.add(sl.getBuyer()));
            // place selected list of buyers
            placementResults.combine(generateShopTogetherDecisions(economy, specialTraders));
            if (!preferentialPlacementOnly) {
                placementResults.combine(generateShopTogetherDecisions(economy, economy.getTraders().stream()
                    .filter(trader -> !specialTraders.contains(trader))
                    .collect(Collectors.toList())));
            }
        } else {
            placementResults.combine(generateShopTogetherDecisions(economy, economy.getTraders()));
        }
        return placementResults;
    }

    /**
     * Returns a list of recommendations to optimize the placement of a trader buying
     * shoppingLists in specific markets
     *
     * <p>As a result of invoking this method, the economy may be changed.
     * </p>
     *
     * @param economy - the economy whose traders' placement we want to optimize
     * @param traders - list of {@link Trader} that are to be placed before the rest present in the
     * {@link Economy}
     * @return shop-together decisions
     */
    public static PlacementResults generateShopTogetherDecisions(@NonNull Economy
                    economy, Collection<Trader> traders) {
        @NonNull final PlacementResults placementResults = new PlacementResults();

        for (Trader buyingTrader : traders) {
            if (economy.getForceStop()) {
                return placementResults;
            }
            if (!shouldConsiderTraderForShopTogether(economy, buyingTrader)) {
                continue;
            }

            // If there are no sellers in any market, the buyer is misconfigured
            boolean generatedReconfigure = false;
            for (Entry<@NonNull ShoppingList, @NonNull Market> entry : economy.moveableSlByMarket(buyingTrader)) {
                ShoppingList sl = entry.getKey();
                if (sl.isMovable() && entry.getValue().getActiveSellers().isEmpty()) {
                    generatedReconfigure = true;
                    // Since the shopping list can be in multiple entries in this loop,
                    // we need to check whether a Reconfigure was already generated for
                    // this list.
                    placementResults.addAction(new Reconfigure(economy, sl).take()
                        .setImportance(Double.POSITIVE_INFINITY));
                    // Set movable to false to prevent generating further reconfigures
                    // for this shopping list
                    sl.setMovable(false);
                }
            }
            if (generatedReconfigure) {
                continue;
            }

            CliqueMinimizer minimizer = computeBestQuote(economy, buyingTrader);
            // If the best suppliers are not current ones, move shopping lists to best places
            placementResults.addActions(checkAndGenerateCompoundMoveActions(economy,
                buyingTrader, minimizer));

            // Add explanations for unplaced traders.
            if (minimizer != null && Double.isInfinite(minimizer.getBestTotalQuote())) {
                placementResults.addUnplacedTraders(buyingTrader,
                    minimizer.getInfiniteQuoteTrackers().values());
            }
        }
        return placementResults;
    }

    /**
     * Check if the current suppliers are the same as best sellers. If not, generate a {@link
     * CompoundMove}.
     *
     * @param economy the economy
     * @param buyer the buyer for which to generate the actions
     * @param minimizer the {@link CliqueMinimizer}
     * @return a list of actions generated
     */
    public static List<Action> checkAndGenerateCompoundMoveActions(
                    Economy economy,
                    Trader buyer,
                   @NonNull CliqueMinimizer minimizer) {
        List<Action> actions = new ArrayList<>();
        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.moveableSlByMarket(buyer);
        List<ShoppingList> shoppingLists = movableSlByMarket.stream().map(Entry::getKey)
                        .collect(Collectors.toList());
        Set<Integer> currentSuppliersIds = traderIds(shoppingLists.stream()
                        .map(ShoppingList::getSupplier));
        Set<Integer> bestSellerIds = minimizer == null || minimizer.getBestSellers() == null
                        ? Collections.emptySet()
                        : traderIds(minimizer.getBestSellers().stream());
        if (minimizer != null && !currentSuppliersIds.equals(bestSellerIds)) {
            double currentTotalQuote = computeCurrentQuote(economy, movableSlByMarket);
            if (minimizer.getBestTotalQuote() < currentTotalQuote
                            * shoppingLists.get(0).getBuyer().getSettings().getQuoteFactor()) {
                List<Trader> bestSellers = minimizer.getBestSellers();
                List<Trader> currentSuppliers = shoppingLists.stream().map(ShoppingList::getSupplier)
                                .collect(Collectors.toList());
                CompoundMove compoundMove =
                                    CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
                                        economy, shoppingLists, currentSuppliers, bestSellers);
                if (compoundMove != null) {
                    actions.add(compoundMove.take().setImportance(currentTotalQuote - minimizer
                            .getBestTotalQuote()));
                }
            }
        }
        return actions;
    }

    /**
     * Convert a stream of traders to a set of the traders' ids. The id of a trader
     * is its economy index.
     *
     * @param tradersStream a stream of traders
     * @return a set contain ing the ids of the traders
     */
    private static Set<Integer> traderIds(Stream<Trader> tradersStream) {
        return tradersStream
                    .map(trader -> trader == null ? null : trader.getEconomyIndex())
                    .collect(Collectors.toSet());

    }

    public static @Nullable CliqueMinimizer computeBestQuote(Economy economy, Trader trader) {
        Set<Long> commonCliques = economy.getCommonCliques(trader);
        if (commonCliques.isEmpty()) {
            return null;
        }
        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.moveableSlByMarket(trader);
        return commonCliques.stream().collect(
            () -> new CliqueMinimizer(economy, movableSlByMarket),
            CliqueMinimizer::accept,
            CliqueMinimizer::combine);
    }

    /**
     * Return true if the trader is active and any of its shopping lists are movable.
     * @param economy the economy that the trader belongs to
     * @param trader the trader
     * @return whether the trader is active and has any movable shopping list
     */
    public static boolean shouldConsiderTraderForShopTogether(Economy economy, Trader trader) {
        return trader.getState().isActive()
                && economy.getMarketsAsBuyer(trader).keySet().stream()
                            .anyMatch(ShoppingList::isMovable);
    }

    /**
     * Compute the current quote for a trader.
     *
     * @param economy the economy the trader associates with
     * @param movableSlByMarket the movable shopping list of a buyer and its participating market
     * @return the current quote given by current supplier
     */
    public static double computeCurrentQuote(Economy economy,
                    List<Entry<@NonNull ShoppingList, @NonNull Market>> movableSlByMarket) {
        // Compute current total quote.
        double quote = 0;
        for (Entry<ShoppingList, Market> entry : movableSlByMarket) {
            if (isQuoteInfinity(entry)) {
                return Double.POSITIVE_INFINITY;
            }
            quote += EdeCommon.quote(economy, entry.getKey(), entry.getKey().getSupplier(),
                                     Double.POSITIVE_INFINITY, false).getQuoteValue();
        }
        return quote;
    }

    /**
     * Check if a quote for a shopping list is infinite. If the shopping list has no active supplier,
     * or the supplier does not satisfy the requests of shopping list, quote is infinity.
     *
     * @param slByMkt shopping list to market mapping
     * @return whether the quote is infinity
     */
    private static boolean isQuoteInfinity(Entry<@NonNull ShoppingList, @NonNull Market> slByMkt) {
        Trader supplier = slByMkt.getKey().getSupplier();
        return  supplier == null || !supplier.getState().isActive()
                || !slByMkt.getValue().getBasket().isSatisfiedBy(supplier.getBasketSold());
    }
    /**
     * Run placement algorithm until the convergence criteria are satisfied.
     * If the placement has been running for more than the maximum number of placements
     * allowed by the economy settings, force placement to stop.
     * @param economy the economy where all the traders for this calculation exist
     * @param ledger the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param callerPhase - tag to identify phase it is being called from
     * @return a list of recommendations about trader placement
     */
    public static @NonNull PlacementResults runPlacementsTillConverge(Economy economy,
                    Ledger ledger, String callerPhase) {
        return runPlacementsTillConverge(economy, new ArrayList<ShoppingList>(), ledger,
                        false, callerPhase);
    }

    /**
     * Run placement algorithm until the convergence criteria are satisfied.
     * If the placement has been running for more than the maximum number of placements
     * allowed by the economy settings, force placement to stop.
     * @param economy the economy where all the traders for this calculation exist
     * @param shoppingLists list of shoppingLists that denotes buyers that are to shop before the others
     * @param ledger the {@link Ledger} with the expenses and revenues of all the traders
     *        and commodities in the economy
     * @param preferentialPlacementOnly - run placements on just the {@link ShoppingList}s passed if
     *                                true and on all {@link ShoppingList}s if false
     * @param callerPhase - tag to identify phase it is being called from
     * @return a list of recommendations about trader placement
     */
    public static @NonNull PlacementResults runPlacementsTillConverge(Economy economy,
                    List<ShoppingList> shoppingLists, Ledger ledger,
                    boolean preferentialPlacementOnly, String callerPhase) {
        @NonNull final PlacementResults placementResults = new PlacementResults();
        // generate placement actions
        boolean keepRunning = true;
        // we want to prevent computation of the expenseMetric when we perform preferentialPlacement
        boolean useExpenseMetric = economy.getSettings().isUseExpenseMetricForTermination()
                            && !preferentialPlacementOnly;
        if (useExpenseMetric) {
            initializeMarketExpenses(economy, ledger);
        }
        int counter = 0;
        if (printMaxPlacementIterations) {
            logger.info("The maximum placement iteration number has been set to: " + economy.getSettings().getMaxPlacementIterations());
            printMaxPlacementIterations = false;
        }
        while (keepRunning) {
            // in certain edge cases, we may have placement keep generating move actions
            // while they don't really improve the performance. We force the placement to
            // stop when reaching the max number allowed.
            if (counter >= economy.getSettings().getMaxPlacementIterations()) {
                keepRunning = false;
                logger.warn("The placement has been running for " +
                    economy.getSettings().getMaxPlacementIterations()
                    + " rounds, forcing placement to stop now!");
                break;
            }
            final PlacementResults intermediateResults =
                placementDecisions(economy, shoppingLists, preferentialPlacementOnly);

            counter++;
            globalCounter++;
            keepRunning = !(intermediateResults.getActions().isEmpty()
                            || intermediateResults.getActions().stream().allMatch(a -> a instanceof Reconfigure)
                            || (useExpenseMetric && areSavingsLessThanThreshold(economy)));
            placementResults.combine(intermediateResults);
            if (useExpenseMetric) {
                adjustMarketBaselineExpenses(economy, ledger);
            }

        }
        if (logger.isDebugEnabled()) {
            logger.debug(callerPhase + " Total Placement Iterations: " + counter + " " + globalCounter);
        }
        return placementResults;
    }

    /**
     * Initialize the total expenses of buyers in the market.
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
     * Initialize the total expenses of buyers in the market.
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
     * Adjust the expenses of each Market for use in next iteration.
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
                if (logger.isDebugEnabled()) {
                    logger.debug("Total savings exceeds base expenses, recalculating market " +
                                 market.getBasket());
                }
            } else {
                market.setExpenseBaseline(newBaseLine);
            }
            market.setPlacementSavings(0);
        }
    }

    /**
     * Returns true if the expenses in any market have not changed by more than epsilon.
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
