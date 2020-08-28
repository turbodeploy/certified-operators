package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.analysis.utilities.Quote;
import com.vmturbo.platform.analysis.utilities.QuoteCache;

/**
 * Contains static methods related to optimizing the placement of {@link Trader}s in an
 * {@link Economy}.
 */
public class Placement {

    private Placement() {}

    static final Logger logger = LogManager.getLogger(Placement.class);

    public static int globalCounter = 0;
    private static boolean printMaxPlacementIterations = true;
    public static final int MOVE_COST_FACTOR_MAX_COMM_SIZE = 10;

    /**
     * Used to set parent id in the merged context, parent id is needed for CBTP cost lookup
     * to work in case of from onPrem to cloud migration.
     */
    @VisibleForTesting
    static final BinaryOperator<Set<Context>> mergeContextSets = (s1, s2) -> {
        // Make up a map of account id to their parent (BillingFamily) ids.
        final Map<Long, Long> accountIdToParentId = new HashMap<>();
        Stream.of(s1, s2)
                .flatMap(Collection::stream)
                .forEach(ctx -> {
                    if (ctx.getBalanceAccount().getParentId() != null) {
                        accountIdToParentId.put(ctx.getBalanceAccount().getId(),
                                ctx.getBalanceAccount().getParentId());
                        ctx.getBalanceAccount().setParentId(null);
                    }
                });
        // Find common context b/w the 2 sets.
        Set<Context> commonContexts = Sets.intersection(s1, s2);

        // make clones of commonContext which derived from active sellers and later pass
        // them to buying trader. The buying trader can not use the same context as the
        // seller mainly because parentId exists only on cbtp seller but not tp seller.
        // The above intersection on context will exclude the cbtp seller context, but it
        // is needed for later cost calculation.
        Set<Context> buyerContexts = Sets.newHashSet();
        for (Context c : commonContexts) {
            long baId = c.getBalanceAccount().getId();
            BalanceAccount ba = accountIdToParentId.containsKey(baId)
                    ? new BalanceAccount(baId, accountIdToParentId.get(baId))
                    : new BalanceAccount(baId);
            buyerContexts.add(new Context(c.getRegionId(), c.getZoneId(), ba));
        }
        return buyerContexts;
    };

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
                // iterate over all buyers in this market that haven't already shopped
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
     * Returns a QuoteMinimizer containing the cheapest quote and cheapest seller.
     *
     * @param economy - the economy that we want to generate a Quote Minimizer in.
     * @param sellers - list of traders that are potential providers for the shopping list
     * @param shoppingList - The {@link ShoppingList} for which we try to find the cheapest
     * destination
     * @param cache - is the {@link QuoteCache} that contains previously computed quotes
     * @param shoppingListIndex The index of <b>shoppingList</b> in the iteration order of the map
     *                          returned by {@link Economy#getMarketsAsBuyer(Trader)}. i.e. the
     *                          first key has index 0, the second index 1 and so on. If <b>cache</b>
     *                          is {@code null}, the value of this argument is ignored.
     * @return the QuoteMinimizer
     */
    public static QuoteMinimizer initiateQuoteMinimizer(@NonNull Economy economy,
                                @NonNull List<@NonNull Trader> sellers, ShoppingList shoppingList,
                                QuoteCache cache, final int shoppingListIndex) {
        // If the shopping list's best provider is its current provider then obtain minimizer again
        // without consider the commodities in the unquoted commodities base type list.
        QuoteMinimizer minimizer;
        for (;;) {
            minimizer = (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                            ? sellers.stream() : sellers.parallelStream())
                                .collect(() -> new QuoteMinimizer(economy, shoppingList,
                                                                    cache, shoppingListIndex),
                                    QuoteMinimizer::accept, QuoteMinimizer::combine);
            if (sellers.size() > 1 && (minimizer.getBestSeller() == null
                            || minimizer.getBestSeller() == shoppingList.getSupplier())) {
                if (!shoppingList.getUnquotedCommoditiesBaseTypeList()
                        .equals(shoppingList.getModifiableUnquotedCommoditiesBaseTypeList())) {
                    shoppingList.getModifiableUnquotedCommoditiesBaseTypeList().clear();
                    shoppingList.getUnquotedCommoditiesBaseTypeList().forEach(c -> {
                        shoppingList.addModifiableUnquotedCommodityBaseType(c);
                    });
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return minimizer;
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

        if (logger.isTraceEnabled()) {
            logger.trace("PSL Sellers for shoppingList: " + shoppingList.toString());
            for (Trader trader : sellers) {
                if (AnalysisToProtobuf.replaceNewSupplier(shoppingList, economy, trader) != null) {
                    logger.trace("PSL Seller: " +
                            trader.toString());
                }
            }
        }
        // sl can be immovable when the underlying provider is not availableForPlacement
        if (!shoppingList.isMovable()) {
            return PlacementResults.empty();
        }
        if (economy.getMarket(shoppingList).getActiveSellers().isEmpty()) {
            final PlacementResults results = PlacementResults.forSingleAction(
                new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
            // To prevent regeneration of duplicate reconfigure actions
            shoppingList.setMovable(false);
            return results;
        }

        final QuoteMinimizer minimizer = initiateQuoteMinimizer(economy, sellers, shoppingList,
                                                    null, 0 /* ignored because cache == null */);

        final double cheapestQuote = minimizer.getTotalBestQuote();
        final Trader cheapestSeller = minimizer.getBestSeller();
        final Trader buyer = shoppingList.getBuyer();
        final double currentQuote = minimizer.getCurrentQuote().getQuoteValue();

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
                                + "} current supplier: " + shoppingList.getSupplier()
                                + " quote: " + currentQuote);
            }
            if (!shoppingList.isMovable()) {
                logger.info("{" + buyerDebugInfo + "} Shopping list of " + shoppingList.getSupplier() + " is not movable.");
            }
            if (cheapestSeller == null) {
                logger.info("{" + buyerDebugInfo + "} The cheapest supplier is null.");
            } else {
                logger.info("{" + buyerDebugInfo + "} The cheapest quote: "
                                + cheapestQuote + " from the cheapest supplier: " + cheapestSeller.getDebugInfoNeverUseInCode());
                logger.trace("{" + buyerDebugInfo + "} shopping list: " + shoppingList.toDebugString());
            }
        }

        // move, and update economy and state
        PlacementResults placementResults = PlacementResults.empty();
        // Move will require destination provider to be cheaper than current host by quote factor
        // and move cost factor. Or there needs to be a change in coverage logged in the context
        if (Math.min(MOVE_COST_FACTOR_MAX_COMM_SIZE, shoppingList.getBasket().size())
                        * buyer.getSettings().getMoveCostFactor() + cheapestQuote
                        < currentQuote * buyer.getSettings().getQuoteFactor() ||
                (cheapestSeller == shoppingList.getSupplier()
                        && minimizer.getBestQuote().getContext().isPresent()
                        && shoppingList.getBuyer().getSettings().getContext().isPresent()
                        && !shoppingList.getBuyer().getSettings().getContext().get()
                            .isEqualCoverages(minimizer.getBestQuote().getContext())) ||
                // isScalingGroupConsistentlySized() is only meaningful for cloud providers.
                // Anything else will return consistently sized.
                (cheapestSeller != null && !economy.isScalingGroupConsistentlySized(shoppingList))
                || anyDecisiveCommodityResized(minimizer.getBestQuote(), shoppingList)) {
            double savings = currentQuote - cheapestQuote;
            if (Double.isInfinite(savings)) {
                savings = Double.MAX_VALUE;
                if (isDebugTrader) {
                    if (shoppingList.getSupplier() != null) {
                        logger.info("{" + buyerDebugInfo + "} The infinite quote is from supplier: "
                                        + shoppingList.getSupplier().getDebugInfoNeverUseInCode());
                    }
                }
            }
            // create recommendation, add it to the result list and  update the economy to
            // reflect the decision
            Move move = new Move(economy, shoppingList, shoppingList.getSupplier(),
                cheapestSeller, minimizer.getBestQuote().getContext(),
                minimizer.getBestQuote().getCommodityContexts());
            placementResults.addAction(move.take().setImportance(savings));
            if (economy.getSettings().isUseExpenseMetricForTermination()) {
                Market myMarket = economy.getMarket(shoppingList);
                double placementSavings = myMarket.getPlacementSavings() + savings;
                if (Double.isInfinite(placementSavings)) {
                    placementSavings = Double.MAX_VALUE;
                }
                myMarket.setPlacementSavings(placementSavings);
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

    private static boolean anyDecisiveCommodityResized(final Quote quote,
                                                 final ShoppingList shoppingList) {
        return quote.getCommodityContexts().stream()
            .anyMatch(context -> {
                final Double assignedCapacity =
                    shoppingList.getAssignedCapacity(context.getCommoditySpecification()
                .getBaseType());
                return context.isCommodityDecisiveOnSeller() && assignedCapacity != null
                    &&  Double.compare(assignedCapacity, context.getNewCapacityOnSeller()) != 0;
            });
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
        if (minimizer == null || minimizer.getBestSellers() == null) {
            return actions;
        }

        final List<ShoppingList> shoppingLists = minimizer.getEntries().stream()
            .map(Entry::getKey).collect(Collectors.toList());
        final List<Trader> currentSuppliers = minimizer.getEntries().stream()
            .map(entry -> entry.getKey().getSupplier()).collect(Collectors.toList());
        final List<Optional<EconomyDTOs.Context>> contextList = new ArrayList<>();
        if (!minimizer.getShoppingListContextMap().isEmpty()) {
            shoppingLists.forEach(sl -> contextList.add(minimizer.getShoppingListContextMap().get(sl)));
        }
        final List<Trader> bestSellers = minimizer.getBestSellers();

        // get the RI coverage from minimizer, compare it with the current RI coverage on buyer,
        // if there is a difference, then create the move action.
        boolean isCoverageSame = true;
        if (!minimizer.getShoppingListContextMap().isEmpty()) {
            for (ShoppingList sl : shoppingLists) {
                if (!sl.getContext().isPresent() || !sl.getContext().get().equals(minimizer
                        .getShoppingListContextMap().get(sl).get())) {
                    isCoverageSame = false;
                    break;
                }
            }
        }
        // create the move action if the current suppliers are different from best sellers, or if
        // they are the same but RI coverage changes
        if (!currentSuppliers.equals(bestSellers) || !isCoverageSame) {
            ShoppingList firstSL = shoppingLists.get(0);
            double currentTotalQuote = computeCurrentQuote(economy, minimizer.getEntries());
            if (!isCoverageSame || Math.min(MOVE_COST_FACTOR_MAX_COMM_SIZE, firstSL.getBasket().size())
                            * firstSL.getBuyer().getSettings().getMoveCostFactor()
                            + minimizer.getBestTotalQuote() < currentTotalQuote
                            * firstSL.getBuyer().getSettings().getQuoteFactor()) {
                double importance = currentTotalQuote - minimizer.getBestTotalQuote();
                generateCompoundMoveOrMoveAction(
                    economy, shoppingLists, currentSuppliers, bestSellers, actions, importance, contextList);
            }
        }
        return actions;
    }

    /**
     * This method is used to decide whether we should generate move actions or compoundMove actions.
     * This method consists of three steps:
     *  1. Split all shoppingLists into two sets.
     *     If a sl has common cliques between its currentSupplier and bestSeller, put it in list A.
     *     If a sl does not have common cliques between its currentSupplier and bestSeller, put it in list B.
     *  2. For each sl (sl1) in A with common cliques (c) between its currentSupplier and bestSeller and
     *     its currentSupplier and bestSeller are different, we should generate a move action
     *     if there exists a sl (sl2) in B without common cliques that satisfies the condition that
     *     the size of the intersection of c and the currentSupplier of sl2 is greater than 0 and
     *     the size of the intersection of c and the bestSeller of sl2 is greater than 0.
     *  3. If the size of the rest of sls is 1, generate a move action for it.
     *     Otherwise, generate a compoundMove action.
     *  This method is suitable for both PM based and DS based bicliques.
     *  E.g. Suppose a vm has two sls: sl1 moves from PM1 to PM2, sl2 moves from Storage1 to Storage2.
     *           (BC-0)PM1----Storage1(BC-0)
     *                    \    /
     *                     \  /
     *                      \/
     *                      /\
     *                     /  \
     *                    /    \
     *           (BC-0)PM2----Storage2(BC-0)
     *       Step 1: PM1 and PM2 have common clique BC-0, put sl1 into list A.
     *               Storage1 and Storage2 have common clique BC-0, put sl2 into list A.
     *               At the end of step 1, A is of size 2 and B is of size 0.
     *       Step 2: Since B is empty, there doesn't exist a sl in B that satisfies the condition.
     *               We don't generate move action for sl1 or sl2.
     *       Step 3: The rest of sls are sl1 and sl2. So we generate a compoundMove for them.
     *  E.g. Suppose a vm has two sls: sl1 moves from PM1 to PM2, sl2 moves from Storage1 to Storage2.
     *           (BC-0)PM1----Storage1(BC-0)
     *                    \
     *                     \
     *                      \
     *                       \
     *                        \
     *                         \
     *           (BC-1)PM2----Storage2(BC-0,BC-1)
     *       Step 1: PM1 and PM2 don't have common cliques, put sl1 into list B.
     *               Storage1 and Storage2 have common cliques BC-0, put sl2 into list A.
     *               At the end of step 1, A is of size 1 and B is of size 1.
     *       Step 2: For sl2 (the only sl in list A), check if there exists a sl in list B that
     *               satisfies the condition. sl1 is the only sl in list B in this case.
     *               Since Storage1, Storage2 and PM1 have common clique BC-0 and Storage1, Storage2
     *               and PM2 do not have common cliques, we don't generate a move action for sl2.
     *       Step 3: The rest of sls are sl1 and sl2. So we generate a compoundMove for them.
     * For more examples, see the review request for bug OM-44059.
     *
     * @param economy the economy
     * @param shoppingLists list of shoppingLists
     * @param currentSuppliers list of current suppliers of shoppingLists
     * @param bestSellers list of future suppliers of shoppingLists
     * @param actions list of actions
     * @param importance importance of the action
     * @param contextList list of contexts associated with each shopping list
     */
    public static void generateCompoundMoveOrMoveAction(Economy economy, List<ShoppingList> shoppingLists,
        List<Trader> currentSuppliers, List<Trader> bestSellers, List<Action> actions, double importance,
        List<Optional<EconomyDTOs.Context>> contextList) {
        // step 1
        int numOfOriginalActions = actions.size();
        List<Integer> slsWithCommonCliques = new ArrayList<>(bestSellers.size());
        List<Integer> slsWithoutCommonCliques = new ArrayList<>(bestSellers.size());
        List<Set<Long>> currentSupplierCliques = new ArrayList<>(bestSellers.size());
        List<Set<Long>> commonCliques = new ArrayList<>(bestSellers.size());
        // Count the number of sls without common cliques between the currentSupplier and bestSeller.
        // Save the common cliques between the currentSupplier and bestSeller.
        for (int i = 0; i < bestSellers.size(); i++) {
            Set<Long> bestSellerClique = bestSellers.get(i).getCliques();
            // currentSupplier can be null
            Set<Long> currentSupplierClique = Optional.ofNullable(currentSuppliers.get(i))
                .map(Trader::getCliques).orElse(Collections.emptySet());
            Set<Long> commonClique = Sets.intersection(bestSellerClique, currentSupplierClique);
            currentSupplierCliques.add(currentSupplierClique);
            commonCliques.add(commonClique);
            if (commonClique.size() == 0) {
                slsWithoutCommonCliques.add(i);
            } else {
                slsWithCommonCliques.add(i);
            }
        }

        boolean isDebugTrader = shoppingLists.get(0).getBuyer().isDebugEnabled();
        if (logger.isTraceEnabled() || isDebugTrader) {
            logger.info("Index of shoppingList with common cliques: {}", slsWithCommonCliques);
            logger.info("Index of shoppingList without common cliques: {}", slsWithoutCommonCliques);
        }

        // Check if we need to generate a move action for sls with common cliques.
        int numOfMoveActions = 0;
        List<Integer> compoundMoveCandidates = new ArrayList<>(bestSellers.size());
        for (Integer i : slsWithCommonCliques) {
            // If the shopping list is a leader (group factor is > 1), then we always want to
            // produce the move, even if the current supplier is the same as the best seller.
            if (currentSuppliers.get(i) == bestSellers.get(i) && shoppingLists.get(i).getGroupFactor() <= 1) {
                continue;
            }
            // step 2
            Set<Long> commonClique = commonCliques.get(i);
            boolean generateMoveAction = slsWithoutCommonCliques.stream().anyMatch(j ->
                Sets.intersection(commonClique, bestSellers.get(j).getCliques()).size() > 0 &&
                Sets.intersection(commonClique, currentSupplierCliques.get(j)).size() > 0);
            // Generate a move action if
            // all sources and destinations are in a common biclique or generateMoveAction is true.
            if (slsWithoutCommonCliques.size() == 0 || generateMoveAction) {
                actions.add(new Move(economy, shoppingLists.get(i), currentSuppliers.get(i),
                        bestSellers.get(i), contextList.isEmpty() ? Optional.empty()
                                : contextList.get(i)).take().setImportance(importance));
                numOfMoveActions++;
            } else {
                compoundMoveCandidates.add(i);
            }
        }

        // step 3
        // Decide if we should generate a compoundMove action for sls in slsWithoutCommonCliques.
        slsWithoutCommonCliques.addAll(compoundMoveCandidates);
        int numOfCompoundMoveActions = 0;
        if (slsWithoutCommonCliques.size() > 1) {
            CompoundMove compoundMove;
            if (contextList.isEmpty()) {
                compoundMove = CompoundMove.createAndCheckCompoundMoveWithExplicitSources(economy,
                        slsWithoutCommonCliques.stream().map(shoppingLists::get).collect(Collectors.toList()),
                        slsWithoutCommonCliques.stream().map(currentSuppliers::get).collect(Collectors.toList()),
                        slsWithoutCommonCliques.stream().map(bestSellers::get).collect(Collectors.toList()));
            } else {
                compoundMove = CompoundMove.createAndCheckCompoundMoveWithExplicitSources(economy,
                        slsWithoutCommonCliques.stream().map(shoppingLists::get).collect(Collectors.toList()),
                        slsWithoutCommonCliques.stream().map(currentSuppliers::get).collect(Collectors.toList()),
                        slsWithoutCommonCliques.stream().map(bestSellers::get).collect(Collectors.toList()),
                        contextList);
            }
            actions.add(compoundMove.take().setImportance(importance));
            numOfCompoundMoveActions++;
        } else if (slsWithoutCommonCliques.size() == 1) {
            int i = slsWithoutCommonCliques.get(0);
            actions.add(new Move(economy, shoppingLists.get(i), currentSuppliers.get(i),
                    bestSellers.get(i), contextList.isEmpty() ? Optional.empty() : contextList.get(i))
                            .take().setImportance(importance));
            numOfMoveActions++;
        }
        if (logger.isTraceEnabled() || isDebugTrader) {
            String buyerDebugInfo = shoppingLists.get(0).getBuyer().getDebugInfoNeverUseInCode();
            IntStream.range(numOfOriginalActions, numOfOriginalActions + numOfMoveActions).forEach(i ->
                logger.info("A new Move from {} to {} with context {} is generated.",
                    printTraderDetail(((Move)actions.get(i)).getSource()),
                    printTraderDetail(((Move)actions.get(i)).getDestination()),
                    ((Move)actions.get(i)).getContext().isPresent()
                    ? ((Move)actions.get(i)).getContext().get() : ""));
            IntStream.range(numOfOriginalActions + numOfMoveActions, actions.size()).forEach(i ->
                logger.info("A new CompoundMove from {} to {} with context {} is generated.",
                    ((CompoundMove)actions.get(i)).getConstituentMoves().stream().map(move ->
                        printTraderDetail(move.getSource())).collect(Collectors.toList()),
                    ((CompoundMove)actions.get(i)).getConstituentMoves().stream().map(move ->
                        printTraderDetail(move.getDestination())).collect(Collectors.toList()),
                    ((CompoundMove)actions.get(i)).getConstituentMoves().stream().map(move ->
                    move.getContext().isPresent() ? move.getContext().get() : "")
                    .collect(Collectors.toList())));
            logger.info("{} Move, {} CompoundMove were generated for trader {}.",
                numOfMoveActions, numOfCompoundMoveActions, buyerDebugInfo);
        }
    }

    /**
     * Return the debug info of trader if it's not null. Otherwise, return empty.
     *
     * @param trader The trader
     * @return Trader info
     */
    public static String printTraderDetail(Trader trader) {
        return Optional.ofNullable(trader).map(Trader::getDebugInfoNeverUseInCode).orElse("");
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

    /**
     * Compute the best quote for the given trader.
     *
     * @param economy the economy.
     * @param trader the buying trader.
     * @return the best CliqueMinimizer.
     */
    public static @Nullable CliqueMinimizer computeBestQuote(Economy economy, Trader trader) {
        Set<Long> commonCliques = economy.getCommonCliques(trader);
        if (commonCliques.isEmpty()) {
            return null;
        }

        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.moveableSlByMarket(trader);
        // check if the trader buys in market that all sellers have a cost function
        boolean shopByBugdet = movableSlByMarket.stream().map(Entry::getValue)
                .map(Market::getActiveSellers)
                .flatMap(List::stream)
                .allMatch(t -> t.getSettings().getCostFunction() != null);
        Optional<Context> origContext = trader.getSettings().getContext();
        // when trader shop by budget, it could be a trader without any context(on prem/cloud workloads
        // shop for cloud provider, when no business account is assigned by plan configuration),
        // or it could be a trader with context partially specified(on prem/cloud workloads shop for
        // cloud provider, when a business account is assigned by plan configuration).
        // NOTE: we have to make sure the cloud to cloud migration plan unset workloads' original
        // region and business account when creating traderDTO in TopologyConverter.
        if (shopByBugdet && (!origContext.isPresent() || origContext.get().getBalanceAccount() == null // on prem entity
                || origContext.get().getRegionId() == -1L)) { // cloud entity without region
            Set<Context> contextCombination = populateContextCombination(movableSlByMarket
                    .stream().map(Entry::getValue).collect(Collectors.toSet()), economy);
            if (logger.isDebugEnabled()) {
                logger.debug("Buying trader {} has possible context combination: {}, parentId {}",
                        trader.getDebugInfoNeverUseInCode(), contextCombination.stream()
                        .collect(Collectors.toSet()), contextCombination.stream()
                        .map(c -> c.getBalanceAccount().getParentId()).collect(Collectors.toSet()));
            }
            CliqueMinimizer bestMinimizer = null;
            for (Context context : contextCombination) {
                // assign a context to buying trader at a time so that it shop by the same context
                // in all markets. For a buying trader, it should have at most 1 context in contextlist.
                // move.internaltake will reset context to shopping list with the one associated with
                // bestMinimizer, but not touch the context on trader settings.
                // move.updateQuantities for UPDATE_COUPON_COMM will need the trader setting to have a context
                trader.getSettings().setContext(context);
                // quoteCache is useful for on prem, but cloud quote may not need it.
                // 1. Every context will give out a different price so that quote is different for
                // different buying context on same seller. 2. The number of cliques in cloud are a
                // lot less than on prem because of the static infrastructure in AWS/Azure
                CliqueMinimizer minimizer = commonCliques.stream()
                        .collect(() -> new CliqueMinimizer(economy, movableSlByMarket, null),
                        CliqueMinimizer::accept, CliqueMinimizer::combine);
                if (bestMinimizer == null || minimizer.getBestTotalQuote() < bestMinimizer.getBestTotalQuote()) {
                    bestMinimizer = minimizer;
                }
            }
            if (logger.isDebugEnabled()) {
                boolean noBestMinimizer = bestMinimizer == null
                        || bestMinimizer.getBestSellers() == null;
                logger.debug("Buying trader {} has best sellers {} with context {}",
                        trader.getDebugInfoNeverUseInCode(),
                        noBestMinimizer ? "N/A" : bestMinimizer.getBestSellers()
                                .stream()
                                .map(Trader::getDebugInfoNeverUseInCode)
                                .collect(Collectors.toList()),
                        noBestMinimizer ? "N/A" : bestMinimizer.getShoppingListContextMap()
                                .values()
                                .iterator()
                                .next()
                                .get());
            }
            return bestMinimizer;
        } else {
            final QuoteCache cache = economy.getSettings().getUseQuoteCacheDuringSNM() && commonCliques.size() > 1
                    ? new QuoteCache(economy.getTraders().size(), economy.getMarketsAsBuyer(trader)
                            .values().stream().mapToInt(market -> market.getActiveSellers().size()).sum(),
                                    economy.getMarketsAsBuyer(trader).size()) : null;
           return commonCliques.stream().collect(() -> new CliqueMinimizer(economy, movableSlByMarket, cache),
                    CliqueMinimizer::accept, CliqueMinimizer::combine);
        }
    }

    /**
     * Populate a set of context that is present in all the given markets.
     *
     * @param markets a list of markets provided.
     * @param economy the economy.
     * @return a set of context.
     */
    @VisibleForTesting
    protected static Set<Context> populateContextCombination(@NonNull Set<Market> markets,
                                                             @NonNull Economy economy) {
        // for each market, we find their complete set of context list by getting context from each
        // active seller, then find intersection of contexts from all markets.
        // NOTE: two contexts are considered equal if the balance account contents are the same
        // and region ids are the same. In the final common contexts that we return, we set the
        // parent context id if there was a match based on the account id.
        if (logger.isDebugEnabled()) {
            for (Market m : markets) {
                Set<Context> sets = m.getActiveSellers().stream()
                                .map(t -> economy.getTraderWithContextMap().get(t))
                                .filter(contextList -> contextList != null && !contextList.isEmpty())
                                .flatMap(Collection::stream)
                                .collect(Collectors.toSet());
                logger.debug("Market basket {}", m.getBasket());
                for (Context c : sets) {
                    logger.debug("Region id " + c.getRegionId() + " Ba id " + c.getBalanceAccount().getId()
                                + " parent id " + c.getBalanceAccount().getParentId());
                }
            }
        }
        return markets.stream()
                .map(m -> m.getActiveSellers().stream()
                        .map(t -> economy.getTraderWithContextMap().get(t))
                        .filter(contextList -> contextList != null && !contextList.isEmpty())
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet()))
                .reduce(mergeContextSets)
                .orElse(Collections.emptySet());
    }

    /**
     * Return true if the trader is active and any of its shopping lists are movable. If any
     * shoppingList has a group factor of 0, the trader should not be considered, because another
     * trader will be shopping on its behalf.
     * @param economy the economy that the trader belongs to
     * @param trader the trader
     * @return whether the trader is active and has any movable shopping list
     */
    public static boolean shouldConsiderTraderForShopTogether(Economy economy, Trader trader) {
        if (!trader.getState().isActive()) {
            return false;
        }
        boolean isMovable = false;
        for (ShoppingList sl : economy.getMarketsAsBuyer(trader).keySet()) {
            if (sl.getGroupFactor() == 0) {
                return false;
            }
            isMovable |= sl.isMovable();
        }
        return isMovable;
    }

    /**
     * Compute the current quote for a trader.
     *
     * @param economy the economy the trader associates with
     * @param movableSlByMarket the movable shopping list of a buyer and its participating market
     * @return the current quote given by current supplier
     */
    public static double computeCurrentQuote(Economy economy,
                    Collection<Entry<@NonNull ShoppingList, @NonNull Market>> movableSlByMarket) {
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
