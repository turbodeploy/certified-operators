package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;

/*
 * This class contains the implementation for generating the provision actions to satisfy
 * unplaced demand in an economy.
 *
 * Here is how we deal with unplaced demand and infinite quotes:
 *
 * 1) Unplaced demand
 *    -If we can place this on any of the existing suppliers, we do that
 *    -Else, we provision the best supplier that can fit this buyer
 *
 *      >If we can FIT this buyer on the clone of one of the existing sellers we clone the seller.
 *      >If none of the sellers can fit this entity, we create a clone that looks like a customized
 *        version of an existing seller.
 *      >If there are no sellers in this market, we generate a reconfigure message.
 *
 * 2) Infinite quotes
 *    -We check if a clone of the current supplier can fit this buyer.
 *    -If not, we try cloning the current supplier and check if it can fit the demand. If not, try
 *      the other sellers in the market. If none fits the demand, provision a new seller large
 *      enough to fit the demand.
 *
 * @author shravan
 *
 */
public class BootstrapSupply {

    static final Logger logger = LogManager.getLogger(BootstrapSupply.class);

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set
     * upper limits.
     *
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @return list of actions that might include provision, move and reconfigure.
     */
    public static @NonNull List<@NonNull Action>
                    bootstrapSupplyDecisions(@NonNull Economy economy) {
        List<@NonNull Action> allActions = new ArrayList<@NonNull Action>();
        allActions.addAll(shopTogetherBootstrap(economy));
        allActions.addAll(nonShopTogetherBootstrap(economy));
        return allActions;
    }

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set upper limits,
     * when shop-together is enabled.
     *
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @return a list of actions that needed to generate supply
     */
    protected static @NonNull List<@NonNull Action> shopTogetherBootstrap(Economy economy) {
        List<@NonNull Action> allActions = new ArrayList<@NonNull Action>();
        Map<ShoppingList, Long> slsThatNeedProvBySupply = new HashMap<>();
        int tradesSize = economy.getShopTogetherTraders().size();
        // Go through all buyers
        // We may need to add some items in the economy.getTraders() list,
        // so we can not use iterator to go through all items
        for (int idx = 0; idx < tradesSize; idx++) {
            if (economy.getForceStop()) {
                return allActions;
            }
            allActions.addAll(shopTogetherBootstrapForIndividualBuyer(economy,
                                                                      economy
                                                                      .getShopTogetherTraders()
                                                                      .get(idx),
                                                                      slsThatNeedProvBySupply));
        }
        // process shoppingLists in slsThatNeedProvBySupplyList and generate provisionBySupply
        allActions.addAll(processCachedShoptogetherSls(economy, slsThatNeedProvBySupply));
        return allActions;
    }

    /**
     * Process the shopping lists that suppose to trigger provisions by iterating
     * {@link slsThatNeedProvBySupply} and checking if there is enough supply to place
     * the shopping list. If there is enough supply move the shopping list to best trader.
     * Otherwise, generate provision actions to add supply.
     *
     * @param economy the economy
     * @return a list of actions generated
     */
    public static @NonNull List<Action> processCachedShoptogetherSls(Economy economy,
                                                                     Map<ShoppingList, Long>
                                                                     slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        for (Entry<ShoppingList, Long> entry : slsThatNeedProvBySupply.entrySet()) {
            ShoppingList sl = entry.getKey();
            Market market = economy.getMarket(sl);
            @NonNull List<@NonNull Trader> sellers = market.getCliques().get(entry.getValue());
            @NonNull Stream<@NonNull Trader> stream =
                            sellers.size() < economy.getSettings().getMinSellersForParallelism()
                            ? sellers.stream() : sellers.parallelStream();
            @NonNull QuoteMinimizer minimizer = stream
                            .collect(() -> new QuoteMinimizer(economy, sl), QuoteMinimizer::accept,
                                     QuoteMinimizer::combine);
            if (Double.isInfinite(minimizer.getBestQuote())) {
                // we use only ActiveSellersAvailableForPlacement for finding possible provider
                Trader sellerThatFits =
                                findTraderThatFitsBuyer(entry.getKey(),
                                                        market.getActiveSellersAvailableForPlacement(),
                                                        market);
                if (sellerThatFits != null) {
                    Map<@NonNull ShoppingList, @NonNull Trader> newSuppliers = new HashMap<>();
                    newSuppliers.put(sl, provisionOrActivateTrader(sellerThatFits, market,
                                                                   allActions, economy));
                    final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
                    slByMarket = economy.getMarketsAsBuyer(sl.getBuyer()).entrySet();
                    final @NonNull @ReadOnly List<Entry<@NonNull ShoppingList, @NonNull Market>>
                    movableSlByMarket = slByMarket.stream().filter(entry1 ->
                    entry1.getKey().isMovable()).collect(Collectors.toList());
                    CompoundMove compoundMove = createCompoundMove(newSuppliers, movableSlByMarket, economy);
                    if (compoundMove != null) {
                        allActions.add(compoundMove);
                    }
                } else {
                    @NonNull Trader buyer = entry.getKey().getBuyer();
                    if (buyer.isDebugEnabled()) {
                        logger.debug("Quote is infinity, and unable to find a seller that will fit the trader: {}",
                                     buyer.getDebugInfoNeverUseInCode());
                    }
                }
            }
        }
        return allActions;
    }

    /**
     * Create enough supply through ProvisionByDemand if needed to place a buyer at utilization levels
     * that comply to user-set upper limits, when shop-together is enabled.
     *
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @param buyingTrader the trader whose placement will be evaluated
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions that needed to generate supply
     */
    public static @NonNull List<@NonNull Action>
                    shopTogetherBootstrapForIndividualBuyer(Economy economy, Trader buyingTrader,
                                                            Map<ShoppingList, Long>
                                                            slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>> slByMarket =
                        economy.getMarketsAsBuyer(buyingTrader).entrySet();
        final @NonNull @ReadOnly List<Entry<@NonNull ShoppingList, @NonNull Market>> movableSlByMarket =
                        slByMarket.stream().filter(entry -> entry.getKey().isMovable())
                                        .collect(Collectors.toList());
        if (!Placement.shouldConsiderTraderForShopTogether(buyingTrader, movableSlByMarket)) {
            return allActions;
        }
        Set<Long> commonCliques = Placement.getCommonCliques(buyingTrader, slByMarket, movableSlByMarket);
        CliqueMinimizer minimizer =
                        Placement.computeBestQuote(economy, movableSlByMarket, commonCliques);
        if (minimizer == null) {
            return allActions;
        }
        if (!Double.isFinite(minimizer.getBestTotalQuote())) {
            // if the best quote is not finite then we should provision sellers to accommodate
            // shopping lists' requests
            // TODO: we now use the first clique in the commonCliques set, it may not be
            // the best decision
            List<Action> provisionAndSubsequentMove = checkAndApplyProvisionForShopTogether (economy,
                                movableSlByMarket, commonCliques.iterator().next(),
                                slsThatNeedProvBySupply);
            allActions.addAll(provisionAndSubsequentMove);
        } else {
            List<ShoppingList> shoppingLists = new ArrayList<>();
            boolean areAllBestSellerSameAsSupplier = true;
            // find if the best seller same as supplier, as the movableSlByMarket
            // is a list and will be passed to minimizers in order, the bestSeller field of
            // cliqueMinimizer will also main the order corresponding to the movableSlByMarket
            for (int i = 0; i < movableSlByMarket.size(); i++) {
                ShoppingList sl = movableSlByMarket.get(i).getKey();
                shoppingLists.add(sl);
                // in cases which the sl is not placed, or its supplier is not the same as
                // best seller, we make areAllBestSellerSameAsSupplier false so that a move
                // will be generated
                if (sl.getSupplier() == null ||
                                !sl.getSupplier().equals(minimizer.getBestSellers().get(i))) {
                    areAllBestSellerSameAsSupplier = false;
                }
            }
            if (!areAllBestSellerSameAsSupplier) {
                // if the best quote is finite and at least one shopping list has a best seller
                // that is not its current supplier, trigger a shop together move
                CompoundMove compoundMove =
                                        CompoundMove.createAndCheckCompoundMoveWithImplicitSources(
                                                economy, shoppingLists, minimizer.getBestSellers());
                if (compoundMove != null) {
                    allActions.add(compoundMove.take());
                }
            }
        }
        return allActions;
    }

    /**
     * If there is a trader that can be reactivated or cloned through ProvisionBySupply to fit buyer
     * that is doing shop together, populate the slsThatNeedProvBySupply list with the buyer that
     * will be processed later or Provision {@link Trader} that can fit the buyer through
     * {@link ProvisionBySupply} and make the buyer consume from the trader
     *
     * @param economy economy the {@Link Economy} for which we want to guarantee enough supply.
     * @param movableSlByMarket the list of movable shopping lists to market
     * @param commonClique the clique which restrains the sellers that the buyer can buy from
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions related to add supply
     */
    public static @NonNull List<@NonNull Action> checkAndApplyProvisionForShopTogether (
                    Economy economy,
                    @NonNull @ReadOnly List<Entry<@NonNull ShoppingList, @NonNull Market>>
                    movableSlByMarket, long commonClique,
                    Map<ShoppingList, Long> slsThatNeedProvBySupply) {
        List<Action> provisionedRelatedActions = new ArrayList<>();
        // find the market in which the shoppinglist gets an infinite quote, then find a satisfying
        // trader to provision
        Map<@NonNull ShoppingList, @NonNull Trader> newSuppliers = new HashMap<>();
        for (Entry<@NonNull ShoppingList, @NonNull Market> entry : movableSlByMarket) {
            ShoppingList sl = entry.getKey();
            Market market = entry.getValue();
            @NonNull List<@NonNull Trader> sellers = market.getCliques().get(commonClique);
            // consider just sellersAvailableForPlacement
            sellers.retainAll(market.getActiveSellersAvailableForPlacement());
            @NonNull Stream<@NonNull Trader> stream =
                            sellers.size() < economy.getSettings().getMinSellersForParallelism()
                                            ? sellers.stream() : sellers.parallelStream();
            @NonNull
            QuoteMinimizer minimizer = stream.collect(() -> new QuoteMinimizer(economy, sl),
                            QuoteMinimizer::accept, QuoteMinimizer::combine);
            // if for a given shoppinglist, the quote is infinity, we need to add supply for it
            if (Double.isInfinite(minimizer.getBestQuote())) {
                Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, market);
                Action action;
                Trader newSeller;
                // provision by supply
                if (sellerThatFits != null) {
                    // log shoppingLists that need ProvisionBySupply in slsThatNeedProvBySupply
                    slsThatNeedProvBySupply.put(sl, commonClique);
                } else { // provision by demand
                    // TODO: maybe pick a better seller instead of the first one
                    List<Trader> clonableSellers = sellers.stream().filter(s ->
                            s.getSettings().isCloneable()).collect(Collectors.toList());
                    if (clonableSellers.isEmpty()) {
                        logger.warn("No clonable trader can be found in market though buyer " +
                                    sl.getBuyer().getDebugInfoNeverUseInCode()
                                    + " has an infinity quote");
                    } else {
                        action = new ProvisionByDemand(economy, sl, clonableSellers.get(0)).take();
                        ((ActionImpl)action).setImportance(Double.POSITIVE_INFINITY);
                        newSeller = ((ProvisionByDemand)action).getProvisionedSeller();
                        // provisionByDemand does not place the provisioned trader. We try finding
                        // best placement for it, if none exists, we create one supply for provisioned trader
                        provisionedRelatedActions
                                        .addAll(shopTogetherBootstrapForIndividualBuyer(economy,
                                                                                        newSeller,
                                                                                        slsThatNeedProvBySupply));
                        provisionedRelatedActions.add(action);
                        newSuppliers.put(sl, newSeller);
                    }
                }
            }
        }
        if (!newSuppliers.isEmpty()) {
            // do a compoundMove
            CompoundMove compoundMove = createCompoundMove(newSuppliers, movableSlByMarket,
                                                                                        economy);
            if (compoundMove != null) {
                provisionedRelatedActions.add(compoundMove);
            }
        }
        return provisionedRelatedActions;
    }

    /**
     * Generate a compound move recommendation to a list of destinations
     *
     * @param newSuppliers Map containing a new destination for every {@Link ShoppingList}
     * @param movableSlByMarket the list of movable shopping lists to market
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @return a {@link CompoundMove} action
     */
    private static @Nullable CompoundMove createCompoundMove(
                                        Map<@NonNull ShoppingList, @NonNull Trader> newSuppliers,
                                        @NonNull @ReadOnly List<Entry<@NonNull ShoppingList,
                                        @NonNull Market>> movableSlByMarket, Economy economy) {
        List<@NonNull Trader> destinations = new ArrayList<>();
        List<@NonNull ShoppingList> movableSlList = new ArrayList<>();
        for(Entry<@NonNull ShoppingList, @NonNull Market> slPerMkt : movableSlByMarket) {
            @NonNull ShoppingList sl = slPerMkt.getKey();
            if (newSuppliers.containsKey(sl)) {
                destinations.add(newSuppliers.get(sl));
            } else {
                destinations.add(sl.getSupplier());
            }
            movableSlList.add(sl);
        }

        CompoundMove compoundMove = CompoundMove.createAndCheckCompoundMoveWithImplicitSources(
                                                            economy, movableSlList, destinations);
        if (compoundMove != null) {
            return compoundMove.take();
        } else {
            return null;
        }
    }

    /**
     * Generate a activate or provisionBySupply recommendation
     *
     * <p>
     *  as a side-effect, the provision or activate action is added to the list of actions passed
     *  as an argument
     * </p>
     *
     * @param sellerThatFits is the {@Link Trader} that when cloned can fit the buyer
     * @param market is the {@link Market} in which the sellerThatFits sells
     * @param actions the list of {@Link Action}s generated during bootstrap
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @return a {@link Trader} the activated/provisioned trader
     */
    private static Trader provisionOrActivateTrader (Trader sellerThatFits,
                        Market market, List<Action> actions, Economy economy) {
        Action action;
        Trader newSeller;
        // clone one of the sellers or reactivate an inactive seller that the VM can fit in
        if (sellerThatFits.getState() == TraderState.ACTIVE) {
            action = new ProvisionBySupply(economy, sellerThatFits).take();
            newSeller = ((ProvisionBySupply)action).getProvisionedSeller();
            actions.addAll(((ProvisionBySupply)action).getSubsequentActions());
        } else {
            action = new Activate(economy, sellerThatFits, market, sellerThatFits)
                            .take();
            newSeller = sellerThatFits;
        }
        ((ActionImpl)action).setImportance(Double.POSITIVE_INFINITY);
        actions.add(action);
        return newSeller;
    }

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set
     * upper limits, when shop-together is disabled
     *
     * @param economy the {@Link Economy} for which we want to guarantee enough supply.
     * @return a list of actions that needed to generate supply
     */
    protected static @NonNull List<@NonNull Action> nonShopTogetherBootstrap(Economy economy) {
        List<@NonNull Action> allActions = new ArrayList<@NonNull Action>();
        Map<ShoppingList, Long> slsThatNeedProvBySupply = new HashMap<>();
        // copy the markets from economy and use the copy to iterate, because in
        // the provision logic, we may add new basket which result in new market
        List<Market> orignalMkts = new ArrayList<>();
        orignalMkts.addAll(economy.getMarkets());
        for (Market market : orignalMkts) {
            if (economy.getForceStop()) {
                return allActions;
            }
            // do not provision traders in markets where guaranteedBuyers are unplaced
            if (market.getBuyers().stream()
                            .allMatch(sl -> sl.getBuyer().getSettings().isGuaranteedBuyer())) {
                continue;
            }
            for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                allActions.addAll(nonShopTogetherBootStrapForIndividualBuyer(economy, shoppingList,
                                                                             market,
                                                                             slsThatNeedProvBySupply));
            }
        }
        // process shoppingLists in slsThatNeedProvBySupplyList and generate provisionBySupply
        allActions.addAll(processSlsThatNeedProvBySupply(economy, slsThatNeedProvBySupply));
        return allActions;
    }

    /**
     * Bootstrap for individual buyer to see if more supply needs to be added.
     *
     * @param economy the economy
     * @param shoppingList the shopping list of the buyer
     * @param market the market the shopping list buys from
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions
     */
    public static @NonNull List<Action>
           nonShopTogetherBootStrapForIndividualBuyer(@NonNull Economy economy,
                                                      @NonNull ShoppingList shoppingList,
                                                      @NonNull Market market,
                                                      Map<ShoppingList, Long>
                                                      slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        // below is the provision logic for non shop together traders, if the trader
        // should shop together, skip the logic
        if (shoppingList.getBuyer().getSettings().isShopTogether()
                || !shoppingList.isMovable()) {
            return allActions;
        }
        List<Trader> sellers = market.getActiveSellersAvailableForPlacement();
        // find the bestQuote
        final QuoteMinimizer minimizer =
                (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                        ? sellers.stream() : sellers.parallelStream())
                        .collect(() -> new QuoteMinimizer(economy, shoppingList),
                                QuoteMinimizer::accept, QuoteMinimizer::combine);

        // unplaced buyer
        if (shoppingList.getSupplier() == null) {
            if (Double.isFinite(minimizer.getBestQuote())) {
                // on getting finiteQuote, move unplaced Trader to the best provider
                allActions.add(new Move(economy, shoppingList, minimizer.getBestSeller())
                        .take().setImportance(Double.POSITIVE_INFINITY));
            } else {
                // on getting an infiniteQuote, provision new Seller and move unplaced Trader to it
                allActions.addAll(checkAndApplyProvision(economy, shoppingList, market,
                                                         slsThatNeedProvBySupply));
            }
        } else {
            // already placed Buyer
            if (Double.isInfinite(minimizer.getBestQuote())) {
                // Start by cloning the best provider that can fit the buyer. If none can fit
                // the buyer, provision a new seller large enough to fit the demand.
                allActions.addAll(checkAndApplyProvision(economy, shoppingList, market,
                                                         slsThatNeedProvBySupply));
            } else if (Double.isInfinite(minimizer.getCurrentQuote()) &&
                    minimizer.getBestSeller() != shoppingList.getSupplier()) {
                // If we have a seller that can fit the buyer getting an infiniteQuote,
                // move buyer to this provider
                allActions.add(new Move(economy, shoppingList, minimizer.getBestSeller())
                        .take().setImportance(minimizer.getCurrentQuote()));
            }
        }
        return allActions;
    }

    /**
     * Iterating slsThatNeedProvBySupply to find the best placement for it, if there is need to
     * add more supply, generate a provision action and take it. If there is no such need,
     * generate a move action and take it.
     *
     * @param economy the economy the shopping lists are in
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions generated
     */
    public static @NonNull List<Action> processSlsThatNeedProvBySupply(Economy economy,
                                                                       Map<ShoppingList, Long>
                                                                       slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        for (ShoppingList sl : slsThatNeedProvBySupply.keySet()) {
            // find the bestQuote
            Market market = economy.getMarket(sl);
            List<Trader> sellers = market.getActiveSellersAvailableForPlacement();
            final QuoteMinimizer minimizer =
                    (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                            ? sellers.stream() : sellers.parallelStream())
                            .collect(() -> new QuoteMinimizer(economy, sl),
                                    QuoteMinimizer::accept, QuoteMinimizer::combine);

            if (Double.isInfinite(minimizer.getBestQuote())) {
                // on getting an infiniteQuote, provision new Seller and move unplaced Trader to it
                // clone one of the sellers or reactivate an inactive seller that the VM can fit in
                Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, market);
                if (sellerThatFits != null) {
                    Trader provisionedSeller = provisionOrActivateTrader(sellerThatFits, market,
                            allActions, economy);
                    allActions.add(new Move(economy, sl, provisionedSeller).take()
                            .setImportance(Double.POSITIVE_INFINITY));
                } else {
                    @NonNull Trader buyer = sl.getBuyer();
                    if (buyer.isDebugEnabled()) {
                        logger.debug("Quote is infinity, and unable to find a seller that will fit the trader: {}",
                                buyer.getDebugInfoNeverUseInCode());
                    }
                }
            } else {
                if (minimizer.getBestSeller() != sl.getSupplier()) {
                    allActions.add(new Move(economy, sl, minimizer.getBestSeller()).take()
                            .setImportance(minimizer.getCurrentQuote()
                                    - minimizer.getBestQuote()));
                }
            }
        }
        return allActions;
    }

    /**
     * If there is a trader that can be reactivated or cloned through ProvisionBySupply to fit buyer,
     * populate the slsThatNeedProvBySupply list with the buyer that will be processed later or Provision
     * {@link Trader} that can fit the buyer through {@link ProvisionBySupply} and make the buyer
     * consume from the trader
     *
     * @param economy the {@Link Economy} that contains the unplaced {@link Trader}
     * @param shoppingList is the {@Link ShoppingList} of the unplaced trader
     * @param market is the market containing the inactiveSellers
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     *
     * @return list of actions that might include provision, move and reconfigure.
     */
    private static List<Action> checkAndApplyProvision(Economy economy, ShoppingList shoppingList,
                                                       Market market,
                                                       Map<ShoppingList, Long> slsThatNeedProvBySupply) {
        List<@NonNull Action> actions = new ArrayList<>();
        if (economy.getForceStop()) {
            return actions;
        }
        List<Action> provisionRelatedActionList = new ArrayList<>();
        Action bootstrapAction;
        Trader provisionedSeller;
        List<Trader> activeSellers = market.getActiveSellersAvailableForPlacement();
        // Return if there are active sellers and none of them are cloneable
        if (!activeSellers.isEmpty() && activeSellers.stream()
                        .filter(seller -> seller.getSettings().isCloneable()).count() == 0) {
            return actions;
        }
        Trader sellerThatFits = findTraderThatFitsBuyer (shoppingList, activeSellers, market);
        if (sellerThatFits != null) {
            // log shoppingLists that need ProvisionBySupply in slsThatNeedProvBySupply
            slsThatNeedProvBySupply.put(shoppingList, new Long(0));
            return Collections.emptyList();
        } else if (!activeSellers.isEmpty()) {
            // if none of the existing sellers can fit the shoppingList, provision customSeller
            // TODO: maybe pick a better seller to base the clone out off
            bootstrapAction = new ProvisionByDemand(economy, shoppingList, activeSellers.get(0))
                                    .take();
            ((ActionImpl)bootstrapAction).setImportance(Double.POSITIVE_INFINITY);
            provisionedSeller = ((ProvisionByDemand)bootstrapAction).getProvisionedSeller();
            provisionRelatedActionList.add(bootstrapAction);
            // provisionByDemand does not place the new newClone provisionedTrader. We try finding
            // best seller, if none exists, we create one
            economy.getMarketsAsBuyer(provisionedSeller).entrySet().forEach(entry -> {
                        ShoppingList sl = entry.getKey();
                        List<Trader> sellers = entry.getValue().getActiveSellers();
                        QuoteMinimizer minimizer =
                            (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                                     ? sellers.stream() : sellers.parallelStream())
                                         .collect(()->new QuoteMinimizer(economy,sl),
                                            QuoteMinimizer::accept, QuoteMinimizer::combine);
                        // If quote is infinite, we create a new provider
                        if (Double.isInfinite(minimizer.getBestQuote())) {
                            provisionRelatedActionList.addAll(checkAndApplyProvision(economy,
                                            sl, entry.getValue(), slsThatNeedProvBySupply));
                        } else {
                            // place the shopping list of the new clone to the best supplier
                            // this is equivalent as Start in legacy market
                            if (!sl.isMovable()) {
                                sl.move(minimizer.getBestSeller());
                                Move.updateQuantities(economy, sl, minimizer.getBestSeller(),
                                                      FunctionalOperatorUtil.ADD_COMM);
                            }
                        }
            });
        } else {
            // when there is no seller in the market we could handle this through 3 approaches,
            // 1) TODO: provisionAction = new ProvisionByDemand(economy, shoppingList); OR
            // 2) consider using templates
            // 3) generating reconfigure action for now
            actions.add(new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
            // set movable false so that we dont generate duplicate reconfigure recommendations
            shoppingList.setMovable(false);
            return actions;
        }
        actions.addAll(provisionRelatedActionList);
        // Note: This move action is to place the newly provisioned trader to a proper
        // supplier.
        actions.add(new Move(economy, shoppingList, provisionedSeller).take().setImportance(Double.POSITIVE_INFINITY));
        return actions;
    }

    /**
     * Out of a list sellers, we check if, we can clone any seller in particular that can fit the buyer
     *
     * @param buyerShoppingList is the {@Link shoppingList} of the buyer
     * @param candidateSellers is the list of candidate {@link Trader sellers} to examine
     * @param market is the {@link Market} in which we try finding the best provider to reactivate/clone
     *
     * @return the any of the candidateSellers that can fit the buyer when cloned, or NULL if none
     * is big enough
     */
    private static Trader findTraderThatFitsBuyer(ShoppingList buyerShoppingList, List<Trader>
                                                  candidateSellers, Market market) {
        for (Trader seller : market.getInactiveSellers()) {
            if (canBuyerFitInSeller(buyerShoppingList, seller)) {
                return seller;
            }
        }
        for (Trader seller : candidateSellers) {
            // pick the first candidate seller that can fit the demand
            if (seller.getSettings().isCloneable() && canBuyerFitInSeller(buyerShoppingList
                            , seller)) {
                return seller;
            }
        }
        return null;
    }

    /**
     * check if the modelSeller has enough capacity for every commodity bought by a trader
     *
     * @param buyerShoppingList is the {@Link shoppingList} of the buyer
     * @param modelSeller is the {@Link Trader} that we will be checking to see if there is enough
     *                    capacity for all the commodities listed in the modelBuyer
     *
     * @return TRUE if the buyer fits in this modelSeller, FALSE otherwise
     */
    public static boolean canBuyerFitInSeller (ShoppingList buyerShoppingList, Trader modelSeller){

        Basket basket = buyerShoppingList.getBasket();
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size()
                        ; boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(modelSeller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            CommoditySold commSold = modelSeller.getCommoditiesSold().get(soldIndex);
            // eliminate the overhead from the effective capacity to make sure there is still
            // enough resource for shopping list
            double overHead = commSold.getQuantity();
            double overHeadPeak = commSold.getPeakQuantity();
            // Inactive trader usually have no customers so it will skip the loop
            for (ShoppingList sl : modelSeller.getCustomers()) {
                int index = sl.getBasket().indexOf(basketCommSpec);
                if (index != -1) {
                    overHead = overHead - sl.getQuantity(index);
                    overHeadPeak = overHeadPeak - sl.getPeakQuantity(index);
                }
            }
            if ((buyerShoppingList.getQuantities()[boughtIndex] > (commSold.getEffectiveCapacity() - overHead))
                            || (buyerShoppingList.getPeakQuantities()[boughtIndex] >
                            (commSold.getEffectiveCapacity() - overHeadPeak))) {
                return false;
            }
        }
        return true;
    }

}
