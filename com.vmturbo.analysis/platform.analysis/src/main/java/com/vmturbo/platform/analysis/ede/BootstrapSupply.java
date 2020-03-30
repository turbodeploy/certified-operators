package com.vmturbo.platform.analysis.ede;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Utility;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utilities.ProvisionUtils;

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
 * @author Shravan
 *
 */
public class BootstrapSupply {

    private BootstrapSupply() {}

    static final Logger logger = LogManager.getLogger(BootstrapSupply.class);

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set
     * upper limits.
     *
     * @param economy the {@link Economy} for which we want to guarantee enough supply.
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
     * @param economy the {@link Economy} for which we want to guarantee enough supply.
     * @return a list of actions that needed to generate supply
     */
    protected static @NonNull List<@NonNull Action> shopTogetherBootstrap(Economy economy) {
        List<@NonNull Action> allActions = new ArrayList<@NonNull Action>();
        // Mapping from Shopping List -> CliqueId
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
     * slsThatNeedProvBySupply and checking if there is enough supply to place
     * the shopping list. If there is enough supply move the shopping list to best trader.
     * Otherwise, generate provision actions to add supply.
     *
     * @param economy the economy
     * @param slsThatNeedProvBySupply Mapping from Shopping List to CliqueId. The keySet of the map
     *                                are the shopping lists that may require provisions.
     * @return a list of actions generated
     */
    public static @NonNull List<Action> processCachedShoptogetherSls(
            Economy economy, Map<ShoppingList, Long> slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        // a set to keep traders that have been placed by processing cached shopping lists in this method
        Set<Trader> traderPlaced = new HashSet<>();
        Deque<Entry<ShoppingList, Long>> shoppingListsToProcess =
            new ArrayDeque<>(slsThatNeedProvBySupply.entrySet());
        while (!shoppingListsToProcess.isEmpty()) {
            Entry<ShoppingList, Long> entry = shoppingListsToProcess.removeFirst();
            ShoppingList sl = entry.getKey();
            long cliqueId = entry.getValue();
            Trader traderToBePlaced = sl .getBuyer();
            if (traderPlaced.contains(traderToBePlaced)) {
                continue;
            }

            boolean isDebugBuyer = traderToBePlaced.isDebugEnabled();
            String buyerDebugInfo = traderToBePlaced.getDebugInfoNeverUseInCode();

            // we first compute quote with current supply to check if the trader can be
            // placed, then we provision/activate seller one at a time to see if with
            // additional supply the trader can be placed
            CliqueMinimizer minimizerWithoutSupply = Placement.computeBestQuote(economy, traderToBePlaced);
            List<Action> move = new ArrayList<>();
            if (minimizerWithoutSupply != null &&
                            Double.isInfinite(minimizerWithoutSupply.getBestTotalQuote())) {
                // there is a need to add some supply, we will add one trader to accommodate
                // the current sl
                if (logger.isTraceEnabled() || isDebugBuyer) {
                    logger.info("We have to add some supply to accommodate the trader "
                                + buyerDebugInfo + ".");
                }
                Market mkt = economy.getMarket(sl.getBasket());
                List<Trader> sellers = mkt.getCliques().get(cliqueId).stream()
                                .filter(t -> mkt.getActiveSellersAvailableForPlacement().contains(t))
                                .collect(Collectors.toList());
                Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, mkt, economy, Optional.of(cliqueId));
                if (sellerThatFits != null) {
                    Trader newSeller;
                    boolean isDebugSeller = sellerThatFits.isDebugEnabled();
                    String sellerDebugInfo = sellerThatFits.getDebugInfoNeverUseInCode();
                    // Obtain list of commodity specs causing infinite quote for the shopping list
                    List<CommoditySpecification> commSpecs = findCommSpecsWithInfiniteQuote(sellerThatFits, sl, economy);
                    if (sellerThatFits.getSettings().isResizeThroughSupplier()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("SellerThatFits is ResizeThroughSupplier: {}",
                                            sellerThatFits.getDebugInfoNeverUseInCode());
                        }
                        // If the trader is resizeThroughSupplier then generate supply provisions
                        // to provide enough capacity for the sellerThatFits to be able to host the
                        // shopping list.
                        List<Trader> newSellers = Utility.provisionSufficientSupplyForResize(economy,
                                        sellerThatFits, commSpecs, sl, allActions);
                        if (newSellers.isEmpty()) {
                            return allActions;
                        }
                        // Obtain the first provisioned seller since they are all the same.
                        newSeller = newSellers.get(0);
                    } else {
                        CommoditySpecification commSpec = commSpecs.isEmpty() ? null
                                        : commSpecs.get(0);
                        newSeller = provisionOrActivateTrader(sellerThatFits, mkt, allActions,
                                        economy, commSpec);
                    }
                    if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                        logger.info("The seller " + sellerDebugInfo + " was"
                                + (newSeller == sellerThatFits? " activated"
                                : " cloned (provision by supply)")
                                + " to accommodate " + buyerDebugInfo + ".");
                    }
                    CliqueMinimizer minimizerWithSupply = Placement
                                    .computeBestQuote(economy, traderToBePlaced);
                    // if minimizerWithSupply is finite, we can place the current buyer with
                    // provision/activate a seller, otherwise, it indicates that more than one
                    // sl from the current buyer needs additional supply and we need to continue
                    // iterating slsThatNeedProvBySupply until all sl from the same buyer
                    // has finite quote
                    if (minimizerWithSupply != null &&
                                    Double.isFinite(minimizerWithSupply.getBestTotalQuote())) {
                        if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                            logger.info("{" + buyerDebugInfo + "} is placed on the"
                                    + " activated/provisioned "
                                    + newSeller.getDebugInfoNeverUseInCode() + ".");
                        }
                        move = Placement.checkAndGenerateCompoundMoveActions(economy, traderToBePlaced,
                                minimizerWithSupply);
                    } else {
                        // One of the shopping list for the trader has an infinite quote.
                        // Find this shopping list and add it to the processing list.
                        // These shopping lists are missed in the initial slsThatNeedProvBySupply
                        // list because in the case of shop together we don't actually place the
                        // trader when creating the slsThatNeedProvBySupply list.
                        boolean hasSlWithInfiniteQuote = false;
                        for (Entry<ShoppingList, Market> slEntry :
                                    economy.getMarketsAsBuyer(traderToBePlaced).entrySet()) {
                            ShoppingList shoppingList = slEntry.getKey();
                            if (shoppingList.equals(sl) || !shoppingList.isMovable()) {
                                continue;
                            }
                            // We add it to the front of the queue for efficiency reasons. For e.g.
                            // consider the case where we are placing huge number of VMs. All the
                            // PMs are highly utilized and the attached storages' have low
                            // utilization. Since we don't actually place the traders while creating
                            // the slsThatNeedProvBySupply list, the storage will still show low
                            // utilization and will not be added to the slsThatNeedProvBySupply
                            // list. The slsThatNeedProvBySupply will only have PMs. If we add at
                            // the storage shopping list at the end of the shoppingListsToProcess
                            // queue, we will have sequence of PM SLs followed by ST SLs. If we add
                            // it at the front of the queue, the SL which was getting infinite quote
                            // will get provisioned and hence it would be able to satisfy
                            // the next set of VMs.
                            Market slMarket = slEntry.getValue();
                            if (hasSlInfiniteQuote(economy,  shoppingList, slMarket, newSeller)) {
                                hasSlWithInfiniteQuote = true;
                                shoppingListsToProcess.addFirst(
                                    new AbstractMap.SimpleImmutableEntry<>(shoppingList,
                                        cliqueId));
                                // We only add one of the SLs to prevent adding duplicate SLs
                                // to the processing queue.
                                break;
                            }
                        }
                        if (!hasSlWithInfiniteQuote) {
                            // Should be an assert or an exception.
                            logger.error("No shopping list with infinite quote for trader {}", traderToBePlaced);
                        }
                    } //end looking for infiniteQuote SL.
                } else {
                    @NonNull Trader buyer = entry.getKey().getBuyer();
                    if (logger.isTraceEnabled() || isDebugBuyer) {
                        logger.info("Quote is infinity, and unable to find a seller that"
                                + " will fit the trader {}.", buyerDebugInfo);
                    } else {
                        logger.debug("Quote is infinity, and unable to find a seller that"
                                    + " will fit the trader {}.", buyerDebugInfo);
                    }
                }
            } else if (minimizerWithoutSupply != null &&
                            Double.isFinite(minimizerWithoutSupply.getBestTotalQuote())) {
                // if minimizerWithoutSupply if finite, we may have provision actions from earlier
                // iteration that adds enough supply to place the current buyer
                move = Placement.checkAndGenerateCompoundMoveActions(economy, traderToBePlaced,
                                                             minimizerWithoutSupply);
            }
            if (!move.isEmpty()) {
                allActions.addAll(move);
                traderPlaced.add(traderToBePlaced);
            }
        }
        return allActions;
    }

    /**
     * TODO(karthik): fix javadoc
     * Extract and return the first shopping list which has infinite best quote.
     *
     * @param economy The Economy.
     * @param shoppingList ShoppingList whose quote needs to computed.
     * @param market The market in which the SL's quote has to be computed.
     * @param newSeller The newly created seller.
     */
    private static boolean hasSlInfiniteQuote(
                    final Economy economy,
                    final ShoppingList shoppingList,
                    final Market market,
                    final Trader newSeller) {

        // We need to find the intersection of the sellers of the cliques of newSeller
        // and the sellers of this shoppingList and calculate the cheapest quote of
        // these common sellers.
        final Set<Trader> sellersForSl =
                new HashSet<>(market.getActiveSellersAvailableForPlacement());
        Map<Long, List<Trader>> cliquesInMarket = market.getCliques();
        List<Trader> commonSellers =
                newSeller.getCliques()
                    .stream()
                    .flatMap(clqId ->
                            cliquesInMarket.getOrDefault(clqId,
                                    Collections.emptyList()).stream())
                    .filter(trader -> sellersForSl.contains(trader))
                    .collect(Collectors.toList());

        final QuoteMinimizer quoteMinimizer =
            commonSellers.stream()
                    .collect(() -> new QuoteMinimizer(economy,
                                        shoppingList),
                        QuoteMinimizer::accept, QuoteMinimizer::combine);

        return Double.isInfinite(quoteMinimizer.getTotalBestQuote());
    }

    /**
     * Create enough supply through ProvisionByDemand if needed to place a buyer at utilization levels
     * that comply to user-set upper limits, when shop-together is enabled.
     *
     * @param economy the {@link Economy} for which we want to guarantee enough supply.
     * @param buyingTrader the trader whose placement will be evaluated
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions that needed to generate supply
     */
    public static @NonNull List<@NonNull Action>
                    shopTogetherBootstrapForIndividualBuyer(Economy economy, Trader buyingTrader,
                                                            Map<ShoppingList,
                                                                    Long> slsThatNeedProvBySupply) {
        List<Action> allActions = new ArrayList<>();
        boolean isDebugTrader = buyingTrader.isDebugEnabled();
        String buyerDebugInfo = buyingTrader.getDebugInfoNeverUseInCode();
        if (!Placement.shouldConsiderTraderForShopTogether(economy, buyingTrader)) {
            return allActions;
        }
        Set<Long> commonCliques = economy.getCommonCliques(buyingTrader);
        CliqueMinimizer minimizer =
                        Placement.computeBestQuote(economy, buyingTrader);
        if (minimizer == null) {
            if (logger.isTraceEnabled() || isDebugTrader) {
                logger.info("{" + buyerDebugInfo + "} can't be placed in any provider.");
            }
            return allActions;
        }
        if (!Double.isFinite(minimizer.getBestTotalQuote())) {
            // if the best quote is not finite then we should provision sellers to accommodate
            // shopping lists' requests
            // TODO: we now use the first clique in the commonCliques set, it may not be
            // the best decision
            if (logger.isTraceEnabled() || isDebugTrader) {
                logger.info("New sellers have to be provisioned to accommodate the trader "
                            + buyerDebugInfo + ".");
            }
            if(commonCliques.isEmpty()){
                logger.error("No clique for Buyer: ", buyingTrader.getDebugInfoNeverUseInCode());
            }
            else {
                Long commonClique = bestCliqueForProvisioning(economy, buyingTrader, commonCliques);
                List<Action> provisionAndSubsequentMove = checkAndApplyProvisionForShopTogether(economy,
                        buyingTrader, commonClique,
                        slsThatNeedProvBySupply);
                allActions.addAll(provisionAndSubsequentMove);
            }
        } else {
            allActions.addAll(Placement.checkAndGenerateCompoundMoveActions(economy, buyingTrader,
                                                                    minimizer));
        }
        return allActions;
    }
    /**
     * Consider all cliques the buyer can buy from. Find the clique which
     * need only provision by supply. If all the cliques need a provision by
     * demand then pick the clique for which the sellers are cloneable.
     *
     * @param economy economy the {@link Economy} for which we want to guarantee enough supply.
     * @param buyer the shop-together buyer
     * @param commonCliques the set of cliques which the buyer can buy
     */
    private static Long bestCliqueForProvisioning(
            Economy economy,
            Trader buyer,
            Set<Long>  commonCliques) {
        // If none of the cliques has cloneable sellers for all
        // shopping lists then we cannot do provisioning by demand at all.
        // so returning the first clique is fine as this variable will never be updated.
        Long cliqueNeedsProvisionByDemandButClonable = commonCliques.iterator().next();
        List<Entry<ShoppingList, Market>> movableSlByMarket =
                economy.moveableSlByMarket(buyer);
        for(Long currentClique : commonCliques) {
            boolean isSelersInCliqueClonable = true;
            boolean needsProvisionByDemand = false;
            for (Entry<ShoppingList, Market> entry : movableSlByMarket) {
                ShoppingList sl = entry.getKey();
                Market market = entry.getValue();
                @NonNull List<@NonNull Trader> sellers =
                        market.getCliques().get(currentClique);
                // consider just sellersAvailableForPlacement
                sellers.retainAll(market.getActiveSellersAvailableForPlacement());
                @NonNull Stream<@NonNull Trader> stream =
                        sellers.size() < economy.getSettings().getMinSellersForParallelism()
                                ? sellers.stream() : sellers.parallelStream();
                @NonNull QuoteMinimizer minimizer = stream.collect(() -> new QuoteMinimizer(economy, sl),
                        QuoteMinimizer::accept, QuoteMinimizer::combine);
                // not all sl in movableSlByMarket needs additional supply, we should check by quote.
                // If quote is infinity, we need to cache it and add supply for it later.
                if (Double.isInfinite(minimizer.getTotalBestQuote())) {
                    Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, market, economy, Optional.of(currentClique));
                    if (sellerThatFits == null) { // Provision By Demand
                        needsProvisionByDemand = true;
                        List<Trader> clonableSellers = sellers.stream().filter(s ->
                                s.getSettings().isCloneable()).collect(Collectors.toList());
                        //if the clique1 had a host with cloneable true. and clique2 had no host with cloneable true.
                        // and we have to do provision-by-demand with clique2 we will not be able to  provision.
                        if (clonableSellers.isEmpty()) {
                            isSelersInCliqueClonable = false;
                            break;

                        }
                    }
                }
            }
            if(!needsProvisionByDemand){
                return currentClique;
            }
            if (isSelersInCliqueClonable){
                cliqueNeedsProvisionByDemandButClonable = currentClique;
            }
        }
        // We provision by demand only if we are in the last available clique and
        // all the other cliques also required provision by demand to accommodate
        // the buyer. i.e none of the existing providers could accommodate the
        // buyer.
        return cliqueNeedsProvisionByDemandButClonable;
    }
    /**
     * If there is a trader that can be reactivated or cloned through ProvisionBySupply to fit buyer
     * that is doing shop together, populate the slsThatNeedProvBySupply list with the buyer that
     * will be processed later or Provision {@link Trader} that can fit the buyer through
     * {@link ProvisionByDemand} and make the buyer consume from the seller.
     *
     * @param economy economy the {@link Economy} for which we want to guarantee enough supply.
     * @param buyer the shop-together buyer
     * @param commonClique the clique which restrains the sellers that the buyer can buy from
     * @param slsThatNeedProvBySupply the list to hold shopping lists that may require provisions
     * @return a list of actions related to add supply
     */
    public static @NonNull List<@NonNull Action> checkAndApplyProvisionForShopTogether(
                    Economy economy,
                    Trader buyer,
                    long commonClique,
                    Map<ShoppingList, Long> slsThatNeedProvBySupply) {
        List<Entry<ShoppingList, Market>> movableSlByMarket =
                economy.moveableSlByMarket(buyer);
        @NonNull List<Action> provisionedRelatedActions = new ArrayList<>();
        @NonNull Map<ShoppingList, Trader> newSuppliers = new HashMap<>();
        // The newSuppliersToIgnore set stores the traders from the newSuppliers
        // map for efficient lookup.
        @NonNull Set<Trader> newSuppliersToIgnore = new HashSet<>();
        if (movableSlByMarket.isEmpty()) {
            return provisionedRelatedActions;
        }
        List<Trader> traderList = new ArrayList<>();
        List<ShoppingList> slList = new ArrayList<>();
        // movableSlByMarket is the map for a buying trader
        for (Entry<ShoppingList, Market> entry : movableSlByMarket) {
            ShoppingList sl = entry.getKey();
            Market market = entry.getValue();
            // For a given clique, we found the sellers that associate with it. We will be using
            // those sellers to add supply in bootstrap so that the buying trader will be placed
            // only on sellers associated with this given clique to make sure it is a valid shop
            // together placement.
            @NonNull List<@NonNull Trader> sellers =
                    market.getCliques().get(commonClique)
                            .stream()
                            // Since we don't actually place the SLs onto the newSuppliers in
                            // this function, the utilization of the newSeller will not change.
                            // Due to this, the best quote for all subsequent SLs which can fit
                            // in the new seller will get finite quote. So we need to ignore
                            // the newSellers to trigger provisionByDemand for each of the
                            // SL(which needs provisionByDemand).
                            //
                            // E.g. Let's consider a scenario where there is a buyer
                            // with 2 Storage SLs of equal size and they both can't
                            // be placed on any existing Storages. The 1st SL will cause
                            // a provision(byDemand) of a new storage to fit this SL.
                            // Since we don't place the 1st SL in this function,
                            // the 2nd SL will get a finite best quote(due to
                            // the newly provisioned storage). Later when the 1st SL is
                            // actually placed on this new storage, there won't be
                            // any more capacity for the 2nd SL. So the VM which has
                            // these SLs will go unplaced as the 2nd SL will get
                            // infinite quote. To fix this problem, we need to provision
                            // a new storage for the 2nd SL too. By ignoring the previously
                            // provisioned storage as a potential seller, we force the
                            // provisioning of a new storage.
                            .filter(trader -> !newSuppliersToIgnore.contains(trader))
                            .collect(Collectors.toList());

            // consider just sellersAvailableForPlacement
            sellers.retainAll(market.getActiveSellersAvailableForPlacement());
            @NonNull Stream<@NonNull Trader> stream =
                    sellers.size() < economy.getSettings().getMinSellersForParallelism()
                            ? sellers.stream() : sellers.parallelStream();
            @NonNull QuoteMinimizer minimizer = stream.collect(() -> new QuoteMinimizer(economy, sl),
                    QuoteMinimizer::accept, QuoteMinimizer::combine);
            // not all sl in movableSlByMarket needs additional supply, we should check by quote.
            // If quote is infinity, we need to cache it and add supply for it later.
            if (Double.isInfinite(minimizer.getTotalBestQuote())) {
                boolean isDebugBuyer = sl.getBuyer().isDebugEnabled();
                String buyerDebugInfo = sl.getBuyer().getDebugInfoNeverUseInCode();
                // Since we are moving the buyer to sellerThatFits we have to make sure
                // that it belongs to the currentClique.
                Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, market, economy, Optional.of(commonClique));
                // provision by supply
                if (sellerThatFits != null) {
                    boolean isDebugSeller = sellerThatFits.isDebugEnabled();
                    String sellerDebugInfo = sellerThatFits.getDebugInfoNeverUseInCode();
                    if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                        logger.info("The seller " + sellerDebugInfo + " can fit the buyer "
                                + buyerDebugInfo + ". Cached for provision by supply.");
                    }
                    // cache the sl that need ProvisionBySupply in slsThatNeedProvBySupply
                    // Since we are going to do a compound move of the buyer we have to make sure
                    // all the shopping lists are in the sl list and corresponding seller in
                    // trader list. The sellerThatFits can be used as seller for now. Eventually
                    // when the provision-by-supply actually happens it will go to something
                    // in the same clique as other shopping lists.
                    slsThatNeedProvBySupply.put(sl, commonClique);
                    if (!sellerThatFits.getState().isActive()) {
                        // if sellerThatFits is inactive, pass in the activated seller
                        provisionedRelatedActions.add((new Activate(economy, sellerThatFits,
                                market, sellerThatFits, null)).take());
                    }
                    traderList.add(sellerThatFits);
                    slList.add(sl);
                } else { // provision by demand
                    if (logger.isTraceEnabled() || isDebugBuyer) {
                        logger.info("No seller who fits buyer " + buyerDebugInfo + " found."
                                + " Trying provision by demand.");
                    }
                    // TODO: maybe pick a better seller instead of the first one
                    List<Trader> clonableSellers = sellers.stream().filter(s ->
                            s.getSettings().isCloneable()).collect(Collectors.toList());
                    if (clonableSellers.isEmpty()) {
                        if (logger.isTraceEnabled() || isDebugBuyer) {
                            logger.info("No clonable trader can be found in market though"
                                    + " buyer {} has an infinity quote.", buyerDebugInfo);
                        } else {
                            logger.warn("No clonable trader can be found in market though"
                                    + " buyer {} has an infinity quote.", buyerDebugInfo);
                        }
                    } else {
                        Trader currentSupplier = sl.getSupplier();
                        Action action = new ProvisionByDemand(economy, sl,
                                (currentSupplier != null && clonableSellers.contains(currentSupplier)) ?
                                        currentSupplier : clonableSellers.get(0)).take();
                        ((ActionImpl)action).setImportance(Double.POSITIVE_INFINITY);
                        Trader newSeller = ((ProvisionByDemand)action).getProvisionedSeller();
                        boolean isDebugSeller = newSeller.isDebugEnabled();
                        String sellerDebugInfo = newSeller.getDebugInfoNeverUseInCode();
                        if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                            logger.info("New seller " + sellerDebugInfo
                                    + " was provisioned to fit " + buyerDebugInfo + ".");
                        }

                        // provisionByDemand does not place the provisioned trader. We try finding
                        // best placement for it, if none exists, we create one supply for
                        // provisioned trader
                        provisionedRelatedActions
                                .addAll(shopTogetherBootstrapForIndividualBuyer(economy, newSeller,
                                        slsThatNeedProvBySupply));
                        provisionedRelatedActions.add(action);
                        provisionedRelatedActions.addAll(((ProvisionByDemand)action).getSubsequentActions());
                        newSuppliers.put(sl, newSeller);
                        newSuppliersToIgnore.add(newSeller);
                    }
                }
            } else {
                // if the VM is able to fit in the provider, place on it
                traderList.add(minimizer.getBestSeller());
                slList.add(sl);
            }
        }
        if (!newSuppliers.isEmpty()) {
            // do a compoundMove
            // iterate map entry to ensure the order of slList and traderList are the same,
            // as there is no guarantee for order using map.keySet and map.values
            newSuppliers.entrySet().forEach(e -> {
                slList.add(e.getKey());
                traderList.add(e.getValue());
            });
        }
        if (slList.size() == movableSlByMarket.size()) {
            List<Trader> currentSuppliers = slList.stream().map(ShoppingList::getSupplier)
                .collect(Collectors.toList());
            // check if all shopping lists have the same source and destination
            // the reason that the source and destination of a shopping list are the same might be
            // the shopping list doesn't need to be moved or
            // we need to do provision by supply to clone the source of this shopping list
            long numOfSlWithDiffSrcAndDest = IntStream.range(0, slList.size())
                .filter(i -> currentSuppliers.get(i) != traderList.get(i)).count();
            // if all shopping lists have same source and destination,
            // then we don't need to generate a CompoundMove
            // if we don't add the following if statement,
            // then when all shopping lists have same source and destination,
            // a compound move with no constituent actions will be generated (OM-42701)
            if (numOfSlWithDiffSrcAndDest > 0) {
                Placement.generateCompoundMoveOrMoveAction(
                    economy, slList, currentSuppliers, traderList, provisionedRelatedActions, 0.0d);
            }
        }
        return provisionedRelatedActions;
    }

    /**
     * Generate a activate or provisionBySupply recommendation.
     *
     * <p>as a side-effect, the provision or activate action is added to the list of actions passed
     *  as an argument
     * </p>
     *
     * @param sellerThatFits is the {@link Trader} that when cloned can fit the buyer
     * @param market is the {@link Market} in which the sellerThatFits sells
     * @param actions the list of {@link Action}s generated during bootstrap
     * @param economy the {@link Economy} for which we want to guarantee enough supply.
     * @param commSpec commodity that led to provision or activation.
     * @return a {@link Trader} the activated/provisioned trader
     */
    private static Trader provisionOrActivateTrader(Trader sellerThatFits,
                        Market market, List<Action> actions, Economy economy,
                        CommoditySpecification commSpec) {
        Action action;
        Trader newSeller;
        // clone one of the sellers or reactivate an inactive seller that the VM can fit in
        if (sellerThatFits.getState() == TraderState.ACTIVE) {
            action = new ProvisionBySupply(economy, sellerThatFits, commSpec).take();
            actions.add(action);
            newSeller = ((ProvisionBySupply)action).getProvisionedSeller();
            actions.addAll(((ProvisionBySupply)action).getSubsequentActions());
        } else {
            action = new Activate(economy, sellerThatFits, market, sellerThatFits, commSpec)
                            .take();
            actions.add(action);
            newSeller = sellerThatFits;
        }
        ((ActionImpl)action).setImportance(Double.POSITIVE_INFINITY);
        return newSeller;
    }

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set
     * upper limits, when shop-together is disabled.
     *
     * @param economy the {@link Economy} for which we want to guarantee enough supply.
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

        boolean isDebugBuyer = shoppingList.getBuyer().isDebugEnabled();
        String buyerDebugInfo = shoppingList.getBuyer().getDebugInfoNeverUseInCode();

        // below is the provision logic for non shop together traders, if the trader
        // should shop together, skip the logic
        if (shoppingList.getBuyer().getSettings().isShopTogether()
                || !shoppingList.isMovable()) {
            if (logger.isTraceEnabled() || isDebugBuyer) {
                if (shoppingList.getBuyer().getSettings().isShopTogether()) {
                    logger.info("{" + buyerDebugInfo + "} can't shop alone because it must"
                                + " shop together.");
                }
                if (!shoppingList.isMovable()) {
                    logger.info("{" + buyerDebugInfo + "} can't shop alone because its"
                                + " moving list is not movable.");
                }
            }
            return allActions;
        }
        List<Trader> sellers = market.getActiveSellersAvailableForPlacement();
        // find the bestQuote
        final QuoteMinimizer minimizer =
                (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                        ? sellers.stream() : sellers.parallelStream())
                        .collect(() -> new QuoteMinimizer(economy, shoppingList),
                                QuoteMinimizer::accept, QuoteMinimizer::combine);

        boolean isDebugSeller = false;
        String sellerDebugInfo = null;
        if (minimizer.getBestSeller() != null) {
            isDebugSeller = minimizer.getBestSeller().isDebugEnabled();
            sellerDebugInfo = minimizer.getBestSeller().getDebugInfoNeverUseInCode();
        }

        // unplaced buyer
        if (shoppingList.getSupplier() == null) {
            if (Double.isFinite(minimizer.getTotalBestQuote())) {
                // on getting finiteQuote, move unplaced Trader to the best provider
                allActions.addAll(new Move(economy, shoppingList, minimizer.getBestSeller())
                        .take().setImportance(Double.POSITIVE_INFINITY)
                        .getAllActions());
                if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                    logger.info("{" + buyerDebugInfo + "} moves to "
                                + sellerDebugInfo + " because it is unplaced.");
                }
            } else {
                // on getting an infiniteQuote, provision new Seller and move unplaced Trader to it
                if (logger.isTraceEnabled() || isDebugBuyer) {
                    logger.info("{" + buyerDebugInfo + "} can't be placed in any supplier.");
                }
                allActions.addAll(checkAndApplyProvision(economy, shoppingList, market,
                                                         slsThatNeedProvBySupply));
            }
        } else {
            // already placed Buyer
            if (Double.isInfinite(minimizer.getBestQuote().getQuoteValue())) {
                // Start by cloning the best provider that can fit the buyer. If none can fit
                // the buyer, provision a new seller large enough to fit the demand.
                if (logger.isTraceEnabled() || isDebugBuyer) {
                    logger.info("{" + buyerDebugInfo + "} can't be placed an any supplier.");
                }
                allActions.addAll(checkAndApplyProvision(economy, shoppingList, market,
                                                         slsThatNeedProvBySupply));
            } else if (Double.isInfinite(minimizer.getCurrentQuote().getQuoteValue()) &&
                    minimizer.getBestSeller() != shoppingList.getSupplier()) {
                // If we have a seller that can fit the buyer getting an infiniteQuote,
                // move buyer to this provider
                allActions.addAll(new Move(economy, shoppingList, shoppingList.getSupplier(),
                        minimizer.getBestSeller(), minimizer.getBestQuote().getContext())
                        .take().setImportance(minimizer.getCurrentQuote().getQuoteValue())
                        .getAllActions());
                if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                    logger.info("{" + buyerDebugInfo + "} moves to "
                            + sellerDebugInfo + " because its current quote is infinite.");
                }
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

            if (Double.isInfinite(minimizer.getTotalBestQuote())) {
                // on getting an infiniteQuote, provision new Seller and move unplaced Trader to it
                // clone one of the sellers or reactivate an inactive seller that the VM can fit in
                Trader sellerThatFits = findTraderThatFitsBuyer(sl, sellers, market, economy, Optional.empty());
                if (sellerThatFits != null) {
                    Trader provisionedSeller;
                    // Obtain list of commodity specs causing infinite quote for the shopping list
                    List<CommoditySpecification> commoditySpecsWithInfiniteQuote =
                                    findCommSpecsWithInfiniteQuote(sellerThatFits, sl, economy);
                    if (sellerThatFits.getSettings().isResizeThroughSupplier()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("SellerThatFits is ResizeThroughSupplier: {}",
                                            sellerThatFits.getDebugInfoNeverUseInCode());
                        }
                        // If the trader is resizeThroughSupplier then generate supply provisions
                        // to provide enough capacity for the sellerThatFits to be able to host the
                        // shopping list.
                        List<Trader> newSellers = Utility.provisionSufficientSupplyForResize(economy,
                                        sellerThatFits, commoditySpecsWithInfiniteQuote, sl, allActions);
                        if (newSellers.isEmpty()) {
                            return allActions;
                        }
                        // Obtain the first provisioned seller since they are all the same.
                        provisionedSeller = newSellers.get(0);
                    } else {
                        CommoditySpecification commoditySpecWithInfiniteQuote =
                                        commoditySpecsWithInfiniteQuote.isEmpty()
                                            ? null : commoditySpecsWithInfiniteQuote.get(0);
                        provisionedSeller = provisionOrActivateTrader(sellerThatFits, market,
                            allActions, economy, commoditySpecWithInfiniteQuote);
                    }
                    // provisionedSeller may not be a clone of the sellerThatFits in the case of
                    // resizeThroughSupplier so ensure that the provisionedSeller is added to this
                    // market's available sellers before moving the sl onto it
                    if (market.getActiveSellersAvailableForPlacement().contains(provisionedSeller)) {
                    allActions.addAll(new Move(economy, sl, provisionedSeller).take()
                            .setImportance(Double.POSITIVE_INFINITY).getAllActions());
                    // if the sellerThatFits is resizeThroughSupplier trader then move the sl to it
                    // if it isn't currently on it.
                    } else if (sellerThatFits.getSettings().isResizeThroughSupplier()
                                    && sl.getSupplier() != sellerThatFits) {
                        allActions.addAll(new Move(economy, sl, sellerThatFits).take()
                                        .setImportance(Double.POSITIVE_INFINITY).getAllActions());
                    }
                } else {
                    @NonNull Trader buyer = sl.getBuyer();
                    if (logger.isTraceEnabled() || buyer.isDebugEnabled()) {
                        logger.debug("Quote is infinity, and unable to find a seller that will fit the trader: {}",
                                buyer.getDebugInfoNeverUseInCode());
                    }
                }
            } else {
                if (minimizer.getBestSeller() != sl.getSupplier()) {
                    allActions.addAll(new Move(economy, sl, minimizer.getBestSeller()).take()
                            .setImportance(minimizer.getCurrentQuote().getQuoteValue()
                                    - minimizer.getBestQuote().getQuoteValue()).getAllActions());
                }
            }
        }
        return allActions;
    }

    /**
     * Returns the commodity specifications in shopping list that led to infinite quote
     * for current or future seller.
     * @param sellerThatFits future supplier of given shopping list.
     * @param sl shopping list's commodities to iterate over.
     * @param economy economy in which current seller and shopping list exist.
     * @return infCommSpecList List of commodities that led to infinite quote if any.
     */
     public static List<CommoditySpecification> findCommSpecsWithInfiniteQuote(Trader sellerThatFits,
                    ShoppingList sl, Economy economy) {
        List<CommoditySpecification> infCommSpecList = new ArrayList<>();
        // If seller that fits is resize through supplier trader then return the infinite
        // commodities the VM would get by moving to this trader.
        Trader traderToInspect = sellerThatFits.getSettings().isResizeThroughSupplier()
                        ? sellerThatFits : chooseSellerToAskQuotes(sellerThatFits, sl);
        if (traderToInspect == null) {
            return infCommSpecList;
        }
        Basket basket = sl.getBasket();
        CommoditySpecification commWithInfQuote = null;
        for (int soldIndex = 0, boughtIndex = 0; boughtIndex < basket.size(); boughtIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);
            while (!basketCommSpec.equals(traderToInspect.getBasketSold().get(soldIndex))) {
                soldIndex++;
                if (soldIndex >= traderToInspect.getBasketSold().size()) {
                    logBasketUnsatisfiedErrorMessage(traderToInspect, sl, basketCommSpec);
                    // we can return null as the reason commSpec with infinite quote because in
                    // AnalysisToProtobuf.actionTO (which is executed before sending over the
                    // results to platform), we calculate the most expensive commodity and make it
                    // the reason commodity if the provision action's reason is null
                    return infCommSpecList;
                }
            }
            double[] tempQuote = EdeCommon.computeCommodityCost(economy, sl, traderToInspect,
                                                                soldIndex, boughtIndex, false);
            if (Double.isInfinite(tempQuote[0])) {
                commWithInfQuote = basketCommSpec;
                logger.debug("Infinite Quote in BootStrap for " + sl.getDebugInfoNeverUseInCode()
                + " due to : " + basketCommSpec.getDebugInfoNeverUseInCode());
                infCommSpecList.add(commWithInfQuote);
            }
        }
        return infCommSpecList;
    }

    /**
     * Choose a seller between the shopping list's current supplier and the sellerThatFits.
     * If the shopping list sl has a current supplier, then we choose to ask quotes from there
     * unless one of the commodities bought by sl does not exist in the current supplier. This can
     * happen when segmentation commodity is missing from the current supplier. If one of
     * the commodities bought by sl does not exist in current supplier, then we return the
     * sellerThatFits.
     *
     * @param sellerThatFits future supplier of given shopping list.
     * @param sl shopping list we are trying to provide for.
     * @return either the sellerThatFits or shopping list's current supplier.
     */
    private static @Nullable Trader chooseSellerToAskQuotes(Trader sellerThatFits,
                                                                            ShoppingList sl) {
        Trader traderToInspect = sl.getSupplier() == null ? sellerThatFits : sl.getSupplier();
        Basket basket = sl.getBasket();
        boolean isBasketUnsatisfied = false;
        for (int soldIndex = 0, boughtIndex = 0; boughtIndex < basket.size(); boughtIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);
            while (!basketCommSpec.equals(traderToInspect.getBasketSold().get(soldIndex))) {
                soldIndex++;
                if (soldIndex >= traderToInspect.getBasketSold().size()) {
                    isBasketUnsatisfied = true;
                    break;
                }
            }
            // If there is a missing commodity, then return seller that fits
            if (isBasketUnsatisfied) {
                if (traderToInspect == sellerThatFits) {
                    logBasketUnsatisfiedErrorMessage(traderToInspect, sl, basketCommSpec);
                    // We are already using seller that fits
                    return null;
                } else {
                    return sellerThatFits;
                }
            }
        }
        return traderToInspect;
    }

    /**
     * Log an error message stating that one of the commodity specs is missing in the modelSeller
     *
     * @param modelSeller the modelSeller we are trying to provision
     * @param sl the shopping list we are trying to satisfy by provisioning
     * @param missingComm the missing commodity in the model seller
     */
    private static void logBasketUnsatisfiedErrorMessage(Trader modelSeller, ShoppingList sl,
                                                  CommoditySpecification missingComm) {
        logger.error("Trying to provision "
                + modelSeller + " for " + sl.getBuyer() + " but it is not selling "
                + missingComm.getDebugInfoNeverUseInCode()
                + " which is needed by buyer.");
    }

    /**
     * If there is a trader that can be reactivated or cloned through ProvisionBySupply to fit buyer,
     * populate the slsThatNeedProvBySupply list with the buyer that will be processed later or Provision
     * {@link Trader} that can fit the buyer through {@link ProvisionBySupply} and make the buyer
     * consume from the trader.
     *
     * @param economy the {@link Economy} that contains the unplaced {@link Trader}
     * @param shoppingList is the {@link ShoppingList} of the unplaced trader
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
        Trader provisionedSeller = null;

        boolean isDebugBuyer = shoppingList.getBuyer().isDebugEnabled();
        String buyerDebugInfo = shoppingList.getBuyer().getDebugInfoNeverUseInCode();

        List<Trader> activeSellerThatCanAcceptNewCustomers = market.getActiveSellersAvailableForPlacement();
        List<Trader> clonableSellers = activeSellerThatCanAcceptNewCustomers.stream().filter(s ->
                s.getSettings().isCloneable()).collect(Collectors.toList());
        // Return if there are active sellers and none of them are cloneable or resizeable through
        // supplier.
        if (!activeSellerThatCanAcceptNewCustomers.isEmpty() && activeSellerThatCanAcceptNewCustomers.stream()
            .noneMatch(seller -> seller.getSettings().isCloneable()
                || Utility.resizeThroughSupplier(seller, shoppingList, economy))) {
            if (logger.isTraceEnabled() || isDebugBuyer) {
                logger.info("There are no active sellers available to place "
                            + buyerDebugInfo + ".");
            }
            return actions;
        }
        Trader sellerThatFits = findTraderThatFitsBuyer(shoppingList, activeSellerThatCanAcceptNewCustomers,
                                                        market, economy, Optional.empty());
        if (sellerThatFits != null) {
            boolean isDebugSeller = sellerThatFits.isDebugEnabled();
            String sellerDebugInfo = sellerThatFits.getDebugInfoNeverUseInCode();
            // log shoppingLists that need ProvisionBySupply in slsThatNeedProvBySupply
            slsThatNeedProvBySupply.put(shoppingList, new Long(0));
            if (logger.isTraceEnabled() || isDebugBuyer || isDebugSeller) {
                logger.info("The seller " + sellerDebugInfo + " can fit the buyer "
                        + buyerDebugInfo + ". Cached for provision by supply.");
            }

            return Collections.emptyList();
        } else if (!clonableSellers.isEmpty()) {
            // if none of the existing sellers can fit the shoppingList, provision current seller
            bootstrapAction = new ProvisionByDemand(economy, shoppingList,
                    (shoppingList.getSupplier() != null
                            && clonableSellers.contains(shoppingList.getSupplier()))
                            ? shoppingList.getSupplier() : clonableSellers.get(0)).take();
            ((ActionImpl)bootstrapAction).setImportance(Double.POSITIVE_INFINITY);
            provisionedSeller = ((ProvisionByDemand)bootstrapAction).getProvisionedSeller();
            provisionRelatedActionList.add(bootstrapAction);
            provisionRelatedActionList.addAll(((ProvisionByDemand)bootstrapAction).getSubsequentActions());
            if (logger.isTraceEnabled() || isDebugBuyer) {
                logger.info(provisionedSeller.getDebugInfoNeverUseInCode() + " was provisioned to"
                            + " accommodate " + buyerDebugInfo + ".");
            }
            // provisionByDemand does not place the new newClone provisionedTrader. We try finding
            // best seller, if none exists, we create one
            economy.getMarketsAsBuyer(provisionedSeller).entrySet().forEach(entry -> {
                        ShoppingList sl = entry.getKey();
                        List<Trader> sellers = entry.getValue().getActiveSellers();
                        QuoteMinimizer minimizer =
                            (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                                     ? sellers.stream() : sellers.parallelStream())
                                         .collect(() -> new QuoteMinimizer(economy, sl),
                                            QuoteMinimizer::accept, QuoteMinimizer::combine);
                        // If quote is infinite, we create a new provider
                        if (Double.isInfinite(minimizer.getTotalBestQuote())) {
                            provisionRelatedActionList.addAll(checkAndApplyProvision(economy,
                                            sl, entry.getValue(), slsThatNeedProvBySupply));
                        } else {
                            // place the sl of the clone on the seller that is able to hold it
                            provisionRelatedActionList
                                .addAll((new Move(economy, sl, minimizer.getBestSeller()))
                                    .take().getAllActions());
                        }
            });
            // When the current seller can't accept new customers the active sellers are not empty
            // and we want to avoid a reconfigure action. Look OM-34627.
        } else if (market.getActiveSellers().isEmpty()) {
            // when there is no seller in the market we could handle this through 3 approaches,
            // 1) TODO: provisionAction = new ProvisionByDemand(economy, shoppingList); OR
            // 2) consider using templates
            // 3) generating reconfigure action for now
            actions.add(new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
            if (logger.isTraceEnabled() || isDebugBuyer) {
                logger.info("Generating a reconfigure action for " + buyerDebugInfo + ".");
            }
            // set movable false so that we don't generate duplicate reconfigure recommendations
            shoppingList.setMovable(false);
            return actions;
        }
        actions.addAll(provisionRelatedActionList);
        // Note: This move action is to place the newly provisioned trader to a proper
        // supplier.
        if (provisionedSeller != null) {
            actions.addAll(new Move(economy, shoppingList, provisionedSeller).take()
                .setImportance(Double.POSITIVE_INFINITY)
                .getAllActions());
        }
        return actions;
    }

    /**
     * Out of a list sellers, we check if, we can clone any seller in particular
     * that can fit the buyer.
     *
     * @param buyerShoppingList is the {@link ShoppingList} of the buyer
     * @param candidateSellers is the list of candidate {@link Trader sellers} to examine
     * @param market is the {@link Market} in which we try finding the best provider to
     * reactivate/clone
     * @param economy the {@link Economy} that contains the unplaced {@link Trader}
     * @param clique a optional parameter, if it is present, the sell's clique list must contains it.
     *
     * @return the any of the candidateSellers that can fit the buyer when cloned, or {@code null}
     * if none is big enough
     */
    private static Trader findTraderThatFitsBuyer(ShoppingList buyerShoppingList, List<Trader>
                                                  candidateSellers, Market market, Economy economy,
                                                  Optional<Long> clique) {
        for (Trader seller : market.getInactiveSellers()) {
            // the seller clique list must contains input cliqueId.
            if ((!clique.isPresent() || seller.getCliques().contains(clique.get())) &&
                    ProvisionUtils.canBuyerFitInSeller(buyerShoppingList, seller, economy)) {
                return seller;
            }
        }
        for (Trader seller : candidateSellers) {
            // pick the first candidate seller that can fit the demand through either cloning
            // or resizing through provisioning its own supplier.
            if ((seller.getSettings().isCloneable()
                && ProvisionUtils.canBuyerFitInSeller(buyerShoppingList, seller, economy))
                || Utility.resizeThroughSupplier(seller, buyerShoppingList, economy)) {
                return seller;
            }
        }
        return null;
    }
}
