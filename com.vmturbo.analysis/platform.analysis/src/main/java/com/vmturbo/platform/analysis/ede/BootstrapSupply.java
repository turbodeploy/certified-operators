package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.GuaranteedBuyerHelper;
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

    static final Logger logger = Logger.getLogger(BootstrapSupply.class);

    /**
     * Guarantee enough supply to place all demand at utilization levels that comply to user-set
     * upper limits.
     *
     * @param the {@Link Economy} for which we want to guarantee enough supply.
     *
     * @return list of actions that might include provision, move and reconfigure.
     */
    public static @NonNull List<@NonNull Action> bootstrapSupplyDecisions(@NonNull Economy economy) {
        List<@NonNull Action> allActions = new ArrayList<>();
        for (Market market : economy.getMarkets()) {
            if (economy.getForceStop()) {
                return allActions;
            }
            // do not provision traders in markets where guaranteedBuyers are unplaced
            // or when all the sellers are not cloneable
            if (market.getActiveSellers().stream().allMatch(trader -> !trader.getSettings()
                   .isCloneable()) || market.getBuyers().stream().allMatch(sl -> sl.getBuyer()
                       .getSettings().isGuaranteedBuyer())) {
                continue;
            }
            List<Trader> sellers = market.getActiveSellers();
            for (@NonNull ShoppingList shoppingList : market.getBuyers()) {
                // find the bestQuote
                final QuoteMinimizer minimizer =
                                (sellers.size() < economy.getSettings().getMinSellersForParallelism()
                                    ? sellers.stream() : sellers.parallelStream())
                                .collect(()->new QuoteMinimizer(economy,shoppingList),
                                    QuoteMinimizer::accept, QuoteMinimizer::combine);

                // unplaced buyer
                if (shoppingList.getSupplier() == null) {
                    if (Double.isFinite(minimizer.getBestQuote())) {
                        // on getting finiteQuote, move unplaced Trader to the best provider
                        allActions.add(new Move(economy,shoppingList,minimizer.getBestSeller())
                                        .take().setImportance(minimizer.getCurrentQuote()
                                                        - minimizer.getBestQuote()));
                    } else {
                        // on getting an infiniteQuote, provision new Seller and move unplaced Trader to it
                        allActions.addAll(provisionTraderToFitBuyer(economy, shoppingList, market));
                    }
                } else {
                    // already placed Buyer
                    if (Double.isInfinite(minimizer.getBestQuote())) {
                        // Start by cloning the best provider that can fit the buyer. If none can fit
                        // the buyer, provision a new seller large enough to fit the demand.
                        if (sellers.stream().filter(seller -> seller.getSettings().isCloneable()).count() != 0) {
                            allActions.addAll(provisionTraderToFitBuyer(economy, shoppingList, market));
                        }
                    } else if (Double.isInfinite(minimizer.getCurrentQuote()) &&
                                    minimizer.getBestSeller() != shoppingList.getSupplier()) {
                        // If we have a seller that can fit the buyer getting an infiniteQuote,
                        // move buyer to this provider
                        allActions.add(new Move(economy,shoppingList,minimizer.getBestSeller())
                                       .take().setImportance(minimizer.getCurrentQuote()
                                                         - minimizer.getBestQuote()));
                    }
                }
            }
        }
        GuaranteedBuyerHelper.processGuaranteedbuyerInfo(economy);
        return allActions;
    }


    /**
     * Reactivate the best trader that can fit the buyer. If there is no inactive trader that can
     * fit the buyer, Provision the best Trader that fits the shoppingList and make the buyer consume
     * from the new Trader
     *
     * @param economy the {@Link Economy} that contains the unplaced {@link Trader}
     * @param shoppingList is the {@Link ShoppingList} of the unplaced trader
     * @param market is the market containing the inactiveSellers
     *
     * @return list of actions that might include provision, move and reconfigure.
     */
    private static List<Action> provisionTraderToFitBuyer (Economy economy, ShoppingList shoppingList,
                                                           Market market) {
        List<@NonNull Action> actions = new ArrayList<>();
        if (economy.getForceStop()) {
            return actions;
        }
        List<Action> provisionRelatedActionList = new ArrayList<>();
        Action bootstrapAction;
        Trader provisionedSeller;
        List<Trader> activeSellers = market.getActiveSellers();
        Trader sellerThatFits = findSellerThatFitsBuyer (shoppingList, activeSellers, market);
        if (sellerThatFits != null) {
            // clone one of the sellers or reactivate an inactive seller that the VM can fit in
            if (sellerThatFits.getState() == TraderState.ACTIVE) {
                bootstrapAction = new ProvisionBySupply(economy, sellerThatFits).take();
                ((ActionImpl)bootstrapAction).setImportance(Double.POSITIVE_INFINITY);
                provisionedSeller = ((ProvisionBySupply)bootstrapAction).getProvisionedSeller();
                provisionRelatedActionList.add(bootstrapAction);
            } else {
                bootstrapAction = new Activate(economy, sellerThatFits, market, sellerThatFits).take();
                ((ActionImpl)bootstrapAction).setImportance(Double.POSITIVE_INFINITY);
                provisionedSeller = sellerThatFits;
                provisionRelatedActionList.add(bootstrapAction);
            }
        } else if (!activeSellers.isEmpty()){
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
                            provisionRelatedActionList.addAll(provisionTraderToFitBuyer(economy,
                                            sl, market));
                        } else {
                            // place the shopping list of the new clone to the best supplier
                            // this is equivalent as Start in legacy market
                            if (!sl.isMovable()) {
                                sl.move(minimizer.getBestSeller());
                                Move.updateQuantities(economy, sl, minimizer.getBestSeller(),
                                                (sold, bought) -> sold + bought);
                            }
                        }
            });
        } else {
            // when there is no seller in the market we could handle this through 3 approaches,
            // 1) TODO: provisionAction = new ProvisionByDemand(economy, shoppingList); OR
            // 2) need templates
            // 3) generating reconfigure action for now
            actions.add(new Reconfigure(economy, shoppingList).take().setImportance(Double.POSITIVE_INFINITY));
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
    private static Trader findSellerThatFitsBuyer(ShoppingList buyerShoppingList, List<Trader>
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

            if (buyerShoppingList.getQuantities()[boughtIndex] > commSold.getEffectiveCapacity()) {
                return false;
            }
        }
        return true;
    }

}