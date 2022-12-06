package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;

/**
 * Comprises a number of static utility methods used for creating and updating shoppingLists of
 * guaranteedBuyers
 */
public final class GuaranteedBuyerHelper {

    // double for comparison
    public static final double EPSILON = 1e-5;

    private static final Logger logger = LogManager.getLogger(GuaranteedBuyerHelper.class);
    // Methods

    /**
     * A helper method to create {@link ShoppingList} to establish buyer-seller relation between
     * guaranteed buyers and a newly provisioned or activated trader.
     *
     * @param economy the economy the targetSeller should be part of
     * @param guaranteedBuyerSlsOnModelSeller a list of {@link ShoppingList} that was used as model
     *  to create the buyer-seller relation
     * @param slsSponsoredByGuaranteedBuyer a map to keep a guaranteed buyer to all of its shopping
     *  lists
     * @param newSupplier the {@link Trader} which would be a seller for the guaranteed buyers
     * @param reuseShoppingList When true new shopping list will be created when adding the basket
     *                          bought.  When false, the existing shopping list in the basket will
     *                          be reused.
     */
    public static void addNewSlAndAdjustExistingSls(Economy economy,
                                                    List<ShoppingList>
                                                        guaranteedBuyerSlsOnModelSeller,
                                                    Map<Trader, Set<ShoppingList>>
                                                        slsSponsoredByGuaranteedBuyer,
                                                    Trader newSupplier,
                                                    boolean reuseShoppingList) {
        for (ShoppingList sl : guaranteedBuyerSlsOnModelSeller) {
            Trader guaranteedBuyer = sl.getBuyer();
            // a set of shopping list sponsored by guaranteed buyer, we create a new set to keep
            // only the sl that is not consuming the new clone
            Set<ShoppingList> slsNeedsUpdate = slsSponsoredByGuaranteedBuyer.get(guaranteedBuyer)
                    .stream()
                    .filter(shoppingList -> sl.getBasket().equals(shoppingList.getBasket()))
                    .collect(Collectors.toSet());
            // we assume the basket sold by the new clone is not changed, thus new sl between
            // guaranteedBuyer and the new clone will shop in an existing market
            Basket newBasket = sl.getBasket();
            ShoppingList newSl = economy.addBasketBought(guaranteedBuyer, newBasket,
                    reuseShoppingList? sl : null);
            newSl.move(newSupplier);
            newSl.setMovable(sl.isMovable());
            // ResizeThroughSuppliers are also set to be GuaranteedBuyers but we don't want
            // to redistribute and update the SL quantities.
            if (!guaranteedBuyer.getSettings().isResizeThroughSupplier()) {
                for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                    newSl.setQuantity(boughtIndex, sl.getQuantity(boughtIndex))
                            .setPeakQuantity(boughtIndex, sl.getPeakQuantity(boughtIndex));
                }
                // Calculate and reset the quantity and peak quantity for each SL sponsored by the
                // guaranteed buyer (including the new SL added), because when a new SL is added
                // between the guaranteed buyer and the new clone, the quantity and peak quantity
                // needs to be re-distributed/re-balanced to take into consideration the effect of
                // the new clone. We assume that all SLs sponsored by a guaranteed buyer should have
                // the same quantity and peak quantity after redistribution. The sold quantities of the
                // providers are also updated accordingly.
                //
                // The above redistribution ONLY applies to commodities with valid redistribution
                // functions (currently only Response Time and Transaction commodities apply).
                // For all other commodities such as application commodity, the exiting SL and sold
                // quantities on the providers are not updated. In addition, the new SL will have the
                // same quantity as that of the model trader, and the sold quantity on the cloned trader
                // will be updated using ADD_COMM update function, effectively setting the sold quantity
                // to the same as that of the model trader.
                Move.updateQuantities(economy, newSl, newSupplier, UpdatingFunctionFactory.ADD_COMM,
                        slsNeedsUpdate, true);
            } else {
                // Copy over the same quantity as existing SL to new SL and set it as the sold
                // for the RTS provider and the RTS consumes the full capacity.
                for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                    newSl.setQuantity(boughtIndex, sl.getQuantity(boughtIndex))
                        .setPeakQuantity(boughtIndex, sl.getPeakQuantity(boughtIndex));
                    // if the quantity is not updated during suspension. dont update during rollback..
                    // it is possible that this part of code is not required atall if we can make sure
                    // the quantity of the commodity sold of host is not updated during suspension.
                    if (newSl.getQuantity(boughtIndex) > EPSILON) {
                        newSupplier.getCommoditySold(newBasket.get(boughtIndex))
                                .setQuantity(newSl.getQuantity(boughtIndex)).setPeakQuantity(
                                newSl.getPeakQuantity(boughtIndex));
                    }

                }
            }
        }
    }

    /**
     * A helper method to remove buyer-seller relation between guaranteed buyers and a seller.
     *
     * @param economy         the economy that the trader to be suspended is a part of
     * @param traderToSuspend the trader to be removed from supplier list of guaranteed buyers
     * @return a list of removed {@link ShoppingList}
     */
    public static List<ShoppingList> removeSlAndAdjustRemainingSls(Economy economy,
                                                                   Trader traderToSuspend) {
        List<ShoppingList> guaranteedBuyerSlsOnSuspendedTrader = GuaranteedBuyerHelper
                        .findSlsBetweenSellerAndGuaranteedBuyer(traderToSuspend);
        Map<Trader, Set<ShoppingList>> slsSponsoredByGuaranteedBuyer =
                        getAllSlsSponsoredByGuaranteedBuyer(economy, guaranteedBuyerSlsOnSuspendedTrader);
        List<ShoppingList> removedShoppingLists = new ArrayList<>();
        for (ShoppingList shoppingList : guaranteedBuyerSlsOnSuspendedTrader) {
            // ResizeThroughSuppliers are also set to be GuaranteedBuyers but we don't want
            // to redistribute and update the SL quantities. RTS also allows removing the last
            // SL and suspending the last trader.
            if (!shoppingList.getBuyer().getSettings().isResizeThroughSupplier()) {
                // Find the guaranteed buyer's shopping lists whose suppliers are still active and
                // trading in the same market
                Set<ShoppingList> slsNeedsUpdate = slsSponsoredByGuaranteedBuyer
                        .get(shoppingList.getBuyer()).stream()
                        .filter(sl -> {
                            Trader supplier = sl.getSupplier();
                            return supplier != null && supplier.getState().isActive()
                                    && shoppingList.getBasket().equals(sl.getBasket());
                        }).collect(Collectors.toSet());
                if (slsNeedsUpdate.size() <= 1) {
                    // Cannot rebalance across zero buyers.
                    logger.warn("Attempt to remove only shopping list from " +
                            shoppingList.getBuyer() + " - skipping");
                    continue;
                }
                // update quantity and peak quantity of the remaining shopping lists sponsored by
                // guaranteed buyer. This only applies to response time and transaction commodities
                // today. For other commodities, the SUB_COMM update function is used.
                Move.updateQuantities(economy, shoppingList, traderToSuspend,
                        UpdatingFunctionFactory.SUB_COMM, slsNeedsUpdate, false);
            }
            economy.removeBasketBought(shoppingList);
            removedShoppingLists.add(shoppingList);
        }
        return removedShoppingLists;
    }

    /**
     * A helper method to find all {@link ShoppingList} between guaranteed buyers and a given
     * seller.
     * @param seller the {@link Trader} whom may be a supplier for guaranteed buyers
     * @return a list of {@link ShoppingList}
     */
    public static List<ShoppingList> findSlsBetweenSellerAndGuaranteedBuyer(Trader seller) {
        List<ShoppingList> shoppingLists = new ArrayList<ShoppingList>();
        for (ShoppingList shoppingList : seller.getCustomers()) {
            if (shoppingList.getBuyer().getSettings().isGuaranteedBuyer()) {
                shoppingLists.add(shoppingList);
            }
        }
        return shoppingLists;
    }

    /**
     * Return the list of guaranteed buyers that this trader supplies
     * @param trader Trader to check for guaranteed buyers
     * @return a list of guaranteed buyers, or an empty list if none
     */
    public static List<@NonNull Trader> findGuaranteedBuyers(final Trader trader) {
        return findSlsBetweenSellerAndGuaranteedBuyer(trader).stream()
                .map(ShoppingList::getBuyer).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Auxiliary data-structure containing information needed to update shoppingLists
     * that are to be sold by new clones and bought by guaranteedBuyers
    */
    public static class BuyerInfo {

        public BuyerInfo(List<ShoppingList> sls, Map<Trader, Set<ShoppingList>> guaranteedBuyerSls,
                         Trader targetSeller) {
            sls_ = sls;
            guaranteedBuyerSls_ = guaranteedBuyerSls;
            newSupplier_ = targetSeller;
        }

        List<ShoppingList> sls_;
        Map<Trader, Set<ShoppingList>> guaranteedBuyerSls_;
        Trader newSupplier_;

        public List<ShoppingList> getSLs() {
            return sls_;
        }
        public Map<Trader, Set<ShoppingList>> getGuaranteedBuyerSls() {
            return guaranteedBuyerSls_;
        }
        public Trader getNewSupplier() {
            return newSupplier_;
        }

    }

    /**
     * create a new basket using an existing basket
     * @param originalBasket is the basket that basket that the guaranteedBuyers were buying from
     *                       the original host
     * @param commSpecMapNew map of oldCommSpecs to the one with new type that are sold by the
     * clone
     * @return basket containing commSpecs that are derived from the originalBasket with new type
     *
     */
    static Basket createBasketForClone(Basket originalBasket,
                                       Map<CommoditySpecification, CommoditySpecification>
                                       commSpecMapNew) {
        List<CommoditySpecification> commSpecList = new ArrayList<>();
        originalBasket.forEach(cs -> commSpecList.add(commSpecMapNew.get(cs)));
        return new Basket(commSpecList);
    }

    /**
     * Create a list of {@link BuyerInfo} based on shoppingLists between traders and its guaranteed
     * buyers.
     *
     * @param slBetweenModelSellerAndGuaranteedBuyer a list of {@link ShoppingList} between the
     *  guaranteedBuyer to seller
     * @param allSlsSponsoredByGuaranteedBuyer a map between guaranteed buyer to its sponsored
     *  shopping lists
     * @param newSupplier the newly provisioned trader
     * @return a list of guaranteed buyer info
     */
    public static List<BuyerInfo>
           storeGuaranteedbuyerInfo(List<ShoppingList> slBetweenModelSellerAndGuaranteedBuyer,
                                    Map<Trader, Set<ShoppingList>> allSlsSponsoredByGuaranteedBuyer,
                                    Trader newSupplier) {
        List<BuyerInfo> guaranteedBuyerInfoList = new ArrayList<>();
        guaranteedBuyerInfoList.add(new BuyerInfo(slBetweenModelSellerAndGuaranteedBuyer,
                                                  allSlsSponsoredByGuaranteedBuyer,
                                                  newSupplier));
        return guaranteedBuyerInfoList;
    }

    /**
     * Iterate over the guaranteed buyer info list that holds saved shoppingLists that need to be
     * added for the guaranteedBuyers
     *
     * @param economy is the {@link Economy} into which markets selling new baskets sold
     * by the newly provisioned clones and bought by the guaranteedBuyers are added.
     * @param guaranteedBuyerInfo a list of guaranteed buyer info to be processed
     */
    public static void processGuaranteedbuyerInfo (Economy economy,
                                                   List<BuyerInfo> guaranteedBuyerInfo) {
        for (BuyerInfo info : guaranteedBuyerInfo) {
            addNewSlAndAdjustExistingSls(economy, info.getSLs(), info.getGuaranteedBuyerSls(),
                    info.getNewSupplier(), false);
        }
    }

    /**
     * A helper method to get a map for a guaranteed buyer to all of its shopping list
     *
     * @param economy the economy
     * @param shoppingLists the guaranteed buyer's shopping list on one seller
     * @return a map for a guaranteed buyer to all of its shopping list
     */
    public static @NonNull Map<Trader, Set<ShoppingList>>
            getAllSlsSponsoredByGuaranteedBuyer(Economy economy, List<ShoppingList> shoppingLists) {
        Map<Trader, Set<ShoppingList>> guaranteedBuyerSls = new HashMap<>();
        shoppingLists.stream().map(ShoppingList::getBuyer).forEach(b -> {
            guaranteedBuyerSls.put(b, economy.getMarketsAsBuyer(b).keySet());
        });
        return guaranteedBuyerSls;
    }

    /**
     * A helper method to get a list of all shopping lists whose buyer is a guaranteed buyer
     *
     * @param shoppingLists a list of shopping lists to select from
     * @return the list os shopping lists with guaranteed buyers
     */
    public static List<ShoppingList> getSlsWithGuaranteedBuyers (List<ShoppingList> shoppingLists) {
        return shoppingLists.stream()
                .filter(sl -> sl.getBuyer().getSettings().isGuaranteedBuyer())
                .collect(Collectors.toList());
    }

    /**
     * A helper method to suspend the provider of the target of the Deactivate action if required.
     * If any provider is also providerMustClone, then those providers are also suspended.  If any
     * subsequent Deactivate actions are generated, they are added this this action's subsequent
     * actions list.
     * @param deactivate The Deactivate context
     */
    public static void suspendProviders(final Deactivate deactivate) {
        if (deactivate.getTarget().getSettings().isProviderMustClone()) {
            for (Map.Entry<ShoppingList, Market> entry :
                    deactivate.getEconomy().getMarketsAsBuyer(deactivate.getTarget()).entrySet()) {
                @Nullable Trader trader = entry.getKey().getSupplier();
                if (trader != null && trader.getState().isActive()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Suspending {} because {} has providerMustClone",
                                entry.getKey().getSupplier().getDebugInfoNeverUseInCode(),
                                deactivate.getTarget().getDebugInfoNeverUseInCode());
                    }
                    Deactivate deactivateAction = new Deactivate(deactivate.getEconomy(), trader,
                            entry.getValue().getBasket());
                    deactivate.getSubsequentActions().add(deactivateAction.take());
                    deactivate.getSubsequentActions()
                            .addAll(deactivateAction.getSubsequentActions());
                }
            }
        }
    }

    /**
     * A helper function to check if the current replicas of a trader is beyond the allowed range.
     * The trader must be a provider to a guaranteed buyer.
     *
     * <p>A trader may theoretically have more than one guaranteed buyers, in which case we need to
     * find the number of replicas for all guaranteed buyers. For provision, the final replicas is
     * the maximum of replicas from all guaranteed buyers. For suspension, the final replicas is
     * the minimum of replicas from all guaranteed buyers.
     *
     * <p>In reality, a trader rarely has more than one guaranteed buyers. For example, a kubernetes
     * application should only belong to one service. If an application belongs to more than one
     * services, most likely there are some mis-configurations, such as incorrect service labels.
     *
     * @param trader the trader to check
     * @param economy the economy
     * @param provision if this check is for provision
     * @return true if the current replicas of the trader is beyond the allowed range, false otherwise
     */
    public static boolean isTraderReplicasBeyondRange(@NonNull final Trader trader,
                                                      @NonNull final Economy economy,
                                                      final boolean provision) {
        final int minReplicas = trader.getSettings().getMinReplicas();
        final int maxReplicas = trader.getSettings().getMaxReplicas();
        if (minReplicas == 0 && maxReplicas == 0) {
            // Min/Max replicas are not set for the trader
            return false;
        }
        // Check semantics of the Min/Max replicas setting
        checkArgument(minReplicas > 0 && maxReplicas > 0 && maxReplicas >= minReplicas,
                "Expect positive minReplicas and maxReplicas, "
                        + "and maxReplicas >= minReplicas. MinReplicas: %s, MaxReplicas: %s.",
                minReplicas, maxReplicas);
        final List<ShoppingList> slsBetweenSellerAndGuaranteedBuyer =
                findSlsBetweenSellerAndGuaranteedBuyer(trader);
        if (slsBetweenSellerAndGuaranteedBuyer.isEmpty()) {
            // The trader is not a provider to guaranteed buyer
            return false;
        }
        // Get the baskets that are sold by this trader. When calculating the number of replicas
        // for a guaranteed buyer, we should only include the shopping lists that buy the same
        // baskets.
        // For example, a service buys SL1 (ResponseTime and Transaction) from app1 (main app),
        // and buys SL2 (Application) from app2 (sidecar app). When checking the replicas of app1,
        // we should only check the SLs that buy the same basket as SL1 (i.e., ResponseTime and Transactions).
        final Set<Basket> validBaskets = slsBetweenSellerAndGuaranteedBuyer.stream()
                .map(ShoppingList::getBasket)
                .collect(Collectors.toSet());
        final Set<Integer> replicasSet = slsBetweenSellerAndGuaranteedBuyer.stream()
                .map(ShoppingList::getBuyer)
                .map(economy::getMarketsAsBuyer)
                .map(Map::keySet)
                .map(allSls -> removeSLsWithInvalidBasket(allSls, validBaskets))
                .map(Set::size)
                .collect(Collectors.toSet());
        return provision ? Collections.max(replicasSet) >= maxReplicas
                : Collections.min(replicasSet) <= minReplicas;
    }

    /**
     * A helper function to remove shopping lists with invalid baskets.
     *
     * @param allSls the list of {@link ShoppingList} to check
     * @param validBaskets the valid set of {@link Basket}
     * @return a set of {@link ShoppingList} that buy the valid baskets
     */
    private static Set<ShoppingList> removeSLsWithInvalidBasket(@NonNull final Set<ShoppingList> allSls,
                                                                @NonNull final Set<Basket> validBaskets) {
        return allSls.stream()
                .filter(sl -> validBaskets.stream().anyMatch(sl.getBasket()::isSatisfiedBy))
                .collect(Collectors.toSet());
    }
} // end GuaranteedBuyerHelper class
