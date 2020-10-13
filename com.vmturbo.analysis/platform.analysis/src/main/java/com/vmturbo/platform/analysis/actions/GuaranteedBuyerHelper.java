package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * Comprises a number of static utility methods used for creating and updating shoppingLists of
 * guaranteedBuyers
 */
public final class GuaranteedBuyerHelper {

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
            Set<ShoppingList> slsNeedsUpdate = new HashSet<>();
            slsNeedsUpdate.addAll(slsSponsoredByGuaranteedBuyer.get(guaranteedBuyer).stream()
                .filter(shoppingList -> sl.getBasket().equals(shoppingList.getBasket()))
                .collect(Collectors.toSet()));
            // we assume the basket sold by the new clone is not changed, thus new sl between
            // guaranteedBuyer and the new clone will shop in an existing market
            Basket newBasket = sl.getBasket();
            ShoppingList newSl = economy.addBasketBought(guaranteedBuyer, newBasket,
                    reuseShoppingList? sl : null);
            newSl.move(newSupplier);
            newSl.setMovable(sl.isMovable());
            // ResizeThroughSuppliers are also set to be GuaranteedBuyers but we don't want
            // to redistribute and update the SL quantities.
            if (!sl.getBuyer().getSettings().isResizeThroughSupplier()) {
                for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                    // calculate and reset the quantity and peak quantity for each shopping list
                    // sponsored by guaranteed buyer because when a new shopping list is added
                    // between guaranteed buyer and the new clone, the quantity and peak quantity
                    // needs to be re-averaged to include the new shopping list. We assume that all
                    // the sl sponsored by a guaranteed buyer should have the same quantity and peak
                    // quantity.
                    // If are reusing the shopping lists that we previously removed, we need to clear
                    // the original quantities on newSupplier.  The adjustCommodity call below will
                    // redistribute the existing quantities across all existing shopping lists into the
                    // new one.
                    // TODO: check with cloud native's team to see if we can assume each supplier of
                    // the guaranteed buyer has the same quantity bought
                    double updatedQuantity = sl.getQuantity(boughtIndex) * slsNeedsUpdate.size() /
                                    (slsNeedsUpdate.size() + 1);
                    double updatedPeakQuantity = sl.getPeakQuantity(boughtIndex) *
                                    slsNeedsUpdate.size() / (slsNeedsUpdate.size() + 1);
                    newSl.setQuantity(boughtIndex, updatedQuantity)
                         .setPeakQuantity(boughtIndex, updatedPeakQuantity);
                    // update quantity of the newly cloned seller to construct the buyer-seller
                    // relation the commSold in new clone already went through Utility.adjustOverhead
                    // so commSoldOnClone.getQuantity is only overhead
                    CommoditySold commSoldOnClone =
                                    newSupplier.getCommoditySold(newBasket.get(boughtIndex));
                    commSoldOnClone.setQuantity(newSl.getQuantity(boughtIndex));
                    commSoldOnClone.setPeakQuantity(newSl.getPeakQuantity(boughtIndex));
                    for (ShoppingList spList : slsNeedsUpdate) {
                        adjustCommodity(boughtIndex, updatedQuantity, updatedPeakQuantity, spList);
                    }
                }
            } else {
                // Copy over the same quantity as existing SL to new SL and set it as the sold
                // for the RTS provider and the RTS consumes the full capacity.
                for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                    newSl.setQuantity(boughtIndex, sl.getQuantity(boughtIndex))
                        .setPeakQuantity(boughtIndex, sl.getPeakQuantity(boughtIndex));
                    newSupplier.getCommoditySold(newBasket.get(boughtIndex))
                        .setQuantity(newSl.getQuantity(boughtIndex))
                        .setPeakQuantity(newSl.getPeakQuantity(boughtIndex));
                }
            }
        }
    }

    /**
     * Helper function to update the commodity sold that the shopping list consumes as a result
     * of changing the quantity and peak quantity of the shopping list.
     * @param boughtIndex commodity index
     * @param updatedQuantity new quantity
     * @param updatedPeakQuantity new peak quantity
     * @param shoppingList shopping list to modify
     */
    private static void adjustCommodity(final int boughtIndex,
                                        final double updatedQuantity,
                                        final double updatedPeakQuantity,
                                        final ShoppingList shoppingList) {
        double origQuantity = shoppingList.getQuantity(boughtIndex);
        double origPeakQuantity = shoppingList.getPeakQuantity(boughtIndex);
        shoppingList.setQuantity(boughtIndex, updatedQuantity);
        shoppingList.setPeakQuantity(boughtIndex, updatedPeakQuantity);
        // update commSold that the shoppingList consume as a result of changing
        // quantity and peak quantity of shoppingList, the commSold is from existing
        // sellers
        Trader supplier = shoppingList.getSupplier();
        if (supplier == null) {
            return;
        }
        CommoditySold commSold = supplier
                .getCommoditySold(shoppingList.getBasket().get(boughtIndex));
        if (commSold != null) {
            commSold.setQuantity(Math.max(0, commSold.getQuantity() - origQuantity +
                    updatedQuantity));
            commSold.setPeakQuantity(Math.max(commSold.getPeakQuantity() - origPeakQuantity
                            + updatedPeakQuantity,
                    commSold.getQuantity()));
        }
    }

    /**
     * A helper method to remove buyer-seller relation between guaranteed buyers and a seller.
     * @param economy the economy the target is part of
     * @param seller the seller to be removed from supplier list of guaranteed buyers
     */
    public static List<ShoppingList>
            removeShoppingListForGuaranteedBuyers(Economy economy, Trader seller) {
        List<ShoppingList> guaranteedBuyerSlsOnNewClone = GuaranteedBuyerHelper
                        .findSlsBetweenSellerAndGuaranteedBuyer(seller);
        Map<Trader, Set<ShoppingList>> slsSponsoredByGuaranteedBuyer =
                        getAllSlsSponsoredByGuaranteedBuyer(economy, guaranteedBuyerSlsOnNewClone);
        List<ShoppingList> removedShoppingLists = new ArrayList<>();
        for (ShoppingList shoppingList : guaranteedBuyerSlsOnNewClone) {
            // ResizeThroughSuppliers are also set to be GuaranteedBuyers but we don't want
            // to redistribute and update the SL quantities.
            if (!shoppingList.getBuyer().getSettings().isResizeThroughSupplier()) {
                // a set of shopping list sponsored by guaranteed buyer, the sl consuming new clone is
                // included
                Set<ShoppingList> slsNeedsUpdate = slsSponsoredByGuaranteedBuyer
                        .get(shoppingList.getBuyer()).stream()
                        .filter(sl -> {
                            Trader supplier = sl.getSupplier();
                            return supplier != null && supplier.getState().isActive()
                                            && shoppingList.getBasket().equals(sl.getBasket());
                        }).collect(Collectors.toSet());
                // Cannot rebalance across zero buyers.
                if (slsNeedsUpdate.size() > 1) {
                    // update quantity and peak quantity of the shopping list sponsored by guaranteed buyer
                    // because we force to remove the shopping list between guaranteed buyer and new clone
                    // it is not the sl between new clone and guaranteed buyer
                    for (int i = 0; i < shoppingList.getBasket().size(); i++) {
                        double updatedQuantity = shoppingList.getQuantity(i) * slsNeedsUpdate.size()
                                / (slsNeedsUpdate.size() - 1);
                        double updatedPeakQuantity = shoppingList.getPeakQuantity(i) *
                                slsNeedsUpdate.size()
                                / (slsNeedsUpdate.size() - 1);
                        for (ShoppingList sl : slsNeedsUpdate) {
                            // no need to update the sl on new clone as it will be removed
                            if (!sl.equals(shoppingList)) {
                                adjustCommodity(i, updatedQuantity, updatedPeakQuantity, sl);
                            }
                        }
                    }
                } else {
                    logger.warn("Attempt to remove only shopping list from " +
                            shoppingList.getBuyer() + " - skipping");
                    continue;
                }
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
} // end GuaranteedBuyerHelper class
