package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
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
     */
    public static void addNewSlAndAdjustExistingSls(Economy economy,
                                                    List<ShoppingList>
                                                        guaranteedBuyerSlsOnModelSeller,
                                                    Map<Trader, Set<ShoppingList>>
                                                        slsSponsoredByGuaranteedBuyer,
                                                    Trader newSupplier) {
        for (ShoppingList sl : guaranteedBuyerSlsOnModelSeller) {
            Trader guaranteedBuyer = sl.getBuyer();
            // a set of shopping list sponsored by guaranteed buyer, we create a new set to keep
            // only the sl that is not consuming the new clone
            Set<ShoppingList> slsNeedsUpdate = new HashSet<>();
            slsNeedsUpdate.addAll(slsSponsoredByGuaranteedBuyer.get(guaranteedBuyer));
            // we assume the basket sold by the new clone is not changed, thus new sl between
            // guaranteedbuyer and the new clone will shop in an existing market
            Basket newBasket = sl.getBasket();
            ShoppingList newSl = economy.addBasketBought(guaranteedBuyer, newBasket);
            newSl.move(newSupplier);
            newSl.setMovable(sl.isMovable());
            for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                // calculate and reset the quantity and peak quantity for each shopping list
                // sponsored by guaranteed buyer because when a new shopping list is added
                // between guaranteed buyer and the new clone, the quantity and peak quantity
                // needs to be re-averaged to include the new shopping list. We assume that all
                // the sl sponsored by a guaranteed buyer should have the same quantity and peak
                // quantity
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
                commSoldOnClone.setQuantity(commSoldOnClone.getQuantity() +
                                            newSl.getQuantity(boughtIndex));
                commSoldOnClone.setPeakQuantity(commSoldOnClone.getPeakQuantity() +
                                                newSl.getPeakQuantity(boughtIndex));
                for (ShoppingList spList : slsNeedsUpdate) {
                    double origQuantity = spList.getQuantity(boughtIndex);
                    double origPeakQuantity = spList.getPeakQuantity(boughtIndex);
                    spList.setQuantity(boughtIndex, updatedQuantity);
                    spList.setPeakQuantity(boughtIndex, updatedPeakQuantity);
                    // update commSold that the spList consume as a result of changing
                    // quantity and peak quantity of spList, the commSold is from existing
                    // sellers
                    CommoditySold commSold = spList.getSupplier()
                                    .getCommoditySold(spList.getBasket().get(boughtIndex));
                    commSold.setQuantity(Math.max(0, commSold.getQuantity() - origQuantity +
                                                  updatedQuantity));
                    commSold.setPeakQuantity(Math.max(commSold.getPeakQuantity()- origPeakQuantity
                                                      + updatedPeakQuantity,
                                                      commSold.getQuantity()));
                }
            }
        }
    }

    /**
     * A helper method to remove buyer-seller relation between guaranteed buyers and a seller.
     * @param economy the economy the target is part of
     * @param target the seller to be removed from supplier list of guaranteed buyers
     */
    public static void removeShoppingListForGuaranteedBuyers(Economy economy, Trader seller) {
        List<ShoppingList> guaranteedBuyerSlsOnNewClone = GuaranteedBuyerHelper
                        .findSlsBetweenSellerAndGuaranteedBuyer(economy, seller);
        Map<Trader, Set<ShoppingList>> slsSponsoredByGuaranteedBuyer =
                        getAllSlsSponsoredByGuaranteedBuyer(economy, guaranteedBuyerSlsOnNewClone);
        for (ShoppingList shoppingList : guaranteedBuyerSlsOnNewClone) {
            // a set of shopping list sponsored by guaranteed buyer, the sl consuming new clone is
            // included
            Set<ShoppingList> slsNeedsUpdate =
                                             slsSponsoredByGuaranteedBuyer
                                                             .get(shoppingList.getBuyer());
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
                        double origQuantity = sl.getQuantity(i);
                        double origPeakQuantity = sl.getPeakQuantity(i);
                        sl.setQuantity(i, updatedQuantity);
                        sl.setPeakQuantity(i, updatedPeakQuantity);
                        // update commSold that the sl consume as a result of changing
                        // quantity and peak quantity of sl
                        CommoditySold commSold = sl.getSupplier()
                                        .getCommoditySold(sl.getBasket().get(i));
                        commSold.setQuantity(Math.max(0, commSold.getQuantity() - origQuantity
                                                         + updatedQuantity));
                        commSold.setPeakQuantity(Math.max(commSold.getPeakQuantity() -
                                                          origPeakQuantity
                                                          + updatedPeakQuantity,
                                                          commSold.getQuantity()));
                    }
                }
            }
            economy.removeBasketBought(shoppingList);
        }
    }

    /**
     * A helper method to find all {@link ShoppingList} between guaranteed buyers and a given
     * seller.
     * @param economy the economy the seller is part of
     * @param seller the {@link Trader} whom may be a supplier for guaranteed buyers
     * @return a list of {@link ShoppingList}
     */
    public static List<ShoppingList> findSlsBetweenSellerAndGuaranteedBuyer(Economy economy,
                                                                            Trader seller) {
        List<ShoppingList> shoppingLists = new ArrayList<ShoppingList>();
        for (ShoppingList shoppingList : seller.getCustomers()) {
            if (shoppingList.getBuyer().getSettings().isGuaranteedBuyer()) {
                shoppingLists.add(shoppingList);
            }
        }
        return shoppingLists;
    }

    /**
     * Auxillary data-structure containing information needed to update shoppingLists
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
                                               info.getNewSupplier());
        }
    }

    /**
     * A helper method to get a map for a guaranteed buyer to all of its shopping list
     *
     * @param eonomy the economy
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

} // end GuaranteedBuyerHelper class
