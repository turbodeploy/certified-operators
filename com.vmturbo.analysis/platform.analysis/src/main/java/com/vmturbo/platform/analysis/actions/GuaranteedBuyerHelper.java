package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
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
     * @param modelSLs a list of {@link ShoppingList} that would be used as model to create
     *                      the buyer-seller relation
     * @param newSupplier the {@link Trader} which would be a seller for the guaranteed buyers
     * @param commSpecMap maps the {@link CommoditySpecification} sold by the modelSeller to the
     *                      guaranteedBuyer to the one to be sold by the newSupplier to the guaranteedBuyer
     */
    public static void addShoppingListForGuaranteedBuyers(Economy economy, List<ShoppingList> modelSLs,
                                                          Trader newSupplier, Map<CommoditySpecification,
                                                          CommoditySpecification> commSpecMapNew) {
        for (ShoppingList sl : modelSLs) {
            Trader guaranteedBuyer = sl.getBuyer();
            // use the commSpecMap and create a newBasket based on every basket that the guaranteedBuyers
            // were buying from the original host
            Basket newBasket = createBasketForClone(sl.getBasket(), commSpecMapNew);
            ShoppingList newSl = economy.addBasketBought(guaranteedBuyer, newBasket);
            newSl.move(newSupplier);
            newSl.setMovable(sl.isMovable());
            for (int boughtIndex = 0; boughtIndex < newBasket.size(); ++boughtIndex) {
                newSl.setQuantity(boughtIndex, sl.getQuantity(boughtIndex))
                     .setPeakQuantity(boughtIndex, sl.getPeakQuantity(boughtIndex));
                // update quantity of the newSupplier to construct the buyer-seller relation
                CommoditySold commSold = newSupplier.getCommoditySold(newBasket.get(boughtIndex));
                commSold.setQuantity(commSold.getQuantity() + sl.getQuantity(boughtIndex));
                // resize the commSold by the guaranteedBuyer to the amount that it buys
                CommoditySold commSoldByBuyer = null;
                for (int commSoldIndex = 0; commSoldIndex < guaranteedBuyer.getBasketSold().size()
                                ; commSoldIndex++) {
                    if (guaranteedBuyer.getBasketSold().get(commSoldIndex).getBaseType()
                                    == newBasket.get(boughtIndex).getBaseType()) {
                        commSoldByBuyer = guaranteedBuyer.getCommoditiesSold().get(commSoldIndex);
                        break;
                    }
                }
                if (commSoldByBuyer != null) {
                    commSoldByBuyer.setCapacity(commSoldByBuyer.getCapacity() + commSold.getCapacity());
                } else {
                    logger.warn("unable to find commSold by guaranteedBuyer to resize based on "
                                    + "commBought" + newBasket.get(boughtIndex) + " from "
                                    + newSupplier.getDebugInfoNeverUseInCode());
                }
            }
        }
    }

    /**
     * A helper method to remove buyer-seller relation between guaranteed buyers and a seller.
     * @param economy the economy the target is part of
     * @param shoppingListForGuaranteedBuyers a list of {@link ShoppingList} needs to removed
     *        between the guaranteed the buyer and seller
     * @param target the seller to be removed from supplier list of guaranteed buyers
     */
    public static void removeShoppingListForGuaranteedBuyers(Economy economy,
                                                             List<ShoppingList> shoppingListForGuaranteedBuyers,
                                                             Trader target) {
        for (ShoppingList shoppingList : shoppingListForGuaranteedBuyers) {
            // update capacity of the targetSeller because we force to remove the buyer-seller relation
            for (int i = 0; i < shoppingList.getBasket().size(); i++) {
                CommoditySold commSold = shoppingList.getSupplier()
                                .getCommoditySold(shoppingList.getBasket().get(i));
                commSold.setCapacity(commSold.getCapacity() - shoppingList.getQuantity(i));
            }
            economy.removeBasketBought(shoppingList);
        }
    }

    /**
     * A helper method to find all {@link ShoppingList} between guaranteed buyers and a seller.
     * @param economy the economy the seller is part of
     * @param seller the {@link Trader} whom may be a supplier for guaranteed buyers
     * @return a list of {@link ShoppingList}
     */
    public static List<ShoppingList> findShoppingListForGuaranteedBuyer(Economy economy, Trader seller) {
        List<ShoppingList> shoppingLists = new ArrayList<ShoppingList>();
        for (ShoppingList shoppingList : seller.getCustomers()) {
            if (shoppingList.getBuyer().getSettings().isGuaranteedBuyer()) {
                shoppingLists.add(shoppingList);
            }
        }
        return shoppingLists;
    }

    /**
     * A helper method to create a new {@link Basket} containing all the {@link CommoditySpecification}s
     * present in origBasketSold except that the ones present in commToReplaceMap that are to be replaced by the
     * new ones with unique commodityType (that represents new key)
     *
     * @param commToReplaceMap is a map where the key is a {@link CommoditySpecification} with a "commodityType"
     * sold by the modelSeller that need to be replaced by the new {@link CommoditySpecification}
     * represented by the corresponding value that is sold only by the clone.
     * @param origBasketSold is the {@link Basket} that the original modelSeller sells
     * @return a new {@link Basket} that contains all the commodities to be sold by the clone
     */
    public static Basket transformBasket(Map<CommoditySpecification, CommoditySpecification> commToReplaceMap
                    , Basket origBasketSold) {
        // creating a new basket that has all the commoditySpecs that the modelSeller sells
        Basket newBasket = new Basket(origBasketSold);
        // replacing the old commSpecs with the ones in the commToReplaceMap. eg, in the case of
        // allocation commodities, we replace the commodities sold by the modelSeller with the
        // new ones that are to be sold by the clone
        for (Entry<CommoditySpecification, CommoditySpecification> entry : commToReplaceMap.entrySet()) {
            newBasket = newBasket.remove(entry.getKey());
            newBasket = newBasket.add(entry.getValue());
        }
        return newBasket;
    }

    /**
     * This method creates a map of newly generated {@link CommoditySpecification}s that are sold
     * by the clone in place of the commodities sold by the modelSeller
     *
     * @param shoppingLists is the list of {@link shoppingList}s that the guaranteedBuyer buys from
     *              the modelSeller
     * @return a map where the key and values are a {@link CommoditySpecification}s with same
     *              "baseType" but different "commodityType"
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                            createCommSpecWithNewKeys (List<ShoppingList> shoppingLists) {
        Map<CommoditySpecification, CommoditySpecification> newCommSoldMap = new HashMap<>();
        shoppingLists.forEach(sl -> {
            // since we start assign type based from Integer.MAX_VALUE and keep decrementing from this
            // we must start cloning CommoditySpecs in a basket starting from the last entity(the one
            // with the largest type to the one with the smallest.)
            Iterator<@NonNull @ReadOnly CommoditySpecification> iter = sl.getBasket().reverseIterator();
            while(iter.hasNext()) {
                CommoditySpecification cs = iter.next();
                if (!newCommSoldMap.containsKey(cs)) {
                    // change commType and add commSpecs into the basket
                    newCommSoldMap.put(cs, new CommoditySpecification(cs.getBaseType(), cs
                                  .getQualityLowerBound(), cs.getQualityUpperBound()));
                }
            }
        });
        return newCommSoldMap;
    }

    private static List<BuyerInfo> guaranteedBuyersToProcess = new ArrayList<>();

    /**
     * Auxillary data-structure containing information needed to update shoppingLists
     * that are to be sold by new clones and bought by guaranteedBuyers
    */
    public static class BuyerInfo {

        public BuyerInfo(List<ShoppingList> sls, Trader targetSeller,
                         @NonNull Map<CommoditySpecification, CommoditySpecification> commSpecMapNew) {
            sls_ = sls;
            newSupplier_ = targetSeller;
            commSpecMapNew_ = commSpecMapNew;
        }

        List<ShoppingList> sls_;
        Trader newSupplier_;
        Map<CommoditySpecification, CommoditySpecification> commSpecMapNew_;

        public List<ShoppingList> getSLs() {
            return sls_;
        }
        public Trader getNewSupplier() {
            return newSupplier_;
        }
        public Map<CommoditySpecification, CommoditySpecification> getCommSpecMapNew() {
            return commSpecMapNew_;
        }

    }

    /**
     * create a new basket using an existing basket
     * @param originalBasket is the basket that basket that the guaranteedBuyers were buying from
     *                       the original host
     * @param commSpecMapNew map of oldCommSpecs to the one with new type that are sold by the clone
     * @return basket containing commSpecs that are derived from the originalBasket with new type
     *
     */
    static Basket createBasketForClone(Basket originalBasket, Map<CommoditySpecification, CommoditySpecification> commSpecMapNew) {
        List<CommoditySpecification> commSpecList = new ArrayList<>();
        originalBasket.forEach(cs -> commSpecList.add(commSpecMapNew.get(cs)));
        return new Basket(commSpecList);
    }

    /**
     * store shoppingLists of traders that need to be added to the economy. Hence, creating new
     * markets.
     *
     * @param sls list of {@link ShoppingList} that the guaranteedBuyers shop for from the
     *            targetSeller
     * @param targetSeller the newly provisioned trader
     * @param commSpecMapNew map of oldCommSpecs to the one with new type that are sold by the clone
     */
    public static void storeGuaranteedbuyerInfo (List<ShoppingList> sls,
                    Trader newSupplier, @NonNull Map<CommoditySpecification,
                    CommoditySpecification> commSpecMapNew) {
        guaranteedBuyersToProcess.add(new BuyerInfo(sls, newSupplier, commSpecMapNew));
    }

    /**
     * iterate over the "guaranteedBuyersToProcess" list that holds saved shoppingLists that need to be
     * added for the guaranteedBuyers
     *
     * @param economy is the {@link Economy} into which markets selling new baskets sold
     * by the newly provisioned clones and bought by the guaranteedBuyers are added.
     */
    public static void processGuaranteedbuyerInfo (Economy economy) {
        for (BuyerInfo info : guaranteedBuyersToProcess) {
            addShoppingListForGuaranteedBuyers(economy, info.getSLs(), info
                            .getNewSupplier(), info.getCommSpecMapNew());
        }
        guaranteedBuyersToProcess.clear();
    }

} // end GuaranteedBuyerHelper class
