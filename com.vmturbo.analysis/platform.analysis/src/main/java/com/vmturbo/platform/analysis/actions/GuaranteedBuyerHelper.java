package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    // Methods

    /**
     * A helper method to create {@link ShoppingList} to establish buyer-seller relation between
     *  guaranteed buyers and a newly provisioned or activated trader.
     * @param economy the economy the targetSeller should be part of
     * @param participations a list of {@link ShoppingList} that would be used as model to create
     *                      the buyer-seller relation
     * @param targetSeller the {@link Trader} which would be a seller for the guaranteed buyers
     * @param modelBasket is the {@link Basket} that is to be purchased by the guaranteedBuyers
     */
    public static void addShoppingListForGuaranteedBuyers(Economy economy, List<ShoppingList> participations,
                                                          Trader targetSeller, Basket modelBasket) {
        if (modelBasket != null) {
            for (ShoppingList modelShoppingList : participations) {
                Trader guranteedBuyer = modelShoppingList.getBuyer();
                ShoppingList newShoppingList = economy.addBasketBought(guranteedBuyer, modelBasket);
                newShoppingList.move(targetSeller);
                newShoppingList.setMovable(modelShoppingList.isMovable());
                for (int boughtIndex = 0; boughtIndex < modelBasket.size(); ++boughtIndex) {
                    newShoppingList.setQuantity(boughtIndex, modelShoppingList
                                       .getQuantity(boughtIndex))
                                   .setPeakQuantity(boughtIndex, modelShoppingList
                                       .getPeakQuantity(boughtIndex));
                    // update quantity of the targetSeller to construct the buyer-seller relation
                    CommoditySold commSold =
                                    targetSeller.getCommoditySold(modelBasket.get(boughtIndex));
                    commSold.setQuantity(commSold.getQuantity() + modelShoppingList
                                                    .getQuantity(boughtIndex));
                    // resize the commSold by the guaranteedBuyer to the amount that it buys
                    CommoditySold commSoldByBuyer = null;
                    for (int commSoldIndex = 0; commSoldIndex < guranteedBuyer.getBasketSold().size()
                                    ; commSoldIndex++) {
                        if (guranteedBuyer.getBasketSold().get(commSoldIndex).getBaseType()
                                        == modelBasket.get(boughtIndex).getBaseType()) {
                            commSoldByBuyer = guranteedBuyer.getCommoditiesSold().get(commSoldIndex);
                            break;
                        }
                    }
                    commSoldByBuyer.setCapacity(commSoldByBuyer.getCapacity() + modelShoppingList
                                    .getQuantity(boughtIndex));
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
     * This method creates a map of newly generated {@link CommoditySpecification}s that are sold by the clone
     * in place of the commodities sold by the modelSeller
     *
     * @param shoppingList is the shoppingList that the guaranteedBuyer buys from the modelSeller
     * @return a map where the key and values are a {@link CommoditySpecification}s with same "baseType" but
     * different "commodityType"
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                            createCommSpecWithNewKeys (ShoppingList shoppingList) {
        Map<CommoditySpecification, CommoditySpecification> newCommSoldMap = new HashMap<>();
        shoppingList.getBasket().forEach(
                        cs ->
                             // change commType and add commSpecs into the basket
                             newCommSoldMap.put(cs, new CommoditySpecification(cs.getBaseType(), cs
                                             .getQualityLowerBound(), cs.getQualityUpperBound()))
        );
        return newCommSoldMap;
    }

    private static List<BuyerInfo> guaranteedBuyersToProcess = new ArrayList<>();

    /**
     * Auxillary data-structure containing information needed to update shoppingLists
     * that are to be sold by new clones and bought by guaranteedBuyers
    */
    public static class BuyerInfo {

        public BuyerInfo(List<ShoppingList> participations, Trader targetSeller,
                        Basket modelBasket) {
            this.participations = participations;
            this.targetSeller = targetSeller;
            this.modelBasket = modelBasket;
        }

        private List<ShoppingList> participations;
        private Trader targetSeller;
        private Basket modelBasket;

        public List<ShoppingList> getParticipations() {
            return participations;
        }
        public Trader getTargetSeller() {
            return targetSeller;
        }
        public Basket getModelBasket() {
            return modelBasket;
        }

    }

    /**
     * store shoppingLists of traders that need to be added to the economy. Hence, creating new markets.
     *
     * @param participants list of {@link ShoppingList} that the guaranteedBuyers shop for
     *                     from the targetSeller
     * @param targetSeller the newly provisioned trader
     * @param modelBasket is the newly created basket that the guaranteedBuyers shop for
     */
    public static void storeGuaranteedbuyerInfo (List<ShoppingList> participations,
                    Trader targetSeller, Basket modelBasket) {
        guaranteedBuyersToProcess.add(new BuyerInfo(participations, targetSeller, modelBasket));
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
            addShoppingListForGuaranteedBuyers(economy, info.getParticipations(), info
                            .getTargetSeller(), info.getModelBasket());
        }
        guaranteedBuyersToProcess.clear();
    }

} // end GuaranteedBuyerHelper class
