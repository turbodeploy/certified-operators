package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * Comprises a number of static utility methods used by many {@link Action} implementations.
 */
public final class Utility {
    // Methods

    /**
     * Appends a human-readable string identifying a trader to a string builder in the form
     * "name [oid] (#index)".
     *
     * @param builder The {@link StringBuilder} to which the string should be appended.
     * @param trader The {@link Trader} for which to append the identifying string.
     * @param uuid A function from {@link Trader} to trader UUID.
     * @param name A function from {@link Trader} to human-readable trader name.
     */
    public static void appendTrader(@NonNull StringBuilder builder, @NonNull Trader trader,
                                    @NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                    @NonNull Function<@NonNull Trader, @NonNull String> name) {
        builder.append(name.apply(trader)).append(" [").append(uuid.apply(trader)).append("] (#")
               .append(trader.getEconomyIndex()).append(")");
    }

    /**
     * A helper method to create {@link ShoppingList} to establish buyer-seller relation between
     *  guaranteed buyers and a newly provisioned or activated trader.
     * @param economy the economy the targetSeller should be part of
     * @param participations a list of {@link ShoppingList} that would be used as model to create the buyer-seller relation
     * @param targetSeller the {@link Trader} which would be a seller for the guaranteed buyers
     */
    public static void addShoppingListForGuaranteedBuyers(Economy economy, List<ShoppingList> participations,
                                                          Trader targetSeller) {
        for (ShoppingList modelShoppingList :participations) {
            ShoppingList newShoppingList = economy.addBasketBought(modelShoppingList.getBuyer(),
                                                                   modelShoppingList.getBasket());
            newShoppingList.move(targetSeller);
            newShoppingList.setMovable(modelShoppingList.isMovable());
            // TODO: discuss and decide what should be a reasonable quantity for new seller and guaranteed buyer
            for (int i = 0; i < modelShoppingList.getBasket().size(); ++i) {
                newShoppingList.setQuantity(i, modelShoppingList.getQuantity(i));
                newShoppingList.setPeakQuantity(i, modelShoppingList.getPeakQuantity(i));
                // update capacity of the targetSeller because we force to construct the buyer-seller relation
                CommoditySold commSold =
                                targetSeller.getCommoditySold(modelShoppingList.getBasket().get(i));
                commSold.setCapacity(commSold.getCapacity() + modelShoppingList.getQuantity(i));
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
} // end Utility class
