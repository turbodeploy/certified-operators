package com.vmturbo.platform.analysis.updatingfunction;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * A functional interface for the commodity update function.
 */
@FunctionalInterface
public interface UpdatingFunction extends Serializable {
    /**
     * The function to update commodity values during move or provision/suspension.
     *
     * @param buyer       the shopping list that is being moved. For provision, this is the
     *                    shopping list being cloned. For suspension, the buyer is null
     * @param boughtIndex the index of the bought commodity
     * @param commSold    the commodity whose quantities will be updated
     * @param seller      the seller of the commodity. For provision, this is the seller being
     *                    cloned. For suspension, this is the seller being suspended.
     * @param economy     the economy the seller participates in
     * @param take        if the action will be taken
     * @param overhead    the overhead of the commodity
     * @param currentSLs  the current set of shopping lists that belong to the same guaranteed
     *                    buyer. This is only set during provision and suspension
     * @return an array of containing the updated quantity and peak quantity
     */
    double[] operate(ShoppingList buyer, int boughtIndex,
                     @Nonnull CommoditySold commSold,
                     @Nonnull Trader seller,
                     @Nonnull UnmodifiableEconomy economy,
                     boolean take, double overhead,
                     @Nullable Set<ShoppingList> currentSLs);
}
