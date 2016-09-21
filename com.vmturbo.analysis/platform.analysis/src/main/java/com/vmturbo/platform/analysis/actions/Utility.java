package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
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
     * removes the effect of the used values of the commoditiesBought by the consumer on
     * the used values of the commoditiesSold of the provisionedSeller
     * @param modelSeller this is the {@link Trader} that the new clone is based out off
     * @param provisionedSeller is the newly cloned {@link Trader}
     */
    public static void adjustOverhead(Trader modelSeller, Trader provisionedSeller) {
        for(ShoppingList sl : modelSeller.getCustomers()) {
            int buyerIndex = 0;
            for (CommoditySpecification commSpec : sl.getBasket()) {
                int soldIndex = provisionedSeller.getBasketSold().indexOf(commSpec);
                // The allocation commodities sold by the modelSeller is not going to be sold by 
                // the clone (we create new ones for the clone to sell). Hence, we wont find these
                // allocComms in the basketSold and we get a negative index (hence skipping them).
                if (soldIndex >= 0) {
                    provisionedSeller.getCommoditiesSold().get(soldIndex).setQuantity(
                                    Math.max(provisionedSeller.getCommoditiesSold().get(soldIndex)
                                    .getQuantity() - sl.getQuantity(buyerIndex), 0));
                    provisionedSeller.getCommoditiesSold().get(soldIndex).setPeakQuantity(
                                    provisionedSeller.getCommoditiesSold().get(soldIndex)
                                    .getQuantity());
                    buyerIndex++;
                }
            }
        }
    }
    
} // end Utility class
