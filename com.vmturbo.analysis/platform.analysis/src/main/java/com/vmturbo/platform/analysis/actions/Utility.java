package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
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
     * For every commodity sold by a newly provisioned seller which may incur overhead, initialize 
     * the used value to a computed overhead. Commodity overhead is computed through the model seller
     * by subtracting the quantities bought by all its buyers from the total quantity if is selling
     * 
     * @param modelSeller this is the {@link Trader} that the new clone is based out off
     * @param provisionedSeller is the newly cloned {@link Trader}
     * @param economy that the Traders are a part of
     */
    public static void adjustOverhead(Trader modelSeller, Trader provisionedSeller,
            						  Economy economy) {
        int soldIndex = 0;
        for (CommoditySpecification specSold : provisionedSeller.getBasketSold()) {
            CommoditySold cs = provisionedSeller.getCommoditiesSold().get(soldIndex);
            if (economy.getCommsToAdjustOverhead().contains(specSold)) {
                for(ShoppingList sl : modelSeller.getCustomers()) {
                    int boughtIndex = sl.getBasket().indexOf(specSold);
                    if (boughtIndex != -1) {
                        cs.setQuantity(Math.max(cs.getQuantity() - sl.getQuantity(boughtIndex), 0))
                                  .setPeakQuantity(cs.getQuantity());
                    }
                }
            } else {
                // set used and peakUsed for soldComm to 0
                cs.setQuantity(0).setPeakQuantity(0);
            }
            soldIndex++;
        }
    }
    
} // end Utility class
