package com.vmturbo.platform.analysis.actions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
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
            double overhead = calculateCommodityOverhead(modelSeller, specSold, economy);
            CommoditySold cs = provisionedSeller.getCommoditiesSold().get(soldIndex);
            cs.setQuantity(overhead).setPeakQuantity(overhead);
            soldIndex++;
        }
    }

    /**
     * Get overhead for a commoditySpecification sold of a trader
     *
     * @param trader this is the {@link Trader} for whom the overhead is to be calculated
     * @param specSold the spec for which the overhead is to be calculated
     * @param economy  that the Trader is a part of
     */
    public static double calculateCommodityOverhead(Trader trader, CommoditySpecification specSold,
                    Economy economy) {
        double overhead = 0;
        if (economy.getCommsToAdjustOverhead().contains(specSold)) {
            int soldIndex = trader.getBasketSold().indexOf(specSold);
            if (soldIndex != -1) {
                // Overhead is the seller's comm sold quantity - sum of quantities of all the
                // customers of this seller
                overhead = trader.getCommoditiesSold().get(soldIndex).getQuantity();
                for(ShoppingList sl : trader.getCustomers()) {
                    int boughtIndex = sl.getBasket().indexOf(specSold);
                    if (boughtIndex != -1) {
                        overhead -= sl.getQuantity(boughtIndex);
                    }
                }
            }
        }
        return Math.max(overhead, 0);
    }

    /**
     * Creates a map of newly generated {@link CommoditySpecification}s that are sold
     * by the clone in place of the commodities sold by the modelSeller
     *
     * @param shoppingList shopping list who may be cloned
     * @return a map where the key and values are a {@link CommoditySpecification}s with same
     *              "baseType" but different "commodityType"
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                            createCommSpecWithNewKeys (ShoppingList sl) {
        Map<CommoditySpecification, CommoditySpecification> newCommSoldMap = new HashMap<>();
        // Note: we start assign type based from Integer.MAX_VALUE and keep decrementing from this
        Iterator<@NonNull @ReadOnly CommoditySpecification> iter = sl.getBasket().iterator();
        while(iter.hasNext()) {
            CommoditySpecification cs = iter.next();
            if (!newCommSoldMap.containsKey(cs) && cs.isCloneWithNewType()) {
                // change commType and add commSpecs into the basket
                newCommSoldMap.put(cs, new CommoditySpecification(cs.getBaseType(),
                                                                  cs.getQualityLowerBound(),
                                                                  cs.getQualityUpperBound(),
                                                                  cs.isCloneWithNewType()));
            }
        }
        return newCommSoldMap;
    }

    /**
     * A helper method to create a new {@link Basket} containing all the
     * {@link CommoditySpecification}s present in origBasketSold except the ones present in
     * commToReplaceMap that are to be replaced by the new ones with unique commodityType (that
     * represents new key)
     *
     * <p>
     *  There may be a new market being created in economy due to the new basket.
     * </p>
     *
     * @param commToReplaceMap is a map where the key is a {@link CommoditySpecification} with
     *  a "commodityType" that need to be replaced by the new {@link CommoditySpecification}
     *  represented by the corresponding value.
     * @param origBasket original basket bought or sold
     * @return a new {@link Basket} that contains the commodities that are replaced based on
     *  commToReplaceMap
     */
    public static Basket transformBasket(Map<CommoditySpecification, CommoditySpecification>
                                         commToReplaceMap, Basket origBasket) {
        // replacing the old commSpecs with the ones in the commToReplaceMap. eg application
        // commodity should be replaced with a new type when cloning the sl or seller contains an
        // application commodity
        Basket newBasket = new Basket(origBasket);
        for (Entry<CommoditySpecification, CommoditySpecification> entry : commToReplaceMap
                        .entrySet()) {
            newBasket = newBasket.remove(entry.getKey());
            newBasket = newBasket.add(entry.getValue());
        }
        return newBasket;
    }
} // end Utility class
