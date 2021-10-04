package com.vmturbo.platform.analysis.utilities;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

public class ProvisionUtils {
    static final Logger logger = LogManager.getLogger();

    /**
     * Check if the modelSeller has enough capacity for every commodity bought by a trader.
     *
     * @param buyerShoppingList is the {@link ShoppingList} of the buyer
     * @param modelSeller is the {@link Trader} that we will be checking to see if there is enough
     *                    capacity for all the commodities listed in the modelBuyer
     * @param economy the {@link Economy} that contains the unplaced {@link Trader}
     *
     * @return TRUE if the buyer fits in this modelSeller, FALSE otherwise
     */
    public static boolean canBuyerFitInSeller(@Nonnull ShoppingList buyerShoppingList,
            @Nonnull Trader modelSeller, @Nonnull Economy economy) {
        return insufficientCapacityComms(buyerShoppingList, modelSeller, economy, true).isEmpty();
    }

    /**
     * Check if the seller has enough capacity for every commodity bought by a trader.
     *
     * @param buyerShoppingList is the {@link ShoppingList} of the buyer.
     * @param seller is the {@link Trader} that we will be checking to see if there is enough
     *     capacity for all the commodities listed in the modelBuyer.
     * @param economy the {@link Economy} that contains the traders.
     * @param exitEarly return as soon as 1 commodity is insufficient.
     *
     * @return Set of comm specs that provider does not have sufficient capacity for the buyer.
     */
    @Nonnull
    public static Set<CommoditySpecification> insufficientCapacityComms(
            @Nonnull ShoppingList buyerShoppingList, @Nonnull Trader seller,
            @Nonnull Economy economy, boolean exitEarly) {
        Basket basket = buyerShoppingList.getBasket();
        Basket basketSold = seller.getBasketSold();
        Set<CommoditySpecification> insufficientComms = new HashSet<>();
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size();
             boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);
            if (soldIndex >= basketSold.size()) {
                logger.error("Unexpected potential supplier {} for {} needing commodity {}"
                        + " which seller does not sell",
                    seller.getDebugInfoNeverUseInCode(), buyerShoppingList.getDebugInfoNeverUseInCode(),
                    basketCommSpec.getDebugInfoNeverUseInCode());
                return Sets.newHashSet(basketCommSpec);
            }
            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.equals(basketSold.get(soldIndex))) {
                soldIndex++;
                if (soldIndex >= basketSold.size()) {
                    logger.error("Unexpected potential supplier {} for {} needing commodity {}"
                            + " which seller does not sell",
                        seller.getDebugInfoNeverUseInCode(), buyerShoppingList.getDebugInfoNeverUseInCode(),
                        basketCommSpec.getDebugInfoNeverUseInCode());
                    return Sets.newHashSet(basketCommSpec);
                }
            }
            CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);
            double overHead = 0;
            double overHeadPeak = 0;
            if (economy.getCommsToAdjustOverhead().contains(basketCommSpec)) {
                // eliminate the overhead from the effective capacity to make sure there is still
                // enough resource for shopping list
                overHead = commSold.getQuantity();
                overHeadPeak = commSold.getPeakQuantity();
                // Inactive trader usually have no customers so it will skip the loop
                for (ShoppingList sl : seller.getCustomers()) {
                    int index = sl.getBasket().indexOf(basketCommSpec);
                    if (index != -1) {
                        overHead = overHead - sl.getQuantity(index);
                        overHeadPeak = overHeadPeak - sl.getPeakQuantity(index);
                    }
                }
                if (overHead < 0) {
                    logger.debug("overHead is less than 0 for seller "
                        + seller.getDebugInfoNeverUseInCode() + " commodity "
                        + basketSold.get(soldIndex).getDebugInfoNeverUseInCode());
                    overHead = 0;
                }
                if (overHeadPeak < 0) {
                    logger.debug("overHeadPeak is less than 0 for seller "
                        + seller.getDebugInfoNeverUseInCode() + " commodity "
                        + basketSold.get(soldIndex).getDebugInfoNeverUseInCode());
                    overHeadPeak = 0;
                }
            }
            if ((buyerShoppingList.getQuantities()[boughtIndex] > (commSold.getEffectiveCapacity() - overHead))
                    || (buyerShoppingList.getPeakQuantities()[boughtIndex] >
                        (commSold.getEffectiveCapacity() - overHeadPeak))) {
                insufficientComms.add(basketCommSpec);
                if (exitEarly) {
                    return insufficientComms;
                }
            }
        }
        return insufficientComms;
    }
}