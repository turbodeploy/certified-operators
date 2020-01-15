package com.vmturbo.platform.analysis.utilities;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.BootstrapSupply;

public class ProvisionUtils {
    static final Logger logger = LogManager.getLogger();
    /**
     * check if the modelSeller has enough capacity for every commodity bought by a trader.
     *
     * @param buyerShoppingList is the {@link ShoppingList} of the buyer
     * @param modelSeller is the {@link Trader} that we will be checking to see if there is enough
     *                    capacity for all the commodities listed in the modelBuyer
     * @param economy the {@link Economy} that contains the unplaced {@link Trader}
     *
     * @return TRUE if the buyer fits in this modelSeller, FALSE otherwise
     */
    public static boolean canBuyerFitInSeller(ShoppingList buyerShoppingList, Trader modelSeller,
                                              Economy economy) {
        Basket basket = buyerShoppingList.getBasket();
        Basket basketSold = modelSeller.getBasketSold();
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size();
             boughtIndex++, soldIndex++) {
            if (soldIndex >= basketSold.size()) {
                return false;
            }
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);
            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.equals(basketSold.get(soldIndex))) {
                soldIndex++;
                if (soldIndex >= basketSold.size()) {
                    return false;
                }
            }
            CommoditySold commSold = modelSeller.getCommoditiesSold().get(soldIndex);
            double overHead = 0;
            double overHeadPeak = 0;
            if (economy.getCommsToAdjustOverhead().contains(basketCommSpec)) {
                // eliminate the overhead from the effective capacity to make sure there is still
                // enough resource for shopping list
                overHead = commSold.getQuantity();
                overHeadPeak = commSold.getPeakQuantity();
                // Inactive trader usually have no customers so it will skip the loop
                for (ShoppingList sl : modelSeller.getCustomers()) {
                    int index = sl.getBasket().indexOf(basketCommSpec);
                    if (index != -1) {
                        overHead = overHead - sl.getQuantity(index);
                        overHeadPeak = overHeadPeak - sl.getPeakQuantity(index);
                    }
                }
                if (overHead < 0) {
                    logger.warn("overHead is less than 0 for seller "
                            + modelSeller.getDebugInfoNeverUseInCode() + " commodity "
                            + basketSold.get(soldIndex).getDebugInfoNeverUseInCode());
                    overHead = 0;
                }
                if (overHeadPeak < 0) {
                    logger.debug("overHeadPeak is less than 0 for seller "
                            + modelSeller.getDebugInfoNeverUseInCode() + " commodity "
                            + basketSold.get(soldIndex).getDebugInfoNeverUseInCode());
                    overHeadPeak = 0;
                }
            }
            if ((buyerShoppingList.getQuantities()[boughtIndex] > (commSold.getEffectiveCapacity() - overHead))
                    || (buyerShoppingList.getPeakQuantities()[boughtIndex] >
                    (commSold.getEffectiveCapacity() - overHeadPeak))) {
                return false;
            }
        }
        return true;
    }
}