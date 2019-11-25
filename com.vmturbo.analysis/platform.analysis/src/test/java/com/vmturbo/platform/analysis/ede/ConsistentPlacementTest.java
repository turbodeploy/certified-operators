package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.testUtilities.TestUtils.VM_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

public class ConsistentPlacementTest {
    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private double[] quantities = {1800.0, 4194304.0};
    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU, MEM);
    private static final double CAPACITY = 111;
    private static final double UTILIZATION_UPPER_BOUND = 0.9;

    /**
     * Return a list of test Traders.  All buyers will be added to a single scaling group
     * @param economy economy to add Traders to
     * @param numBuyers number of buyers to create
     * @param numSellers number of sellers to create
     * @return list of Traders.  The first numBuyers Traders are the buyers, and the remaining
     * Traders are the sellers.
     */
    private Trader[] createTraders(Economy economy, int numBuyers, int numSellers) {
        Trader[] traders = new Trader[numBuyers + numSellers];
        int traderIndex = 0;
         for (int i = 1; i <= numBuyers; i++) {
            // Create two Traders in a single scaling group.
            Trader trader = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
            trader.setDebugInfoNeverUseInCode("buyer-" + i);
            trader.setScalingGroupId("sg-1");
            trader.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
            ShoppingList shoppingList = economy.addBasketBought(trader, PM_SMALL).setMovable(true);
            // First buyer is the group leader
            shoppingList.setGroupFactor(i == 1 ? numBuyers : 0);
            economy.registerShoppingListWithScalingGroup("sg-1", shoppingList);
            traders[traderIndex++] = trader;
        }

        // Add sellers to economy
        for (int i = 1; i <= numSellers; i++) {
            Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
            seller.setDebugInfoNeverUseInCode("seller-" + i);
            seller.getSettings().setCanAcceptNewCustomers(true);

            // Give capacity and utilization upper bound default values
            for (@NonNull CommoditySold commoditySold : seller.getCommoditiesSold()) {
                commoditySold.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND);
            }

            // Populate quantities sold
            for (i = 0; i < quantities.length; ++i) {
                seller.getCommoditiesSold().get(i).setQuantity(quantities[i]);
            }
            traders[traderIndex++] = seller;
        }
        economy.populateMarketsWithSellers();
        return traders;
    }

    private ShoppingList getSl(Economy economy, Trader trader) {
        // Return the first (and only) ShoppingList for buyer
        return economy.getMarketsAsBuyer(trader).keySet().iterator().next();
    }

    @Test
    public void testScalingGroupResize() {
        Economy e = new Economy();
        Trader[] traders = createTraders(e, 3, 1);  // 3 buyers, 1 seller

        // Expected actions are moves for both trader 1 and trader 2
        Trader seller = traders[3];
        final ShoppingList sl1 = getSl(e, traders[0]);
        Action[] expectedActions = {
            new Move(e, sl1, seller).setImportance(Double.POSITIVE_INFINITY),
            new Move(e, getSl(e, traders[2]), seller).setImportance(Double.POSITIVE_INFINITY),
            new Move(e, getSl(e, traders[1]), seller).setImportance(Double.POSITIVE_INFINITY),
        };

        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getUnplacedTraders().isEmpty());
    }

    @Test
    public void testScalingGroupReconfigure() {
        Economy e = new Economy();
        Trader[] traders = createTraders(e, 3, 0);  // 3 buyers, no sellers

        final ShoppingList sl1 = getSl(e, traders[0]);
        Action[] expectedActions = {
            new Reconfigure(e, sl1).setImportance(Double.POSITIVE_INFINITY),
            new Reconfigure(e, getSl(e, traders[2])).setImportance(Double.POSITIVE_INFINITY),
            new Reconfigure(e, getSl(e, traders[1])).setImportance(Double.POSITIVE_INFINITY)
        };

        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getUnplacedTraders().isEmpty());
    }
}
