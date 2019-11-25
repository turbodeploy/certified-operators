package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.testUtilities.TestUtils.VM_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
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

    @Test
    public void testScalingGroupResize() {
        Economy e = new Economy();

        // Create two Traders in a single scaling group.
        Trader trader1 = e.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        trader1.setDebugInfoNeverUseInCode("trader-1");
        trader1.setScalingGroupId("sg-1");
        trader1.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
        ShoppingList sl1 =  e.addBasketBought(trader1, PM_SMALL).setMovable(true);
        sl1.setGroupFactor(2);
        e.registerShoppingListWithScalingGroup("sg-1", sl1);

        Trader trader2 = e.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        trader2.setDebugInfoNeverUseInCode("trader-2");
        trader2.setScalingGroupId("sg-1");
        trader2.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
        ShoppingList sl2 =  e.addBasketBought(trader2, PM_SMALL).setMovable(true);;
        sl2.setGroupFactor(0);
        e.registerShoppingListWithScalingGroup("sg-1", sl2);

        // Add seller to economy
        Trader seller = e.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.setDebugInfoNeverUseInCode("seller");
        seller.getSettings().setCanAcceptNewCustomers(true);

        // Give capacity and utilization upper bound default values
        for (@NonNull CommoditySold commoditySold : seller.getCommoditiesSold()) {
            commoditySold.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND);
        }

        // Populate quantities sold
        for (int i = 0 ; i < quantities.length ; ++i) {
            seller.getCommoditiesSold().get(i).setQuantity(quantities[i]);
        }

        e.populateMarketsWithSellers();

        // Expected actions are moves for both trader 1 and trader 2
        Action[] expectedActions = {
            new Move(e, sl1, seller).setImportance(Double.POSITIVE_INFINITY),
            new Move(e, sl2, seller).setImportance(Double.POSITIVE_INFINITY)
        };

        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getUnplacedTraders().isEmpty());
    }

    @Test
    public void testScalingGroupReconfigure() {
    }
}
