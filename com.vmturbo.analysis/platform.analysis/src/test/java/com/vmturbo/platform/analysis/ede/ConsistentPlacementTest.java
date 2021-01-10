package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.testUtilities.TestUtils.PM_TYPE;
import static com.vmturbo.platform.analysis.testUtilities.TestUtils.VM_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

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
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

/**
 * Tests for consistent scaling of cloud groups.
 */
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
     * @param topology topology to add Traders to
     * @param numBuyers number of buyers to create. If numBuyers > 1, the market will not be
     *                  automatically populated with Traders, and the test must do that manually.
     * @param numSellers number of sellers to create
     * @return list of Traders.  The first numBuyers Traders are the buyers, and the remaining
     * Traders are the sellers.
     */
    private Trader[] createTraders(Topology topology, int numBuyers, int numSellers) {
        Economy economy = topology.getEconomyForTesting();
        Trader[] traders = new Trader[numBuyers + numSellers];
        int traderIndex = 0;
         for (int buyerNum = 1; buyerNum <= numBuyers; buyerNum++) {
             final long oid = traderIndex + 100;
             final long slOid = oid + 100;
            // Create Traders in a single scaling group.
            Trader trader = topology.addTrader(oid, VM_TYPE, TraderState.ACTIVE, EMPTY,
                                               new ArrayList<>());
            trader.setDebugInfoNeverUseInCode("buyer-" + buyerNum);
            trader.setScalingGroupId("sg-1");
            trader.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
            ShoppingList shoppingList = topology.addBasketBought(slOid, trader, PM_SMALL).setMovable(true);
            // First buyer is the group leader
            shoppingList.setGroupFactor(buyerNum == 1 ? numBuyers : 0);
            economy.registerShoppingListWithScalingGroup("sg-1", shoppingList);
            traders[traderIndex++] = trader;
        }

        // Add sellers to economy
        for (int sellerNum = 1; sellerNum <= numSellers; sellerNum++) {
            long oid = traderIndex + 100;
            Trader seller = topology.addTrader(oid, PM_TYPE, TraderState.ACTIVE, PM_SMALL,
                                               new ArrayList<>());
            seller.setDebugInfoNeverUseInCode("seller-" + sellerNum);
            seller.getSettings().setCanAcceptNewCustomers(true);

            // Give capacity and utilization upper bound default values
            for (@NonNull CommoditySold commoditySold : seller.getCommoditiesSold()) {
                commoditySold.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND);
            }

            // Populate quantities sold
            for (int i = 0; i < quantities.length; ++i) {
                seller.getCommoditiesSold().get(i).setQuantity(quantities[i]);
            }
            traders[traderIndex++] = seller;
        }
        if (numSellers <= 1) {
            // If numSellers > 1, the test will execute this after modifying the economy further.
            topology.populateMarketsWithSellersAndMergeConsumerCoverage();
        }
        return traders;
    }

    private ShoppingList getSl(Economy economy, Trader trader) {
        // Return the first (and only) ShoppingList for buyer
        return economy.getMarketsAsBuyer(trader).keySet().iterator().next();
    }

    /**
     * Verify that actions are generated for all scaling group members that need to move when the
     * group leader wants to resize.  This is the normal consistent scaling case for cloud groups.
     */
    @Test
    public void testScalingGroupResize() {
        Topology topology = new Topology();
        Trader[] traders = createTraders(topology, 3, 1);  // 3 buyers, 1 seller
        Economy e = topology.getEconomyForTesting();

        // Expected actions are moves for both trader 1 and trader 2
        Trader seller = traders[3];
        final ShoppingList sl1 = getSl(e, traders[0]);
        Action[] expectedActions = {
            new Move(e, sl1, seller).setImportance(Double.POSITIVE_INFINITY),
            new Move(e, getSl(e, traders[1]), seller).setImportance(Double.POSITIVE_INFINITY),
            new Move(e, getSl(e, traders[2]), seller).setImportance(Double.POSITIVE_INFINITY)
        };

        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getInfinityQuoteTraders().isEmpty());
    }

    /**
     * Ensure that moves are not generated for a matched scaling group when the group leader
     * (and therefore all members) has no need to resize.
     */
    @Test
    public void testLeaderDoesntNeedToResizeMatchedGroup() {
        final Topology topology = new Topology();
        Economy e = topology.getEconomyForTesting();
        // 3 buyers, 2 sellers
        Trader[] traders = createTraders(topology, 3, 2);

        // Move all traders to seller-1, to make the scaling group consistently sized.
        Trader seller = traders[3];
        for (int traderNum = 0; traderNum < 3; traderNum++) {
            ShoppingList sl = getSl(e, traders[traderNum]);
            sl.move(seller);
        }
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();

        // Expected actions are no moves because the leader doesn't need to move and the group
        // is already consistently scaled.
        Action[] expectedActions = { };
        // Run placement on the group leader
        PlacementResults results = Placement
                        .generateShopAlonePlacementDecisions(e, getSl(e, traders[0]));
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getInfinityQuoteTraders().isEmpty());
    }

    /**
     * Ensure that moves are generated for a mismatched scaling group even when the group leader
     * has no need to resize.
     */
    @Test
    public void testLeaderDoesntNeedToResizeMismatchedGroup() {
        final Topology topology = new Topology();
        Economy e = topology.getEconomyForTesting();
        // 3 buyers, 2 sellers
        Trader[] traders = createTraders(topology, 3, 2);

        // Move the first two traders to seller-1 and the third one to seller-2 to make the
        // scaling group not consistently sized.
        Trader seller1 = traders[3];
        Trader seller2 = traders[4];
        for (int traderNum = 0; traderNum < 3; traderNum++) {
            ShoppingList sl = getSl(e, traders[traderNum]);
            sl.move(traderNum == 2 ? seller2 : seller1);
        }
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();

        // Expected actions are moves for all traders, because although the group leader does not
        // need to move, the scaling group is mismatched.
        Action[] expectedActions = {
                        new Move(e, getSl(e, traders[0]), seller1)
                                        .setImportance(Double.POSITIVE_INFINITY),
                        new Move(e, getSl(e, traders[1]), seller1)
                                        .setImportance(Double.POSITIVE_INFINITY),
                        new Move(e, getSl(e, traders[2]), seller1)
                                        .setImportance(Double.POSITIVE_INFINITY)
                        };
        // Run placement on the group leader
        PlacementResults results = Placement
                        .generateShopAlonePlacementDecisions(e, getSl(e, traders[0]));
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getInfinityQuoteTraders().isEmpty());
    }

    /**
     * Ensure that a reconfigure generated by a group leader synthesizes reconfigures for other
     * members of the scaling group.
     */
    @Test
    public void testScalingGroupReconfigure() {
        Topology topology = new Topology();
        Trader[] traders = createTraders(topology, 3, 0);  // 3 buyers, no sellers
        Economy e = topology.getEconomyForTesting();

        final ShoppingList sl1 = getSl(e, traders[0]);
        Action[] expectedActions = {
            new Reconfigure(e, sl1).setImportance(Double.POSITIVE_INFINITY),
            new Reconfigure(e, getSl(e, traders[1])).setImportance(Double.POSITIVE_INFINITY),
            new Reconfigure(e, getSl(e, traders[2])).setImportance(Double.POSITIVE_INFINITY)
        };

        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertArrayEquals(expectedActions, results.getActions().toArray());
        assertTrue(results.getInfinityQuoteTraders().isEmpty());
    }

    /**
     * The first move is the group leader's move.  Ensure that the other two moves are in the
     * subsequent actions list.  The other two moves should have no subsequent actions.
     */
    @Test
    public void testSubsequentActions() {
        Topology topology = new Topology();
        Trader[] traders = createTraders(topology, 3, 1);  // 3 buyers, 1 seller
        Economy e = topology.getEconomyForTesting();

        // Expected actions are moves for both trader 1 and trader 2
        Trader seller = traders[3];
        final ShoppingList sl1 = getSl(e, traders[0]);
        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
        assertEquals(3, results.getActions().size());
        assertEquals(3, results.getActions().get(0).getAllActions().size());
        assertEquals(1, results.getActions().get(1).getAllActions().size());
        assertEquals(1, results.getActions().get(2).getAllActions().size());
    }
}
