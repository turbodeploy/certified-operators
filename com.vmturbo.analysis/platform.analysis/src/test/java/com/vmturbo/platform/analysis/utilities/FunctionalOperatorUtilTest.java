package com.vmturbo.platform.analysis.utilities;

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

public class FunctionalOperatorUtilTest {

    private static final long zoneId = 0L;
    Economy economy;
    Trader vm;
    Trader pm;
    ShoppingList sl1, sl2;

    /**
     * Set up before tests.
     * Set up a VM, a PM and a ShoppingList.
     * The PM's CPU capacity is 100.
     * The shopping list's CPU quantity is 50, CPU peak quantity is 90.
     */
    @Before
    public void setUp() {
        economy = new Economy();
        vm = TestUtils.createVM(economy);
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                        Arrays.asList(TestUtils.CPU), new double[] {100}, true, false);
        sl1 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {50}, new double[] {90}, pm);
        sl2 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {30}, new double[] {60}, null);
    }

    /**
     * Unit test for ADD_COMM().
     * res[0] = 50 + 30 = 80.
     * res[1] = 90 + 60 = 150.
     */
    @Test
    public void testAddComm() {
        double[] res = FunctionalOperatorUtil.ADD_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false, 0);
        Assert.assertEquals(80, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(150, res[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for SUB_COMM().
     * res[0] = max(0, 50 - 50) = 0.
     * res[1] = max(0, 90 - 90) = 0.
     */
    @Test
    public void testSubComm() {
        double[] res = FunctionalOperatorUtil.SUB_COMM.operate(sl1, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false, 0);
        Assert.assertEquals(0, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(0, res[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for IGNORE_CONSUMPTION().
     * res1[0] = 50.
     * res1[1] = 90.
     *
     * res2[0] = 30 + 50 = 80.
     * res2[1] = 60 + 90 = 150.
     */
    @Test
    public void testIgnoreConsumption() {
        double[] res1 = FunctionalOperatorUtil.IGNORE_CONSUMPTION.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, true, 0);
        Assert.assertEquals(50, res1[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res1[1], TestUtils.FLOATING_POINT_DELTA);

        double[] res2 = FunctionalOperatorUtil.IGNORE_CONSUMPTION.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false, 0);
        Assert.assertEquals(0, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(0, res2[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for AVG_COMMS().
     * res1[0] = (60 * 2 + 30)/(2 + 0) = 75.
     * res1[1] = (90 * 2 + 60)/(2 + 0) = 120.
     *
     * res2[0] = (60 * 2 + 30)/(2 + 1) = 50. Take Max(60, 50).
     * res2[1] = (90 * 2 + 60)/(2 + 1) = 80. Take Max(90, 80).
     * Provider usage should not decrease when a VM is moving into it.
     */
    @Test
    public void testAvgComms() {
        // Add one more shopping list.
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {10}, new double[] {10}, pm);
        Assert.assertEquals(2, pm.getCustomers().size());
        // populate numConsumers on the commSold by the pm
        pm.getCommoditiesSold().forEach(cs -> cs.setNumConsumers(2));

        double[] res1 = FunctionalOperatorUtil.AVG_COMMS.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, true, 0);
        Assert.assertEquals(75, res1[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(120, res1[1], TestUtils.FLOATING_POINT_DELTA);

        double[] res2 = FunctionalOperatorUtil.AVG_COMMS.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, false, 0);
        Assert.assertEquals(60, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res2[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for MAX_COMM().
     * res[0] = max(30, 50) = 50.
     * res[1] = max(60, 90) = 90.
     */
    @Test
    public void testMaxComm() {
        double[] res = FunctionalOperatorUtil.MAX_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false, 0);
        Assert.assertEquals(50, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for MIN_COMM().
     * res[0] = min(30, 50) = 30.
     * res[1] = min(60, 90) = 60.
     */
    @Test
    public void testMinComm() {
        double[] res = FunctionalOperatorUtil.MIN_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false, 0);
        Assert.assertEquals(30, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(60, res[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for RETURN_BOUGHT_COMM().
     * res[0] = 30.
     * res[1] = 60.
     */
    @Test
    public void testReturnBoughtComm() {
        double[] res = FunctionalOperatorUtil.RETURN_BOUGHT_COMM.operate(sl2, 0, null, null, null,
                        false, 0);
        Assert.assertEquals(30, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(60, res[1], TestUtils.FLOATING_POINT_DELTA);
    }
}
