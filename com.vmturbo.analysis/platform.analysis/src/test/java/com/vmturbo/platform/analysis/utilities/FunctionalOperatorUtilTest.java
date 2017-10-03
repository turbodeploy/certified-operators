package com.vmturbo.platform.analysis.utilities;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

public class FunctionalOperatorUtilTest {
    Economy economy;
    Trader vm;
    Trader pm;
    ShoppingList sl1, sl2;
    double epsilon = 0.00001;

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
                        pm.getCommoditySold(TestUtils.CPU), null, null, false);
        Assert.assertEquals(80, res[0], epsilon);
        Assert.assertEquals(150, res[1], epsilon);
    }

    /**
     * Unit test for SUB_COMM().
     * res[0] = max(0, 50 - 50) = 0.
     * res[1] = max(0, 90 - 90) = 0.
     */
    @Test
    public void testSubComm() {
        double[] res = FunctionalOperatorUtil.SUB_COMM.operate(sl1, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false);
        Assert.assertEquals(0, res[0], epsilon);
        Assert.assertEquals(0, res[1], epsilon);
    }

    /**
     * Unit test for UPDATE_EXPENSES().
     * Set 200 as the spent amount for the economy.
     * res1[0] = 200 - 30 + 40 = 210.
     * res1[1] 0.
     *
     * res2[0] = 40.
     * res2[1] = 60.
     */
    @Test
    public void testUpdateExpenses() {
        economy.setSpent(200);

        Trader vm1 = TestUtils.createVM(economy);
        Trader pm1 = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                        Arrays.asList(TestUtils.COST_COMMODITY), new double[] {100}, true, false);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COST_COMMODITY), vm1,
                        new double[] {40}, new double[] {60}, pm1);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COST_COMMODITY), vm1,
                        new double[] {30}, new double[] {50}, null);

        double[] res1 = FunctionalOperatorUtil.UPDATE_EXPENSES.operate(sl3, 0,
                        pm1.getCommoditySold(TestUtils.COST_COMMODITY), null, economy, false);
        Assert.assertEquals(210, res1[0], epsilon);
        Assert.assertEquals(0, res1[1], epsilon);

        double[] res2 = FunctionalOperatorUtil.UPDATE_EXPENSES.operate(sl3, 0,
                        pm1.getCommoditySold(TestUtils.COST_COMMODITY), null, economy, true);
        Assert.assertEquals(40, res2[0], epsilon);
        Assert.assertEquals(60, res2[1], epsilon);
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
                        pm.getCommoditySold(TestUtils.CPU), null, null, true);
        Assert.assertEquals(50, res1[0], epsilon);
        Assert.assertEquals(90, res1[1], epsilon);

        double[] res2 = FunctionalOperatorUtil.IGNORE_CONSUMPTION.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false);
        Assert.assertEquals(80, res2[0], epsilon);
        Assert.assertEquals(150, res2[1], epsilon);
    }

    /**
     * Unit test for AVG_COMMS().
     * res1[0] = (60 * 2 + 30)/(2 + 0) = 75.
     * res1[1] = (90 * 2 + 60)/(2 + 0) = 120.
     *
     * res2[0] = (60 * 2 + 30)/(2 + 1) = 50.
     * res2[1] = (90 * 2 + 60)/(2 + 1) = 80.
     */
    @Test
    public void testAvgComms() {
        // Add one more shopping list.
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {10}, new double[] {10}, pm);
        Assert.assertEquals(2, pm.getCustomers().size(), 0);

        double[] res1 = FunctionalOperatorUtil.AVG_COMMS.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, true);
        Assert.assertEquals(75, res1[0], epsilon);
        Assert.assertEquals(120, res1[1], epsilon);

        double[] res2 = FunctionalOperatorUtil.AVG_COMMS.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, false);
        Assert.assertEquals(50, res2[0], epsilon);
        Assert.assertEquals(80, res2[1], epsilon);
    }

    /**
     * Unit test for MAX_COMM().
     * res[0] = max(30, 50) = 50.
     * res[1] = max(60, 90) = 90.
     */
    @Test
    public void testMaxComm() {
        double[] res = FunctionalOperatorUtil.MAX_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false);
        Assert.assertEquals(50, res[0], epsilon);
        Assert.assertEquals(90, res[1], epsilon);
    }

    /**
     * Unit test for MIN_COMM().
     * res[0] = min(30, 50) = 30.
     * res[1] = min(60, 90) = 60.
     */
    @Test
    public void testMinComm() {
        double[] res = FunctionalOperatorUtil.MIN_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), null, null, false);
        Assert.assertEquals(30, res[0], epsilon);
        Assert.assertEquals(60, res[1], epsilon);
    }

    /**
     * Unit test for RETURN_BOUGHT_COMM().
     * res[0] = 30.
     * res[1] = 60.
     */
    @Test
    public void testReturnBoughtComm() {
        double[] res = FunctionalOperatorUtil.RETURN_BOUGHT_COMM.operate(sl2, 0, null, null, null,
                        false);
        Assert.assertEquals(30, res[0], epsilon);
        Assert.assertEquals(60, res[1], epsilon);
    }
}
