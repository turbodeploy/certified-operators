package com.vmturbo.platform.analysis.utilities;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;

/**
 * Tests for UpdatingFunctionFactory.
 */
@RunWith(JUnitParamsRunner.class)
public class UpdatingFunctionFactoryTest {

    private static final long zoneId = 0L;
    Economy economy;
    Trader vm;
    Trader pm;
    ShoppingList sl1;
    ShoppingList sl2;

    /**
     * Set up before tests.
     * Set up a VM, a PM and a ShoppingList.
     * The PM's CPU capacity is 100.
     * The shopping list's CPU quantity is 50, CPU peak quantity is 90.
     */
    @Before
    public void setUp() {
        initializeEconomy(true);
    }

    private void initializeEconomy(boolean placePM) {
        economy = new Economy();
        vm = TestUtils.createVM(economy)
                .setDebugInfoNeverUseInCode("vm-1");
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                        Arrays.asList(TestUtils.CPU), new double[] {100}, true, false)
                .setDebugInfoNeverUseInCode("pm-1");
        sl1 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {50}, new double[] {90}, placePM ? pm : null);
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
        double[] res = Move.updatedQuantities(economy, UpdatingFunctionFactory.ADD_COMM, sl2,
                0, pm, 0, false, null, true);
        Assert.assertEquals(80, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(150, res[1], TestUtils.FLOATING_POINT_DELTA);
        pm.getCommoditiesSold()
                .forEach(commoditySold -> Assert.assertEquals(1, commoditySold.getNumConsumers()));
    }

    /**
     * Unit test for SUB_COMM().
     * res[0] = max(0, 50 - 50) = 0.
     * res[1] = max(0, 90 - 90) = 0.
     */
    @Test
    public void testSubComm() {
        double[] res = Move.updatedQuantities(economy, UpdatingFunctionFactory.SUB_COMM, sl1,
                0, pm, 0, false, null, false);
        Assert.assertEquals(0, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(0, res[1], TestUtils.FLOATING_POINT_DELTA);
        pm.getCommoditiesSold()
                .forEach(commoditySold -> Assert.assertEquals(1, commoditySold.getNumConsumers()));
    }

    /**
     * Unit test for IGNORE_CONSUMPTION().
     * res1[0] = 50.
     * res1[1] = 90.
     * res2[0] = 0.
     * res2[1] = 0.
     */
    @Test
    public void testIgnoreConsumption() {
        double[] res1 = Move.updatedQuantities(economy, UpdatingFunctionFactory.IGNORE_CONSUMPTION, sl2,
                0, pm, 0, true, null, true);
        Assert.assertEquals(50, res1[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res1[1], TestUtils.FLOATING_POINT_DELTA);
        double[] res2 = Move.updatedQuantities(economy, UpdatingFunctionFactory.IGNORE_CONSUMPTION, sl2,
                0, pm, 0, false, null, true);
        Assert.assertEquals(0, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(0, res2[1], TestUtils.FLOATING_POINT_DELTA);
        pm.getCommoditiesSold()
                .forEach(commoditySold -> Assert.assertEquals(2, commoditySold.getNumConsumers()));
    }

    /**
     * Unit test for AVG_COMMS().
     * There are 2 Shopping lists on PM.
     * SL1 (already on PM)      = 50 used and 90 peak.
     * SL  (already on PM)      = 10 used and 30 peak.
     * PM has 60 used and 90 peak. This is treated as current average.
     *
     * <p>Scenario 1:
     * SL2 (moving into the PM) = 90 used and 120 peak.
     * res1[0] = (60 * 2 + 90)/(2 + 1) = 70. Result = Max(70, 60 (current avg)) = 70.
     * res1[1] = (90 * 2 + 120)/(2 + 1) = 100. Result = Max(100, 90 (current avg)) = 100.
     *
     * Scenario 2:
     * SL2 (moving out of the PM) = 90 used and 120 peak.
     * res1[0] = (70 * 3 - 90)/(3 - 1) = 60. Result = Min(60, 70 (current avg)) = 60.
     * res1[1] = (100 * 3 - 120)/(3 - 1) = 90. Result = Min(90, 100 (current avg)) = 90.
     *
     * Scenario 3:
     * SL2 (asking for quote from PM) = 90 used and 120 peak.
     * res1[0] = (60 * 2 + 90)/(2 + 1) = 70. Result = Max(70, 60 (current avg)) = 70.
     * res1[1] = (90 * 2 + 120)/(2 + 1) = 100. Result = Max(100, 90 (current avg)) = 100.
     * </p>
     */
    @Test
    public void testAvgComms() {
        // Add one more shopping list.
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
                        new double[] {10}, new double[] {30}, pm);
        Assert.assertEquals(2, pm.getCustomers().size());

        sl2 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
            new double[] {90}, new double[] {120}, null);

        // Moving into PM
        double[] res1 = Move.updatedQuantities(economy, UpdatingFunctionFactory.AVG_COMMS, sl2,
                0, pm, 0, true, null, true);
        CommoditySold commoditySold = pm.getCommoditySold(TestUtils.CPU);
        Assert.assertEquals(70, res1[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(100, res1[1], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(3, commoditySold.getNumConsumers());

        commoditySold.setQuantity(res1[0]).setPeakQuantity(res1[1]);

        // Moving out of PM
        double[] res2 = Move.updatedQuantities(economy, UpdatingFunctionFactory.AVG_COMMS, sl2,
                0, pm, 0, true, null, false);
        Assert.assertEquals(60, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res2[1], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(2, commoditySold.getNumConsumers());
        commoditySold.setQuantity(res2[0]).setPeakQuantity(res2[1]);

        // Asking for a quote from PM
        double[] res3 = Move.updatedQuantities(economy, UpdatingFunctionFactory.AVG_COMMS, sl2,
                0, pm, 0, false, null, true);
        Assert.assertEquals(70, res3[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(100, res3[1], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(2, commoditySold.getNumConsumers());
    }

    /**
     * Unit test for MAX_COMM().
     * SL1 (used = 50, peak = 90) and SL3 (used = 60, peak = 80) are on the PM.
     * Set the PM quantity as 60 and peakQuantity as 90.
     * SL 2 is incoming and has 100 used and 100 peak.
     * When SL2 is moving in, Max updating function should return 100, 100.
     * When SL2 is moving out, Max updating function should return the max out of its other
     * customers - 60 and 90.
     */
    @Test
    public void testMaxComm() {
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
            new double[] {60}, new double[] {80}, pm);
        sl2 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
            new double[] {100}, new double[] {100}, null);
        sl2.move(pm);
        pm.getCommoditySold(TestUtils.CPU).setQuantity(60).setPeakQuantity(90);
        double[] res = UpdatingFunctionFactory.MAX_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, true, null, true);
        Assert.assertEquals(100, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(100, res[1], TestUtils.FLOATING_POINT_DELTA);

        pm.getCommoditySold(TestUtils.CPU).setQuantity(res[0]).setPeakQuantity(res[1]);
        double[] res2 = UpdatingFunctionFactory.MAX_COMM.operate(sl2, 0,
            pm.getCommoditySold(TestUtils.CPU), pm, null, true, null, false);
        Assert.assertEquals(60, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(90, res2[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for MIN_COMM().
     * res[0] = min(30, 50) = 30.
     * res[1] = min(60, 90) = 60.
     */
    @Test
    public void testMinComm() {
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU), vm,
            new double[] {60}, new double[] {80}, pm);
        pm.getCommoditySold(TestUtils.CPU).setQuantity(50).setPeakQuantity(80);
        sl2.move(pm);
        double[] res = UpdatingFunctionFactory.MIN_COMM.operate(sl2, 0,
                        pm.getCommoditySold(TestUtils.CPU), pm, null, true, null, true);
        Assert.assertEquals(30, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(60, res[1], TestUtils.FLOATING_POINT_DELTA);
        pm.getCommoditySold(TestUtils.CPU).setQuantity(res[0]).setPeakQuantity(res[1]);
        double[] res2 = UpdatingFunctionFactory.MIN_COMM.operate(sl2, 0,
            pm.getCommoditySold(TestUtils.CPU), pm, null, true, null, false);
        Assert.assertEquals(50, res2[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(80, res2[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for RETURN_BOUGHT_COMM().
     * res[0] = 30.
     * res[1] = 60.
     */
    @Test
    public void testReturnBoughtComm() {
        double[] res = UpdatingFunctionFactory.RETURN_BOUGHT_COMM.operate(sl2, 0, null, null, null,
                        false, null, true);
        Assert.assertEquals(30, res[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(60, res[1], TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Unit test for MERGE_PEAK.
     *
     * <p>Start with commodity used = 50 and peak = 90
     * Update with used = 30 and peak = 60
     * Using the peak merging updating function, the results should be:
     * - Used = current used + new used = 80
     * - Peak = current used + new used + sqrt((current peak - current used)^2 + (new peak - new used)^2)
     *      which is 50 + 30 + sqrt((90 - 50)^2 + (60 - 30)^2)
     *      which is 80 + sqrt(1600 + 900)
     *      which is 80 + sqrt(2500)
     *      which is 80 + 50 = 130
     */
    @Test
    @Parameters({
            // Initial SL, SL to move in, SL to move out
            "sl1,sl2,sl1",  // expecting sl2 to remain
            "sl1,sl2,sl2",  // expecting sl1 to remain
            "sl2,sl1,sl1",  // expecting sl2 to remain
            "sl2,sl1,sl2"   // expecting sl1 to remain
    })
    @TestCaseName("Test #{index}: Start with {0}, move in {1}, move out {2}")
    public void testMergePeakComm(String initialSL, String inSL, String outSL) {
        initializeEconomy(false);  // Do environment setup without any initial SL placements
        Map<String, ShoppingList> sls = ImmutableMap.of("sl1", sl1, "sl2", sl2);

        // Initial move in
        Move.updateQuantities(economy, sls.get(initialSL), pm, UpdatingFunctionFactory.MERGED_PEAK, true);

        // Second move in
        Move.updateQuantities(economy, sls.get(inSL), pm, UpdatingFunctionFactory.MERGED_PEAK, true);
        CommoditySold updatedCommSold = pm.getCommoditySold(TestUtils.CPU);
        Assert.assertEquals(80d, updatedCommSold.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(130d, updatedCommSold.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);

        // Move out. Backing out one SL should yield the other SL
        Move.updateQuantities(economy, sls.get(outSL), pm, UpdatingFunctionFactory.MERGED_PEAK, false);
        updatedCommSold = pm.getCommoditySold(TestUtils.CPU);
        ShoppingList resultantSL = outSL.equals("sl1") ? sl2 : sl1;
        Assert.assertEquals(resultantSL.getQuantity(0), updatedCommSold.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(resultantSL.getPeakQuantity(0), updatedCommSold.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Verify inner updatedQuantities using the merged peak updating function.
     */
    @Test
    public void testUpdatedQuantities() {
        double[] result = Move.updatedQuantities(economy, UpdatingFunctionFactory.MERGED_PEAK, sl2,
                0, pm, 0, false, null, true);
        Assert.assertEquals(80d, result[0], TestUtils.FLOATING_POINT_DELTA);
        Assert.assertEquals(130d, result[1], TestUtils.FLOATING_POINT_DELTA);
    }
}
