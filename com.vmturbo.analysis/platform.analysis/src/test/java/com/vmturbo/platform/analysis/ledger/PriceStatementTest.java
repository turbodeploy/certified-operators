package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;

/**
 * Test the functionality of {@link PriceStatement#computePriceIndex}.
 */
public class PriceStatementTest {

    private static final double MAX_UNIT_PRICE = 1e22;

    /**
     * Setup economy with 1 PM selling CPU and memory and 1 DS selling storage.
     * Expected Result: 2 traders added to the price statement info.
     */
    @Test
    public void testComputePriceIndex_NewTradersNumber() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().size(), 2);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory and 1 DS selling storage.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_NoCommoditiesSold() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_DSSellsStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                       Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU from PM.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.5) ^ 2 = 4
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPU() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                      Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 4.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU and memory from PM.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.7) ^ 2 = 100/9
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemory() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                     Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 11.1111111111, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU from PM and storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.5) ^ 2 = 4
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUAndDSSellsStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{260}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 4.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU and memory from PM and storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.7) ^ 2 = 100/9
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryAndDSSellsStorage() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{240}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 11.1111111111, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_DSSellsStorageTwoBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                       Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{100}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU from PM.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.8) ^ 2 = 25
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUTwoBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                      Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 0}, pm1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 25.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU and memory from PM.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.9) ^ 2 = 100
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryTwoBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                     Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 20}, pm1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 100.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU from PM and storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.8) ^ 2 = 25
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUAndDSSellsStorageTwoBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{130}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{130}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 25.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU and memory from PM and storage from DS.
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0.9) ^ 2 = 100
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryAndDSSellsStorageTwoBuyers() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{110}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 20}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{170}, st1);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 100.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory and 1 DS selling storage.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_NoCommoditiesSoldLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.7);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_DSSellsStorageLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                       Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU from PM.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 5/9) ^ 2 = 5.0625
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPULowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                      Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 5.0625, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU and memory from PM.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 7/9) ^ 2 = 20.25
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                     Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.6);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 20.25, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU from PM and storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 5/9) ^ 2 = 5.0625
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUAndDSSellsStorageLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{260}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 5.0625, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 1 VM buying CPU and memory from PM and storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 7/8) ^ 2 = 64
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryAndDSSellsStorageLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.7);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 64.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 0) ^ 2 = 1
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_DSSellsStorageTwoBuyersLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                       Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{120}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{150}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.7);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU from PM.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = MAX_UNIT_PRICE (when commSoldUtil=1)
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUTwoBuyersLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                      Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 0}, pm1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 0}, pm1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), MAX_UNIT_PRICE, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU and memory from PM.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = MAX_UNIT_PRICE (when commSoldUtil=1)
     *                  priceIndex for DS = 1 when utilization < 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryTwoBuyersLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                     Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 20}, pm1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), MAX_UNIT_PRICE, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 1.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU from PM and storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = 1 / (1 - 7/8) ^ 2 = 64
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUAndDSSellsStorageTwoBuyersLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{40, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{130}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{130}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.8);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), 64.0, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with 1 PM selling CPU and memory, 1 DS selling storage
     * and 2 VMs buying CPU and memory from PM and storage from DS.
     * Upper bound utilization for all commodities is set to a number < 1
     * Formulas: commSoldUtil = quantity / (utilizationUpperBound * capacity)
     *           priceIndex = 1 / (1 - commSoldUtil) ^ 2
     * Expected result: priceIndex for PM = MAX_UNIT_PRICE (when commSoldUtil=1)
     *                  priceIndex for DS = 10000 when utilization > 80%
     */
    @Test
    public void testComputePriceIndex_PMSellsCPUMemoryAndDSSellsStorageTwoBuyersLowerUtilUpperBound() {
        Economy economy = new Economy();
        economy.getSettings().setEstimatesEnabled(false);
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, true);
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, true);
        // Place vm1 and vm2 on pm1 and st1.
        Trader vm1 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                    Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{50, 70}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{140}, st1);
        Trader vm2 = TestUtils.createVM(economy);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2, new double[]{30, 20}, pm1);
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[]{110}, st1);
        // Setting upper bound utilization for all commodities
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) pm1.getCommoditiesSold().get(1)).setUtilizationUpperBound(0.9);
        ((CommoditySoldSettings) st1.getCommoditiesSold().get(0)).setUtilizationUpperBound(0.9);
        PriceStatement priceStatement = new PriceStatement();
        priceStatement.computePriceIndex(economy);

        assertEquals(priceStatement.getTraderPriceStatements().get(0).getPriceIndex(), MAX_UNIT_PRICE, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(priceStatement.getTraderPriceStatements().get(1).getPriceIndex(), 10000.0, TestUtils.FLOATING_POINT_DELTA);
    }
}
