package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.RangeTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteBelowMinAboveMaxCapacityLimitationQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * Test for StorageTierCostFunction.
 */
public class StorageTierCostFunctionTest {
    private static CommoditySpecification stAmt;
    private static CommoditySpecification iops;
    private static Economy economy;
    private static Trader storageTier;
    private static ShoppingList stSL;
    private static Trader vm;

    /**
     * Set up test variables.
     */
    @BeforeClass
    public static void setup() {
        stAmt = TestUtils.ST_AMT;
        iops = TestUtils.IOPS;
        economy = new Economy();
        vm = TestUtils.createVM(economy);
    }

    /**
     * Test calculateCost for storage tier when the VM with no region no business account(on prem VM).
     * Expected: VM will get cheapest cost across all regions all business accounts.
     */
    @Test
    public void testOnPremMCPStorageCost() {
        List<CommoditySpecification> stCommList = Arrays.asList(stAmt);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {500}, new double[] {500}, storageTier);
        CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(createStorageCost()).build());
        MutableQuote quote = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(600, quote.getQuoteValue(), 0.000);
    }

    /**
     * Helper method to create storageTier costDTO.
     * @return StorageTierCostDTO
     */
    private StorageTierCostDTO createStorageCost() {
        List<CostTuple> storageTuples = new ArrayList<>();
        storageTuples.add(CostTuple.newBuilder().setBusinessAccountId(1).setRegionId(1L)
                .setPrice(3.27).build());
        storageTuples.add(CostTuple.newBuilder().setBusinessAccountId(1).setRegionId(2L)
                .setPrice(4.15).build());
        storageTuples.add(CostTuple.newBuilder().setBusinessAccountId(2).setRegionId(2L)
                .setPrice(1.20).build());
        return StorageTierCostDTO.newBuilder().addStorageResourceCost(StorageResourceCost
                .newBuilder().setResourceType(TestUtils.stAmtTO)
                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                        .addAllCostTupleList(storageTuples).setIsAccumulativeCost(false)
                        .setIsUnitPrice(true).setUpperBound(Double.MAX_VALUE)))
                .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                        .setResourceType(TestUtils.stAmtTO).setMaxCapacity(10000)).build();
    }

    /**
     * Test final quote when commodity quantity is above max, or below min of StorageResourceLimitation,
     * with Savings or Reversibility mode.
     */
    @Test
    public void testResourceCapacityLimitationNotMet() {
        // Above max, return infinite quote.
        List<CommoditySpecification> stCommList = Arrays.asList(stAmt);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {500}, new double[] {500}, storageTier);
        CostFunction storageCostFunction1 = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                .setResourceType(TestUtils.stAmtTO).setMaxCapacity(100)).build()).build());
        MutableQuote quote1 = storageCostFunction1.calculateCost(stSL, storageTier, true, economy);
        assertTrue(quote1 instanceof InfiniteBelowMinAboveMaxCapacityLimitationQuote);
        // Below min in Reversibility mode, return quote with Savings mode with penalty.
        CostFunction storageCostFunction2 = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                .setResourceType(TestUtils.stAmtTO).setMaxCapacity(5000)
                                .setMinCapacity(1000).setCheckMinCapacity(true))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.stAmtTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(2)
                                                .setRegionId(2L).setPrice(1.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(true)
                                        .setUpperBound(Double.MAX_VALUE))).build()).build());
        MutableQuote quote2 = storageCostFunction2.calculateCost(stSL, storageTier, true, economy);
        assertEquals(1000 + CostFunctionFactoryHelper.REVERSIBILITY_PENALTY_COST, quote2.getQuoteValue(), 0.000);
        // Below min in Savings mode, return finite quote.
        // Cost is calculated with StorageResourceLimitation min capacity 1000.
        stSL.setDemandScalable(true);
        MutableQuote quote3 = storageCostFunction2.calculateCost(stSL, storageTier, true, economy);
        assertEquals(1000, quote3.getQuoteValue(), 0.000);
        // Finite quote carries CommodityContext with new demand
        assertEquals(1, quote3.getCommodityContexts().size());
        CommodityContext commContext = quote3.getCommodityContexts().iterator().next();
        assertEquals(1000, commContext.getNewCapacityOnSeller(), 0.000);
    }

    /**
     * Test final quote when dependent commodity quantity is larger than (base commodity quantity * max ratio)
     * of StorageResourceRatioDependency, in Savings and Reversibility mode.
     */
    @Test
    public void testRatioBasedResourceDependencyAboveMaxRatio() {
        // Above max ratio in Reversibility mode, return infinite quote.
        List<CommoditySpecification> stCommList = Arrays.asList(stAmt, iops);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000, 10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {500, 2100}, new double[] {500, 2100}, storageTier);
        CostDTO storageCostDTO = CostDTO.newBuilder()
                .setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceRatioDependency(StorageResourceRatioDependency.newBuilder()
                                .setBaseResourceType(TestUtils.stAmtTO)
                                .setDependentResourceType(TestUtils.iopsTO)
                                .setMaxRatio(3))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.stAmtTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(2)
                                                .setRegionId(2L).setPrice(1.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(true)
                                        .setUpperBound(Double.MAX_VALUE))).build()).build();
        CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(storageCostDTO);
        MutableQuote quote1 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(700 + CostFunctionFactoryHelper.REVERSIBILITY_PENALTY_COST, quote1.getQuoteValue(), 0.000);
        // Above max ratio in Savings mode, adjust base commodity quantity to achieve max ratio constraint.
        stSL.setDemandScalable(true);
        MutableQuote quote2 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(700, quote2.getQuoteValue(), 0.000);
        // Finite quote carries CommodityContext with new demand
        assertEquals(2, quote2.getCommodityContexts().size());
        CommodityContext stAmtContext = quote2.getCommodityContexts().stream()
                .filter(d -> stAmt.equals(d.getCommoditySpecification())).findAny().orElse(null);
        assertEquals(700, stAmtContext.getNewCapacityOnSeller(), 0.000);
        CommodityContext iopsContext = quote2.getCommodityContexts().stream()
                .filter(d -> iops.equals(d.getCommoditySpecification())).findAny().orElse(null);
        assertEquals(2100, iopsContext.getNewCapacityOnSeller(), 0.000);
    }

    /**
     * Test final quote when dependent commodity quantity is lower than (base commodity quantity * min ratio)
     * of StorageResourceRatioDependency, dependent commodity quantity is adjusted to achieve min ratio constraint.
     */
    @Test
    public void testRatioBasedResourceDependencyBelowMinRatio() {
        List<CommoditySpecification> stCommList = Arrays.asList(stAmt, iops);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000, 10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {500, 800}, new double[] {500, 800}, storageTier);
        CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceRatioDependency(StorageResourceRatioDependency.newBuilder()
                                .setBaseResourceType(TestUtils.stAmtTO)
                                .setDependentResourceType(TestUtils.iopsTO)
                                .setMaxRatio(5).setMinRatio(2))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.stAmtTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(2)
                                                .setRegionId(2L).setPrice(1.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(true)
                                        .setUpperBound(Double.MAX_VALUE)))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.iopsTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(2)
                                                .setRegionId(2L).setPrice(1.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(true)
                                        .setUpperBound(Double.MAX_VALUE))).build()).build());
        MutableQuote quote = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(1500, quote.getQuoteValue(), 0.000);
        // Finite quote carries CommodityContext with new demand
        assertEquals(2, quote.getCommodityContexts().size());
        CommodityContext stAmtContext = quote.getCommodityContexts().stream()
                .filter(d -> stAmt.equals(d.getCommoditySpecification())).findAny().orElse(null);
        assertEquals(500, stAmtContext.getNewCapacityOnSeller(), 0.000);
        CommodityContext iopsContext = quote.getCommodityContexts().stream()
                .filter(d -> iops.equals(d.getCommoditySpecification())).findAny().orElse(null);
        // dependent commodity quantity is adjusted to match min ratio constraint
        assertEquals(1000, iopsContext.getNewCapacityOnSeller(), 0.000);
    }

    /**
     * Test final quote when dependent commodity quantity exceeds the quantity with current base
     * commodity quantity based on StorageResourceRangeDependency, in Savings and Reversibility mode.
     */
    @Test
    public void testRangeBasedResourceDependencyNotMet() {
        // Reversibility mode, return quote with Savings mode with penalty.
        List<CommoditySpecification> stCommList = Arrays.asList(stAmt, iops);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000, 10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {100, 300}, new double[] {100, 300}, storageTier);
        CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceRangeDependency(StorageResourceRangeDependency.newBuilder()
                                .setBaseResourceType(TestUtils.stAmtTO).setDependentResourceType(TestUtils.iopsTO)
                                .addRangeTuple(RangeTuple.newBuilder().setBaseMaxCapacity(100).setDependentMaxCapacity(200))
                                .addRangeTuple(RangeTuple.newBuilder().setBaseMaxCapacity(200).setDependentMaxCapacity(400)))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.stAmtTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(2)
                                                .setRegionId(2L).setPrice(1.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(true)
                                        .setUpperBound(Double.MAX_VALUE))).build()).build());
        MutableQuote quote1 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        // Reversibility mode, return quote with Savings mode with penalty
        assertEquals(200 + CostFunctionFactoryHelper.REVERSIBILITY_PENALTY_COST, quote1.getQuoteValue(), 0.000);
        // Savings mode, adjust base commodity quantity to achieve dependent commodity quantity.
        stSL.setDemandScalable(true);
        MutableQuote quote2 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(200, quote2.getQuoteValue(), 0.000);
        // Finite quote carries CommodityContext with new demand
        assertEquals(2, quote2.getCommodityContexts().size());
        CommodityContext stAmtContext = quote2.getCommodityContexts().stream()
                .filter(d -> stAmt.equals(d.getCommoditySpecification())).findAny().orElse(null);
        assertEquals(200, stAmtContext.getNewCapacityOnSeller(), 0.000);
        CommodityContext iopsContext = quote2.getCommodityContexts().stream()
                .filter(d -> iops.equals(d.getCommoditySpecification())).findAny().orElse(null);
        assertEquals(400, iopsContext.getNewCapacityOnSeller(), 0.000);
    }
}
