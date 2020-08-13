package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory.CapacityLimitation;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory.PriceData;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;

/**
 * Unit tests for cost calculate in CostFunctionFactory.
 *
 */
public class CostFunctionFactoryTest {

    private static Economy economy = new Economy();
    private static final int licenseAccessCommBaseType = 111;
    private static final int couponBaseType = 4;
    private static Trader computeTier;
    private static Trader storageTier;
    private static ShoppingList linuxComputeSL;
    private static ShoppingList stSL;
    private static ShoppingList windowsComputeSL;

    @BeforeClass
    public static void setUp() {
        List<CommoditySpecification> computeCommList = Arrays
                .asList(TestUtils.CPU, new CommoditySpecification(TestUtils.LINUX_COMM_TYPE,
                        licenseAccessCommBaseType), new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE,
                                licenseAccessCommBaseType));
        Trader linuxVM = TestUtils.createVM(economy);
        computeTier = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                computeCommList, new double[] {10000, 10000, 10000}, true, false);
        linuxComputeSL = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, new CommoditySpecification(TestUtils.LINUX_COMM_TYPE,
                        licenseAccessCommBaseType)), linuxVM, new double[] {50, 1},
                                new double[] {90, 1}, computeTier);
        linuxComputeSL.setGroupFactor(1);
        Trader windowsVM = TestUtils.createVM(economy);
        windowsComputeSL = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE,
                        licenseAccessCommBaseType)), windowsVM, new double[] {50, 1},
                                new double[] {90, 1}, computeTier);
        windowsComputeSL.setGroupFactor(1);

        List<CommoditySpecification> stCommList = Arrays.asList(TestUtils.ST_AMT);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT),
                linuxVM, new double[] {500}, new double[] {500}, storageTier);

    }

    /**
     * Helper method to create compute cost tuples.
     * @return
     */
    private List<CostTuple> generateComputeCostTuples() {
        List<CostTuple> computeTuples = new ArrayList<>();
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(1L)
                .setRegionId(100L).setLicenseCommodityType(TestUtils.LINUX_COMM_TYPE).setPrice(5).build());
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(2L).setRegionId(200L)
                .setLicenseCommodityType(TestUtils.WINDOWS_COMM_TYPE).setPrice(100).build());
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(2L).setZoneId(201L)
                .setLicenseCommodityType(TestUtils.WINDOWS_COMM_TYPE).setPrice(50).build());
        return computeTuples;
    }

    /**
     * Helper method to create compute cost DTO.
     * @return
     */
    private ComputeTierCostDTO createComputeCost() {
        return ComputeTierCostDTO.newBuilder().addAllCostTupleList(generateComputeCostTuples())
                .setCouponBaseType(couponBaseType)
                .setLicenseCommodityBaseType(licenseAccessCommBaseType).build();
    }

    /**
     * Helper method to create storage cost DTO.
     * @return
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
                        .setIsUnitPrice(true).setUpperBound(Double.MAX_VALUE))).build();
    }


    /**
     * Helper method to create cbtp cost DTO.
     * @return
     */
    private CbtpCostDTO createCbtpCost() {
        return CbtpCostDTO.newBuilder().setCouponBaseType(couponBaseType).setDiscountPercentage(0.5)
                .addAllCostTupleList(generateComputeCostTuples())
                .setLicenseCommodityBaseType(licenseAccessCommBaseType)
                .build();
    }

    /**
     * Tests storage tier quote lookup for with and without context cases.
     */
    @Test
    public void calculateStorageTierCost() {
        final long accountId1 = 101;
        final long accountId2 = 102;
        final long regionId11 = 211;
        final long regionId12 = 212;
        final long regionId21 = 221;

        // Price for 500 GB storage (SL requested amount) in region1 in account1 should be sum of:
        // 200 x $10 (unit price per GB) = $2000
        PriceData priceUpTo50Gb = new PriceData(200d, 10d, true, true, regionId11);
        // 300 x $20 (unit price per GB) = $6000
        PriceData priceUpTo250Gb = new PriceData(600d, 20d, true, true, regionId11);
        // For a total price of $8000

        // Price for 500 GB storage (SL requested amount) in region2 in account2 should be:
        // 500 x $10 (unit price) = $5000
        PriceData priceUnbounded = new PriceData(Double.POSITIVE_INFINITY, 10d, true, true, regionId21);

        // Add a bad bound, we are requesting 500, but there is only 1 bound in this region,
        // set to 300. So we should get back an infinite quote for this case.
        PriceData priceBadBound = new PriceData(300d, 30d, true, true, regionId12);

        // Add prices for both accounts to map.
        Map<Long, List<PriceData>> priceData = new HashMap<>();
        priceData.put(accountId1, ImmutableList.of(priceUpTo50Gb, priceUpTo250Gb));
        priceData.put(accountId2, ImmutableList.of(priceUnbounded, priceBadBound));

        // Setup commodity spec mapping.
        CommoditySpecification commSpec1 = TestUtils.ST_AMT;
        Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap = new HashMap<>();
        priceDataMap.put(commSpec1, priceData);

        Map<CommoditySpecification, CapacityLimitation> commCapacity = new HashMap<>();
        commCapacity.put(commSpec1, new CapacityLimitation(0d, 500d));

        final Trader buyerVm = TestUtils.createVM(economy);
        final ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Collections.singletonList(TestUtils.ST_AMT), buyerVm, new double[] {500},
                new double[] {500}, storageTier);

        // 1. Set the context to region 1 (higher price), verify we are getting that quote,
        // as we specifically asked for the account/region.
        BalanceAccount account1 = new BalanceAccount(100, 10000, accountId1, 0);
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId11, 0, account1));
        CommodityQuote quote1 = CostFunctionFactory.calculateStorageTierCost(priceDataMap,
                commCapacity, shoppingList, storageTier);
        assertNotNull(quote1);
        assertTrue(quote1.getContext().isPresent());
        Context context1 = quote1.getContext().get();
        assertEquals(regionId11, context1.getRegionId());
        assertEquals(accountId1, context1.getBalanceAccount().getId());
        assertEquals(8000d, quote1.getQuoteValue(), 0d);

        // 2. Bad bounds check for region12, should get infinite quote back.
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId12, 0, account1));
        CommodityQuote quote2 = CostFunctionFactory.calculateStorageTierCost(priceDataMap,
                commCapacity, shoppingList, storageTier);
        assertNotNull(quote2);
        assertFalse(quote2.getContext().isPresent());
        assertEquals(Double.POSITIVE_INFINITY, quote2.getQuoteValue(), 0d);

    }
}
