package com.vmturbo.platform.analysis.utilities;

import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple.DependentResourceOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseServerTierCostDTO;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;

/**
 * Parameterized test cases to test the resizing of commodities based on the StorageResourceRatioDependency.
 */
@RunWith(JUnitParamsRunner.class)
public class DBSRatioBasedResizeTest {

    private static final long DB_REGION_ID = 11L;
    private static final long DB_BUSINESS_ACCOUNT_ID = 33L;
    private static final int licenseAccessCommBaseType = 111;
    private static final int DB_LICENSE_COMMODITY_TYPE = 200;
    private static final double DELTA = 0.01;
    private static final double DBS_TIER_COST = 10;
    private static final int couponBaseType = 4;
    private static final CommoditySpecification dbLicense = new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, licenseAccessCommBaseType);

    private Economy economy;
    private Context context;
    Trader databaseServerTier;

    /**
     * Setup an economy with a databaseServerTier.
     */
    @Before
    public void setUp() {
        economy = new Economy();
        Context.BalanceAccount balanceAccount = Mockito.mock(Context.BalanceAccount.class);
        context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        List<CommoditySpecification> dbsCommList = Arrays.asList(TestUtils.IOPS, TestUtils.ST_AMT,
                dbLicense);
        databaseServerTier = TestUtils.createTrader(economy, TestUtils.DB_TIER_TYPE,
                Arrays.asList(0L), dbsCommList, new double[]{8000, 2048, 10000}, false, false);
    }

    /**
     * Test body for DBS ratio based resize.
     * @param storageAmountDemand storage amount demand
     * @param iopsDemand iops demand
     * @param minRatio iops has to be at least minRatio times the storageAmountDemand
     * @param maxRatio iops cannot exceed max ratio times the storageAmountDemand
     * @param stAmtOptions storage amount dependent resource options
     * @param iopsOptions iops dependent resource options
     * @param expectedTotalPrice expected total price
     * @param expectedStorageAmount expected storage amount
     * @param expectedIops expected iops
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: testDBSRatioBasedResize({0}, {1}, {2}, {3}, {6}, {7}, {8})")
    public final void testDBSRatioBasedResize(double storageAmountDemand, double iopsDemand, Double minRatio, Double maxRatio,
                                              List<DependentResourceOption> stAmtOptions, List<DependentResourceOption> iopsOptions,
                                              double expectedTotalPrice, double expectedStorageAmount, double expectedIops) {
        ShoppingList dbsSL = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.IOPS, TestUtils.ST_AMT, dbLicense), TestUtils.createDBS(economy),
                new double[]{iopsDemand, storageAmountDemand, 0},
                new double[]{iopsDemand, storageAmountDemand, 0}, databaseServerTier);
        dbsSL.getBuyer().getSettings().setContext(context);
        DatabaseServerTierCostDTO dbsCostDTO = createDBSCostDTO(
                Optional.ofNullable(minRatio), Optional.of(maxRatio), stAmtOptions, iopsOptions);
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbsCostDTO.getCostTupleList(0));
        Quote.MutableQuote quote1 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseServerTier, dbsSL,
                        costTable, licenseAccessCommBaseType);
        //check total price
        Assert.assertEquals(expectedTotalPrice, quote1.quoteValues[0], DELTA);
        //storage amount
        Quote.CommodityContext commodityContext1 = quote1.getCommodityContexts().get(0);
        Assert.assertEquals(TestUtils.ST_AMT.getType(),
                commodityContext1.getCommoditySpecification().getType());
        Assert.assertEquals(expectedStorageAmount, commodityContext1.getNewCapacityOnSeller(), DELTA);
        //iops amount
        Quote.CommodityContext commodityContext2 = quote1.getCommodityContexts().get(1);
        Assert.assertEquals(TestUtils.IOPS.getType(),
                commodityContext2.getCommoditySpecification().getType());
        Assert.assertEquals(expectedIops, commodityContext2.getNewCapacityOnSeller(), DELTA);
    }

    /**
     * Create DBS Cost DTO.
     * @param minRatio iops has to be at least minRatio times the storageAmountDemand
     * @param maxRatio iops cannot exceed max ratio times the storageAmountDemand
     * @param stAmtOptions storage amount dependent resource options
     * @param iopsOptions iops dependent resource options
     * @return Created DatabaseServerTierCostDTO
     */
    public static DatabaseServerTierCostDTO createDBSCostDTO(Optional<Double> minRatio, Optional<Double> maxRatio,
                                                             List<DependentResourceOption> stAmtOptions,
                                                             List<DependentResourceOption> iopsOptions) {
        CommodityDTOs.CommoditySpecificationTO stAmtCommSpecTO = AnalysisToProtobuf.commoditySpecificationTO(TestUtils.ST_AMT);
        CommodityDTOs.CommoditySpecificationTO iopsCommSpecTO = AnalysisToProtobuf.commoditySpecificationTO(TestUtils.IOPS);
        CostDTOs.CostDTO.CostTuple.DependentCostTuple dependentCostTuple1 = CostDTOs.CostDTO.CostTuple.DependentCostTuple.newBuilder()
                .setDependentResourceType(stAmtCommSpecTO)
                .addAllDependentResourceOptions(stAmtOptions)
                .build();
        CostDTOs.CostDTO.CostTuple.DependentCostTuple dependentCostTuple2 = CostDTOs.CostDTO.CostTuple.DependentCostTuple.newBuilder()
                .setDependentResourceType(iopsCommSpecTO)
                .addAllDependentResourceOptions(iopsOptions)
                .build();

        CostDTOs.CostDTO.CostTuple.Builder costTuple = CostDTOs.CostDTO.CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(licenseAccessCommBaseType)
                .setPrice(DBS_TIER_COST)
                .addDependentCostTuples(dependentCostTuple1)
                .addDependentCostTuples(dependentCostTuple2)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID);
        if (minRatio.isPresent() || maxRatio.isPresent()) {
            CostDTOs.CostDTO.StorageResourceRatioDependency.Builder ratioDependency =
                    CostDTOs.CostDTO.StorageResourceRatioDependency.newBuilder()
                            .setBaseResourceType(stAmtCommSpecTO)
                            .setDependentResourceType(iopsCommSpecTO);
            minRatio.ifPresent(ratioDependency::setMinRatio);
            maxRatio.ifPresent(ratioDependency::setMaxRatio);
            costTuple.addStorageResourceRatioDependency(ratioDependency);
        }
        return DatabaseServerTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .build();
    }

    /**
     * Creates a DependentResourceOption.
     * @param increment increment
     * @param endRange endRange
     * @param price price
     * @return Created DependentResourceOption
     */
    public static DependentResourceOption createOption(long increment, long endRange, double price) {
        return DependentResourceOption.newBuilder()
                .setAbsoluteIncrement(increment)
                .setEndRange(endRange)
                .setPrice(price)
                .build();
    }

    private static List<DependentResourceOption> createGP2StorageAmountOptions() {
        return ImmutableList.of(
                createOption(5, 5, 0.1),
                createOption(1, 65536, 0.1));
    }

    private static List<DependentResourceOption> createGP2IOPSOptions() {
        return ImmutableList.of(
                createOption(100, 100, 0),
                createOption(1, 16000, 0));
    }

    private static List<DependentResourceOption> createIO1StorageAmountOptions() {
        return ImmutableList.of(
                createOption(20, 20, 0.1),
                createOption(1, 16384, 0.1));
    }

    private static List<DependentResourceOption> createIO1IOPSOptions() {
        return ImmutableList.of(
                createOption(1000, 1000, 0.2),
                createOption(1, 64000, 0.2));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestDBSRatioBasedResize() {
        return new Object[][]{
                // GP2 test cases
                //storageAmountDemand, iopsDemand, minRatio, maxRatio, stAmtOptions, iopsOptions, expectedTotalPrice, expectedStorageAmount, expectedIops
                // Storage amount resized to meet IOPS demand
                {1000, 4000, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), 143.4, 1334d, 4002},
                // Nothing needs to be resized
                {1000, 2900, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), 110d, 1000d, 3000},
                // Nothing needs to be resized - 2
                {6000, 15000, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), 610, 6000, 15000},
                // IOPS causing storage amount to resize and Infinite quote
                {1000, 16001, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), Double.POSITIVE_INFINITY, 5334, 100},
                // IOPS causing Infinite quote
                {5334, 16001, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), Double.POSITIVE_INFINITY, 5334, 100},
                // IOPS causing Infinite quote
                {5334, 16001, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), Double.POSITIVE_INFINITY, 5334, 100},
                // Storage amount causing infinite quote
                {65537, 15000, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), Double.POSITIVE_INFINITY, 5, 15000},
                // Storage amount * maxRatio is less than iopsDemand. Do not resize storageAmount to meet iopsDemand
                {20, 100, null, 3d, createGP2StorageAmountOptions(), createGP2IOPSOptions(), 12, 20, 100},

                // IO1 test cases
                // No change. Make sure IOPS stays at 2000 and does not go to maxRatio * storage amount
                {100, 2000, 1d, 50d, createIO1StorageAmountOptions(), createIO1IOPSOptions(), 420, 100, 2000},
                // IOPS resized up to MinRatio * StorageAmount
                {1000, 200, 1d, 50d, createIO1StorageAmountOptions(), createIO1IOPSOptions(), 310, 1000, 1000},
                // Storage amount resized to meet IOPS demand
                {20, 2000, 1d, 50d, createIO1StorageAmountOptions(), createIO1IOPSOptions(), 414, 40, 2000},
                // IOPS causes infinite quote
                {1000, 65000, 1d, 50d, createIO1StorageAmountOptions(), createIO1IOPSOptions(), Double.POSITIVE_INFINITY, 1300, 1000},
                // Storage amount causes infinite quote
                {17000, 100, 1d, 50d, createIO1StorageAmountOptions(), createIO1IOPSOptions(), Double.POSITIVE_INFINITY, 20, 17000}
        };
    }
}
