package com.vmturbo.platform.analysis.utilities;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseServerTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.RangeTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.CapacityLimitation;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
import com.vmturbo.platform.analysis.utilities.Quote.CostUnavailableQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

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
    private static final long accountId1 = 101L;
    private static final long accountId2 = 102L;
    private static final long regionId11 = 211L;
    private static final long regionId12 = 212L;
    private static final long regionId21 = 221L;
    private static final long zoneId213 = 321L;
    public static final long DB_REGION_ID = 11L;
    public static final long DB_BUSINESS_ACCOUNT_ID = 33L;
    public static final long IOPS_MIN = 100L;
    public static final int DB_LICENSE_COMMODITY_TYPE = 200;
    public static final int DB_STORAGE_AMOUNT_TYPE = TestUtils.ST_AMT.getType();
    public static final double DB_S3_DTU_PRICE = 150.0;
    public static final double DELTA = 0.01;
    private static final double STORAGE_COST_PER_GB = 0.2;
    private static final double IOPS_COST = 0.1;
    private static final double DBS_TIER_COST = 10;
    private static final long DBS_STORAGE_AMOUNT = 1024;
    private static final long DBS_IOPS_AMOUNT = 1000;
    private static CommoditySpecification dbLicense;
    private static Trader databaseTier;
    private static ShoppingList databaseSL1;
    private static ShoppingList databaseSL2;
    private static ShoppingList databaseSL3;
    private static Trader databaseServerTier;
    private static ShoppingList databaseServerSL;
    private static ShoppingList databaseServerSL0;

    @BeforeClass
    public static void setUp() {
        List<CommoditySpecification> computeCommList = Arrays.asList(TestUtils.CPU,
                new CommoditySpecification(TestUtils.LINUX_COMM_TYPE, licenseAccessCommBaseType),
                new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, licenseAccessCommBaseType));
        Trader linuxVM = TestUtils.createVM(economy);
        computeTier = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                computeCommList, new double[]{10000, 10000, 10000}, true, false);
        linuxComputeSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU,
                new CommoditySpecification(TestUtils.LINUX_COMM_TYPE, licenseAccessCommBaseType)),
                linuxVM, new double[]{50, 1}, new double[]{90, 1}, computeTier);
        linuxComputeSL.setGroupFactor(1);
        Trader windowsVM = TestUtils.createVM(economy);
        windowsComputeSL = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE,
                        licenseAccessCommBaseType)), windowsVM, new double[]{50, 1},
                new double[]{90, 1}, computeTier);
        windowsComputeSL.setGroupFactor(1);
        // DB test setup
        dbLicense = new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, licenseAccessCommBaseType);
        List<CommoditySpecification> dbCommList =
                Arrays.asList(TestUtils.DTU, TestUtils.ST_AMT, dbLicense);
        Trader testVM = TestUtils.createVM(economy);
        databaseTier = TestUtils.createTrader(economy, TestUtils.DB_TIER_TYPE, Arrays.asList(0L),
                dbCommList, new double[]{100, 1024, 10000}, false, false);
        databaseSL1 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.DTU, TestUtils.ST_AMT, dbLicense), testVM,
                new double[]{77, 730, 0}, new double[]{77, 730, 0}, databaseTier);
        databaseSL1.setGroupFactor(1);
        databaseSL2 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.DTU, TestUtils.ST_AMT, dbLicense), testVM,
                new double[]{77, 230, 0}, new double[]{77, 230, 0}, databaseTier);
        databaseSL2.setGroupFactor(1);
        databaseSL3 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.DTU, TestUtils.ST_AMT, dbLicense), testVM,
                new double[]{77, 1024, 0}, new double[]{77, 1024, 0}, databaseTier);
        databaseSL3.setGroupFactor(1);

        List<CommoditySpecification> dbsCommList = Arrays.asList(TestUtils.IOPS, TestUtils.ST_AMT,
                dbLicense);
        databaseServerTier = TestUtils.createTrader(economy, TestUtils.DB_TIER_TYPE,
                Arrays.asList(0L), dbsCommList, new double[]{8000, 2048, 10000}, false, false);
        databaseServerSL = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.IOPS, TestUtils.ST_AMT, dbLicense), testVM,
                new double[]{DBS_IOPS_AMOUNT, DBS_STORAGE_AMOUNT, 0},
                new double[]{DBS_IOPS_AMOUNT, DBS_STORAGE_AMOUNT, 0}, databaseServerTier);
        databaseServerSL.setGroupFactor(1);

        databaseServerSL0 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.IOPS, TestUtils.ST_AMT, dbLicense), testVM,
                new double[]{0, DBS_STORAGE_AMOUNT, 0},
                new double[]{0, DBS_STORAGE_AMOUNT, 0}, databaseServerTier);
        databaseServerSL0.setGroupFactor(1);

        List<CommoditySpecification> stCommList = Arrays.asList(TestUtils.ST_AMT);
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000}, true, false);
        stSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.ST_AMT),
                linuxVM, new double[] {500}, new double[] {500}, storageTier);
    }

    /**
     * Test CostFunctionFactory.calculateComputeAndDatabaseCostQuote with buyer asking for linux
     * license template without context and with context which has region id being regionId11,
     * business account id being accountId1.
     */
    @Test
    public void testCalculateComputeCost() {
        final Trader buyerVm = TestUtils.createVM(economy);
        final ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, new CommoditySpecification(TestUtils.LINUX_COMM_TYPE,
                        licenseAccessCommBaseType)), buyerVm, new double[] {500, 1},
                new double[] {500, 1}, computeTier);
        CostTable costTable = new CostTable(createComputeCost().getCostTupleListList());
        MutableQuote quote1 = CostFunctionFactory.calculateComputeAndDatabaseCostQuote(computeTier,
                shoppingList, costTable, licenseAccessCommBaseType);
        // 1. test without context computation
        assertTrue(quote1 instanceof CostUnavailableQuote);
        assertTrue(quote1.getSeller().equals(computeTier) && Double.isInfinite(quote1.getQuoteValue()));
        // 2. test with context computation
        BalanceAccount account1 = new BalanceAccount(100, 10000, accountId1, 0);
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId11, 0, account1));
        MutableQuote quote2 = CostFunctionFactory.calculateComputeAndDatabaseCostQuote(computeTier,
                shoppingList, costTable, licenseAccessCommBaseType);
        assertNotNull(quote2);
        EconomyDTOs.Context context = quote2.getContext().get();
        assertEquals(regionId11, context.getRegionId());
        assertEquals(accountId1, context.getBalanceAccount().getId());
        assertEquals(5, quote2.getQuoteValue(), 0d);
    }

    /**
     * Helper method to create compute cost tuples.
     */
    private List<CostTuple> generateComputeCostTuples() {
        List<CostTuple> computeTuples = new ArrayList<>();
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(accountId1)
                .setRegionId(regionId11).setLicenseCommodityType(TestUtils.LINUX_COMM_TYPE)
                .setPrice(5).build());
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(accountId2)
                .setRegionId(regionId21).setLicenseCommodityType(TestUtils.WINDOWS_COMM_TYPE)
                .setPrice(100).build());
        computeTuples.add(CostTuple.newBuilder().setBusinessAccountId(accountId2)
                .setZoneId(zoneId213).setLicenseCommodityType(TestUtils.WINDOWS_COMM_TYPE)
                .setPrice(50).build());
        return computeTuples;
    }

    /**
     * Helper method to create compute cost DTO.
     */
    private ComputeTierCostDTO createComputeCost() {
        return ComputeTierCostDTO.newBuilder().addAllCostTupleList(generateComputeCostTuples())
                .setCouponBaseType(couponBaseType)
                .setLicenseCommodityBaseType(licenseAccessCommBaseType).build();
    }

    /**
     * Tests storage tier quote lookup for with and without context cases.
     */
    @Test
    public void calculateStorageTierCost() {
        // Price ranges for region1 in account1:
        // Price for 500 GB storage (SL requested amount) in region1 in account1 should fall at priceUpTo600Gb,
        // which is $20
        StorageTierPriceData priceUpTo200Gb = StorageTierPriceData.newBuilder()
                .setUpperBound(200D)
                .addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountId1)
                        .setRegionId(regionId11)
                        .setPrice(10D)
                        .build())
                .setIsUnitPrice(false)
                .setIsAccumulativeCost(false)
                .setAppliedToHistoricalQuantity(false)
                .build();
        StorageTierPriceData priceUpTo600Gb = StorageTierPriceData.newBuilder()
                .setUpperBound(600D)
                .addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountId1)
                        .setRegionId(regionId11)
                        .setPrice(20D)
                        .build())
                .setIsUnitPrice(false)
                .setIsAccumulativeCost(false)
                .setAppliedToHistoricalQuantity(false)
                .build();

        // Price for 500 GB storage (SL requested amount) in region2 in account2 should be:
        // 500 x $10 (unit price) = $5000
        StorageTierPriceData priceUnbounded = StorageTierPriceData.newBuilder()
                .setUpperBound(Double.POSITIVE_INFINITY)
                .addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountId2)
                        .setRegionId(regionId21)
                        .setPrice(10D)
                        .build())
                .setIsUnitPrice(true)
                .setIsAccumulativeCost(false)
                .setAppliedToHistoricalQuantity(false)
                .build();

        // Add a bad bound, we are requesting 500, but there is only 1 bound in this region,
        // set to 300. So we should get back an infinite quote for this case.
        StorageTierPriceData priceBadBound = StorageTierPriceData.newBuilder()
                .setUpperBound(300D)
                .addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountId2)
                        .setRegionId(regionId12)
                        .setPrice(30D)
                        .build())
                .setIsUnitPrice(false)
                .setIsAccumulativeCost(true)
                .setAppliedToHistoricalQuantity(false)
                .build();

        // Add prices for both accounts to map.
        StorageResourceCost resourceCost = StorageResourceCost.newBuilder()
                .setResourceType(CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(1)
                        .build())
                .addStorageTierPriceData(priceUpTo200Gb)
                .addStorageTierPriceData(priceUpTo600Gb)
                .addStorageTierPriceData(priceUnbounded)
                .addStorageTierPriceData(priceBadBound)
                .build();
        AccountRegionPriceTable priceData = new AccountRegionPriceTable(resourceCost);

        // Setup commodity spec mapping.
        CommoditySpecification commSpec1 = TestUtils.ST_AMT;
        Map<CommoditySpecification, AccountRegionPriceTable> priceDataMap = new HashMap<>();
        priceDataMap.put(commSpec1, priceData);

        Map<CommoditySpecification, CapacityLimitation> commCapacity = new HashMap<>();
        commCapacity.put(commSpec1, new CapacityLimitation(0d, 500d, true));

        final Trader buyerVm = TestUtils.createVM(economy);
        final ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Collections.singletonList(TestUtils.ST_AMT), buyerVm, new double[] {500},
                new double[] {500}, storageTier);
        shoppingList.move(buyerVm);
        final Map<CommoditySpecification, Double> commQuantityMap = new HashMap<>();
        commQuantityMap.put(TestUtils.ST_AMT, 500d);

        // 1. Set the context to region 1 (higher price), verify we are getting that quote,
        // as we specifically asked for the account/region.
        BalanceAccount account1 = new BalanceAccount(100, 10000, accountId1, 0);
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId11, 0, account1));
        MutableQuote quote1 = CostFunctionFactoryHelper.calculateStorageTierQuote(shoppingList,
                storageTier, commQuantityMap, Collections.emptyMap(), priceDataMap, commCapacity,
                new ArrayList<>(), new ArrayList<>(), true, false);
        assertNotNull(quote1);
        assertTrue(quote1.getContext().isPresent());
        EconomyDTOs.Context context1 = quote1.getContext().get();
        assertEquals(regionId11, context1.getRegionId());
        assertEquals(accountId1, context1.getBalanceAccount().getId());
        assertEquals(20d, quote1.getQuoteValue(), 0d);

        // 2. Bad bounds check for region12, should get infinite quote back.
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId12, 0, account1));
        MutableQuote quote2 = CostFunctionFactoryHelper.calculateStorageTierQuote(shoppingList,
                storageTier, commQuantityMap, Collections.emptyMap(), priceDataMap, commCapacity,
                new ArrayList<>(), new ArrayList<>(), true, false);
        assertNotNull(quote2);
        assertFalse(quote2.getContext().isPresent());
        assertEquals(Double.POSITIVE_INFINITY, quote2.getQuoteValue(), 0d);

        // 3. Set null context for buyer
        buyerVm.getSettings().setContext(null);
        MutableQuote quote3 = CostFunctionFactoryHelper.calculateStorageTierQuote(shoppingList,
                storageTier, commQuantityMap, Collections.emptyMap(), priceDataMap, commCapacity,
                new ArrayList<>(), new ArrayList<>(), true, false);
        assertTrue(quote3 instanceof CostUnavailableQuote);
        assertTrue(quote3.getSeller().equals(storageTier) && Double.isInfinite(quote3.getQuoteValue()));
    }

    /**
     * Tests storage tier quote calculation when price is based on historical used.
     */
    @Test
    public void calculateHistoricalBasedStorageTierCost() {
        final double price = 10D;
        final StorageResourceCost resourceCost = StorageResourceCost.newBuilder()
                .setResourceType(CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(1)
                        .build())
                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                        .setUpperBound(1000D)
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(accountId1)
                                .setRegionId(regionId11)
                                .setPrice(price)
                                .build())
                        .setIsUnitPrice(true)
                        .setIsAccumulativeCost(false)
                        .setAppliedToHistoricalQuantity(true)
                        .build())
                .build();
        final AccountRegionPriceTable priceTable = new AccountRegionPriceTable(resourceCost);

        final CommoditySpecification commSpec = TestUtils.ST_AMT;
        final Map<CommoditySpecification, AccountRegionPriceTable> priceDataMap =
                ImmutableMap.of(commSpec, priceTable);

        final Map<CommoditySpecification, CapacityLimitation> commCapacity =
                ImmutableMap.of(commSpec, new CapacityLimitation(0D, 500D, true));

        final Trader buyerVm = TestUtils.createVM(economy);
        final ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Collections.singletonList(commSpec), buyerVm, new double[] {300D},
                new double[] {300D}, storageTier);
        final Map<CommoditySpecification, Double> commQuantityMap =
                ImmutableMap.of(commSpec, 300D);
        final double historicalQuantity = 400D;
        final Map<CommoditySpecification, Double> commHistoricalQuantityMap =
                ImmutableMap.of(commSpec, historicalQuantity);

        final BalanceAccount account = new BalanceAccount(100, 10000, accountId1, 0);
        buyerVm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId11, 0, account));

        final MutableQuote quote = CostFunctionFactoryHelper.calculateStorageTierQuote(
                shoppingList, storageTier, commQuantityMap, commHistoricalQuantityMap,
                priceDataMap, commCapacity, Collections.emptyList(), Collections.emptyList(),
                true, false);
        assertNotNull(quote);
        assertEquals(historicalQuantity * price, quote.getQuoteValue(), 0d);
    }

    /**
     * Test that cost calculation with ratio dependency rounds up the max quantity calculation
     * for dependent commodity.
     */
    @Test
    public void testCostCalculationWithRatioDependency() {
        final double st1IopsRatio = 40.0 / 1024;
        final long accountId1 = 101;
        final long regionId11 = 211;
        BalanceAccount account1 = new BalanceAccount(100, 10000, accountId1, 0);
        List<CommoditySpecification> stCommList = Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS);
        Trader vm = TestUtils.createVM(economy);
        vm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
            regionId11, 0, account1));
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
            stCommList, new double[] {10000, 10000}, true, false);
        final CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
            CostDTO.newBuilder()
                .setStorageTierCost(StorageTierCostDTO.newBuilder()
                    .addStorageResourceRatioDependency(StorageResourceRatioDependency.newBuilder()
                        .setBaseResourceType(TestUtils.stAmtTO)
                        .setDependentResourceType(TestUtils.iopsTO)
                        .setMaxRatio(st1IopsRatio)
                        .build())
                    .addStorageResourceCost(StorageResourceCost.newBuilder()
                        .setResourceType(TestUtils.stAmtTO)
                        .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                            .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountId1)
                                .setRegionId(regionId11).setPrice(30.0).build())
                            .setIsAccumulativeCost(false).setIsUnitPrice(false)
                            .setUpperBound(100))
                        .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                            .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountId1)
                                .setRegionId(regionId11).setPrice(40.0).build())
                            .setIsAccumulativeCost(true).setIsUnitPrice(false)
                            .setUpperBound(200)))
                    .build())
                .build()
        );
        final double stAmountQuantity = 3000;
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
            new double[] {stAmountQuantity, 118}, new double[] {80, 300}, storageTier);
        stSL.setDemandScalable(true);
        MutableQuote quote = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        final Optional<CommodityContext> stAmountComContext = quote.getCommodityContexts().stream()
            .filter(c -> c.getCommoditySpecification().getType() == TestUtils.stAmtTO.getBaseType())
            .findAny();
        Assert.assertTrue(stAmountComContext.isPresent());
        // No change in decisive commodity
        Assert.assertEquals(stAmountQuantity, stAmountComContext.get().getNewCapacityOnSeller(), 0);
    }

    /**
     * Tests generating a quote for a DB Server with NO dependent options.
     */
    @Test
    public void testDbServerWithoutDependentOptionsQuoteGeneration() {

        final double cost = 100.0;

        CostTuple costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(TestUtils.WINDOWS_COMM_TYPE)
                .setPrice(cost)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .build();
        DatabaseServerTierCostDTO serverTierCostDTO = DatabaseServerTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .setLicenseCommodityBaseType(licenseAccessCommBaseType)
                .build();

        final CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setDatabaseServerTierCost(serverTierCostDTO).build());

        CommoditySpecification dbLicense = new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE,
                licenseAccessCommBaseType);

        List<CommoditySpecification> commList = Arrays.asList(TestUtils.VCPU, TestUtils.VMEM,
                dbLicense);

        Trader vm = TestUtils.createVM(economy);

        Trader tier = TestUtils.createTrader(economy, TestUtils.DBS_TYPE, Arrays.asList(0L),
                commList, new double[]{10000, 10000, 1}, false, false);
        ShoppingList sl = TestUtils.createAndPlaceShoppingList(economy, commList, vm,
                new double[]{20, 118, 1}, new double[]{80, 300, 1}, tier);

        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        sl.getBuyer().getSettings().setContext(context);

        MutableQuote quote = storageCostFunction.calculateCost(sl, tier, true, economy);
        Assert.assertEquals(cost, quote.quoteValues[0], DELTA);
    }

    /**
     * Test case for cost calculation with range dependency.
     */
    @Test
    public void testCostCalculationWithRangeDependency() {
        final long accountId1 = 101;
        final long regionId11 = 211;
        BalanceAccount account1 = new BalanceAccount(100, 10000, accountId1, 0);
        List<CommoditySpecification> stCommList = Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS);
        Trader vm = TestUtils.createVM(economy);
        vm.getSettings().setContext(new com.vmturbo.platform.analysis.economy.Context(
                regionId11, 0, account1));
        storageTier = TestUtils.createTrader(economy, TestUtils.ST_TYPE, Arrays.asList(0L),
                stCommList, new double[] {10000, 10000}, true, false);

        CostFunction storageCostFunction = CostFunctionFactory.createCostFunction(
                CostDTO.newBuilder().setStorageTierCost(StorageTierCostDTO.newBuilder()
                        .addStorageResourceRangeDependency(StorageResourceRangeDependency.newBuilder()
                                .setBaseResourceType(TestUtils.stAmtTO).setDependentResourceType(TestUtils.iopsTO)
                                .addRangeTuple(RangeTuple.newBuilder().setBaseMaxCapacity(100).setDependentMaxCapacity(200))
                                .addRangeTuple(RangeTuple.newBuilder().setBaseMaxCapacity(200).setDependentMaxCapacity(400)))
                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                .setResourceType(TestUtils.stAmtTO)
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountId1)
                                                .setRegionId(regionId11).setPrice(30.0).build())
                                        .setIsAccumulativeCost(false).setIsUnitPrice(false)
                                        .setUpperBound(100))
                                .addStorageTierPriceData(StorageTierPriceData.newBuilder()
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountId1)
                                                .setRegionId(regionId11).setPrice(40.0).build())
                                        .setIsAccumulativeCost(true).setIsUnitPrice(false)
                                        .setUpperBound(200))).build()).build());

        // IOPS exceeds maximum capacity supported by the tier. Expects infinite quote.
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {150, 500}, new double[] {150, 500}, storageTier);
        stSL.setDemandScalable(true);
        MutableQuote quote1 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(Double.POSITIVE_INFINITY, quote1.getQuoteValue(), 0.000);

        // IOPS = 300 (200-400 range), storage amount is 80GB(0-100 range). Increase storage amount to 200. Cost is based on 200GB.
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {80, 300}, new double[] {80, 300}, storageTier);
        stSL.setDemandScalable(true);
        MutableQuote quote2 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(40, quote2.getQuoteValue(), 0.000);

        // IOPS = 150(0-100 range), storage amount is 150GB (in 100-200 range). Cost is based on 200GB.
        stSL = TestUtils.createAndPlaceShoppingList(economy, stCommList, vm,
                new double[] {150, 150}, new double[] {150, 150}, storageTier);
        stSL.setDemandScalable(true);
        MutableQuote quote3 = storageCostFunction.calculateCost(stSL, storageTier, true, economy);
        assertEquals(40, quote3.getQuoteValue(), 0.000);
    }

    /**
     * Tests generating a quote for a DB with dependent options. The demand is DTU: 77, Storage
     * Amount: SL1:730GB SL2:250GB SL3:1024GB.
     */
    @Test
    public void testCalculateComputeAndDatabaseCostQuote() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        databaseSL1.getBuyer().getSettings().setContext(context);
        DatabaseTierCostDTO dbCostDTO = createDBCostDTO();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTO.getCostTupleList(0));
        // SL demand: DTU: 77 Storage Amount: 730GB
        MutableQuote quote1 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, databaseSL1,
                        costTable, licenseAccessCommBaseType);
        Assert.assertEquals(250.0, quote1.quoteValues[0], DELTA);
        CommodityContext commodityContext1 = quote1.getCommodityContexts().get(0);
        Assert.assertEquals(DB_STORAGE_AMOUNT_TYPE,
                commodityContext1.getCommoditySpecification().getType());
        Assert.assertEquals(750.0, commodityContext1.getNewCapacityOnSeller(), DELTA);
        // SL demand: DTU: 77 Storage Amount: 230GB
        MutableQuote quote2 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, databaseSL2,
                        costTable, licenseAccessCommBaseType);
        Assert.assertEquals(150.0, quote2.quoteValues[0], DELTA);
        CommodityContext commodityContext2 = quote2.getCommodityContexts().get(0);
        Assert.assertEquals(DB_STORAGE_AMOUNT_TYPE,
                commodityContext2.getCommoditySpecification().getType());
        Assert.assertEquals(250.0, commodityContext2.getNewCapacityOnSeller(), DELTA);
        // SL demand: DTU: 77 Storage Amount: 1024GB
        MutableQuote quote3 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, databaseSL3,
                        costTable, licenseAccessCommBaseType);
        Assert.assertEquals(304.8, quote3.quoteValues[0], DELTA);
        CommodityContext commodityContext3 = quote3.getCommodityContexts().get(0);
        Assert.assertEquals(DB_STORAGE_AMOUNT_TYPE,
                commodityContext3.getCommoditySpecification().getType());
        Assert.assertEquals(1024.0, commodityContext3.getNewCapacityOnSeller(), DELTA);
    }

    /**
     * Tests generating a quote for a DB Server with dependent options for StorageAmount and IOPS.
     */
    @Test
    public void testCalculateComputeAndDatabaseServerCostQuote() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        databaseServerSL.getBuyer().getSettings().setContext(context);
        DatabaseServerTierCostDTO dbCostDTO = createDBSCostDTO();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTO.getCostTupleList(0));
        MutableQuote quote1 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseServerTier, databaseServerSL,
                        costTable, licenseAccessCommBaseType);
        //check total price
        final double expectedPrice = DBS_TIER_COST + STORAGE_COST_PER_GB * DBS_STORAGE_AMOUNT + IOPS_COST * DBS_IOPS_AMOUNT;
        Assert.assertEquals(expectedPrice, quote1.quoteValues[0], DELTA);
        //storage amount
        CommodityContext commodityContext1 = quote1.getCommodityContexts().get(0);
        Assert.assertEquals(TestUtils.ST_AMT.getType(),
                commodityContext1.getCommoditySpecification().getType());
        Assert.assertEquals(DBS_STORAGE_AMOUNT, commodityContext1.getNewCapacityOnSeller(), DELTA);
        //iops amount
        CommodityContext commodityContext2 = quote1.getCommodityContexts().get(1);
        Assert.assertEquals(TestUtils.IOPS.getType(),
                commodityContext2.getCommoditySpecification().getType());
        Assert.assertEquals(DBS_IOPS_AMOUNT, commodityContext2.getNewCapacityOnSeller(), DELTA);

    }

    /**
     * Check that new capacity respects min limits defined by dependent cost tuple is commodity used
     * is 0.
     */
    @Test
    public void testDBServerStorageAccessNewCapacityWithZeroUsed() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        databaseServerSL0.getBuyer().getSettings().setContext(context);
        DatabaseServerTierCostDTO dbCostDTO = createDBSCostDTO();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTO.getCostTupleList(0));
        MutableQuote quote1 = CostFunctionFactory.calculateComputeAndDatabaseCostQuote(
                databaseServerTier, databaseServerSL0, costTable, licenseAccessCommBaseType);
        Optional<CommodityContext> storageAccessCC = quote1.getCommodityContexts().stream().filter(
                v -> v.getCommoditySpecification().getType() == TestUtils.IOPS.getType()).findAny();
        Assert.assertTrue(storageAccessCC.isPresent());
        //new capacity should be set to min IOPS defined in dependent cost tuple.
        Assert.assertEquals(IOPS_MIN, storageAccessCC.get().getNewCapacityOnSeller(), DELTA);
    }

    /**
     * Tests generating a quote for a DB Server with dependent options for StorageAmount and IOPS.
     */
    @Test
    public void testCalculateComputeAndDatabaseServerWithNoIncrementStorageCost() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        databaseServerSL.getBuyer().getSettings().setContext(context);
        DatabaseServerTierCostDTO dbCostDTO = createDBSCostDTOWithNoIncrement();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTO.getCostTupleList(0));
        MutableQuote quote1 =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseServerTier, databaseServerSL,
                        costTable, licenseAccessCommBaseType);
        //check total price
        final double expectedPrice = DBS_TIER_COST + STORAGE_COST_PER_GB * DBS_STORAGE_AMOUNT;
        Assert.assertEquals(expectedPrice, quote1.quoteValues[0], DELTA);
        //storage amount
        CommodityContext commodityContext1 = quote1.getCommodityContexts().get(0);
        Assert.assertEquals(TestUtils.ST_AMT.getType(),
                commodityContext1.getCommoditySpecification().getType());
        Assert.assertEquals(DBS_STORAGE_AMOUNT, commodityContext1.getNewCapacityOnSeller(), DELTA);
    }

    /**
     * Tests generating a quote for a DB with dependent options. The demand is DTU: 77, Storage
     * Amount: SL1:730GB.
     */
    @Test
    public void testCalculateComputeAndDatabaseWithInfiniteCostQuote() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        databaseSL1.getBuyer().getSettings().setContext(context);
        DatabaseTierCostDTO dbCostDTOForBasicFamily = createDBCostDTOWithBasicFamily();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTOForBasicFamily.getCostTupleList(0));
        // SL demand: DTU: 77 Storage Amount: 730GB: This will not be met by dbCostDTOForBasicFamily.
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, databaseSL1,
                        costTable, licenseAccessCommBaseType);
        Assert.assertEquals(POSITIVE_INFINITY, mutableQuote.quoteValues[0], DELTA);
    }

    /**
     * Scenario where seller can support scale up and chooses until the price is 0. ie: 250GB.
     * DB assigned capacity = 10
     * DB Resized capacity = 200
     * DB recommended size = 250
     * DB Storage cost = 0;
     */
    @Test
    public void testCalculateComputeAndDatabaseWithScaleUpToFreeStorage() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[]{200, 0}, new double[]{200, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 10D);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        // because no extra cost till 250 GB.
        Assert.assertEquals(250.0, mutableQuote.getCommodityContexts().get(0).getNewCapacityOnSeller(), DELTA);
        Assert.assertEquals(DB_S3_DTU_PRICE, mutableQuote.quoteValues[0], DELTA);
    }

    /**
     * Scenario where seller can support scale up and chooses something which meets resized capacity of 300GB.
     * DB assigned capacity = 10
     * DB Resized capacity = 280
     * DB recommended size = 300
     * DB Storage cost = 10;
     */
    @Test
    public void testCalculateComputeAndDatabaseWithScaleUp() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[] {280, 0}, new double[] {280, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 10D);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        // because no extra cost till 250 GB.
        Assert.assertEquals(300, mutableQuote.getCommodityContexts().get(0).getNewCapacityOnSeller(), DELTA);
        Assert.assertEquals(DB_S3_DTU_PRICE + 50 * STORAGE_COST_PER_GB, mutableQuote.quoteValues[0], DELTA);
    }

    /**
     * Scenario where DB scale down can be supported by current seller.
     * DB assigned capacity = 1024
     * DB Resized capacity = 10
     * DB recommended size = 250
     * DB Storage cost = 0;
     */
    @Test
    public void testCalculateComputeAndDatabaseSupporteSellerScaleDown() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[] {10, 0}, new double[] {10, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 1024);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        // because no extra cost till 250 GB.
        Assert.assertEquals(250, mutableQuote.getCommodityContexts().get(0).getNewCapacityOnSeller(), DELTA);
        Assert.assertEquals(DB_S3_DTU_PRICE, mutableQuote.quoteValues[0], DELTA);
    }

    /**
     * Scenario where the shoppingList can not support assignedCapacity.
     * DB assigned capacity = 4096
     * DB Resized capacity = 10
     * DB recommended size = 250
     * DB Storage cost = 0;
     */
    @Test
    public void testCalculateComputeAndDatabaseUnsupportedSLScaleUp() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[] {10, 0}, new double[] {10, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 4096);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        // because no extra cost till 250 GB.
        Assert.assertEquals(250, mutableQuote.getCommodityContexts().get(0).getNewCapacityOnSeller(), DELTA);
        Assert.assertEquals(DB_S3_DTU_PRICE, mutableQuote.quoteValues[0], DELTA);
    }


    /**
     * Scenario where the seller and SL is not on same supplier; so DB goes to 250GB storage.
     * DB assigned capacity = 30
     * DB Resized capacity = 10
     * DB recommended size = 250
     * DB Storage cost = 0;
     */
    @Test
    public void testCalculateComputeAndDatabaseDifferentSLAndSeller() {
        Context context = Mockito.mock(Context.class);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[] {10, 0}, new double[] {10, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        final Trader buyerDB = TestUtils.createDB(economy);
        shoppingList.move(buyerDB);
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 30);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        // because no extra cost till 250 GB.
        Assert.assertEquals(250, mutableQuote.getCommodityContexts().get(0).getNewCapacityOnSeller(), DELTA);
        Assert.assertEquals(DB_S3_DTU_PRICE, mutableQuote.quoteValues[0], DELTA);
    }

    /**
     * Scenario where seller can not support Resized capacity. Leads to positive infinity cost.
     * DB assigned capacity = 10
     * DB Resized capacity = 4096
     * DB recommended size = ~
     * DB Storage cost = POSITIVE.INFINITY;
     */
    @Test
    public void testCalculateComputeAndDatabaseInfiniteCost() {
        BalanceAccount balanceAccount = Mockito.mock(BalanceAccount.class);
        Context context = Mockito.mock(Context.class);
        CostTable costTable = getDBCostTable(balanceAccount, context);
        ShoppingList shoppingList = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, dbLicense), TestUtils.createVM(economy),
                new double[] {4096, 0}, new double[] {4096, 0}, databaseTier);
        shoppingList.getBuyer().getSettings().setContext(context);
        shoppingList.addAssignedCapacity(DB_STORAGE_AMOUNT_TYPE, 10);
        MutableQuote mutableQuote =
                CostFunctionFactory.calculateComputeAndDatabaseCostQuote(databaseTier, shoppingList,
                        costTable, licenseAccessCommBaseType);
        Assert.assertEquals(POSITIVE_INFINITY, mutableQuote.quoteValues[0], DELTA);
    }

    @Nonnull
    private CostTable getDBCostTable(final BalanceAccount balanceAccount, final Context context) {
        when(context.getRegionId()).thenReturn(DB_REGION_ID);
        when(context.getBalanceAccount()).thenReturn(balanceAccount);
        when(balanceAccount.getPriceId()).thenReturn(DB_BUSINESS_ACCOUNT_ID);
        DatabaseTierCostDTO dbCostDTO = createDBCostDTO();
        CostTable costTable = Mockito.mock(CostTable.class);
        when(costTable.hasAccountId(DB_BUSINESS_ACCOUNT_ID)).thenReturn(true);
        when(costTable.getTuple(DB_REGION_ID, DB_BUSINESS_ACCOUNT_ID,
                DB_LICENSE_COMMODITY_TYPE)).thenReturn(dbCostDTO.getCostTupleList(0));
        return costTable;
    }

    private DatabaseTierCostDTO createDBCostDTO() {
        DependentCostTuple dependentCostTuple = DependentCostTuple.newBuilder()
                .setDependentResourceType(DB_STORAGE_AMOUNT_TYPE)
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(250)
                        .setEndRange(250)
                        .setPrice(0.0)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(50)
                        .setEndRange(300)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(100)
                        .setEndRange(500)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(250)
                        .setEndRange(750)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(274)
                        .setEndRange(1024)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .build();
        return createDatabaseTierCostDTO(dependentCostTuple);
    }

    private DatabaseTierCostDTO createDBCostDTOWithBasicFamily() {
        DependentCostTuple dependentCostTuple = DependentCostTuple.newBuilder()
                .setDependentResourceType(DB_STORAGE_AMOUNT_TYPE)
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(2)
                        .setEndRange(2)
                        .setPrice(1.0)
                        .build())
                .build();
        return createDatabaseTierCostDTO(dependentCostTuple);
    }

    private static DatabaseTierCostDTO createDatabaseTierCostDTO(final DependentCostTuple dependentCostTuple) {
        CostTuple costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(licenseAccessCommBaseType)
                .setPrice(DB_S3_DTU_PRICE).addDependentCostTuples(dependentCostTuple)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .build();
        return DatabaseTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .build();
    }

    private DatabaseServerTierCostDTO createDBSCostDTO() {
        DependentCostTuple dependentCostTuple1 = DependentCostTuple.newBuilder()
                .setDependentResourceType(TestUtils.ST_AMT.getType())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(20)
                        .setEndRange(20)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(1)
                        .setEndRange(16384)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .build();
        DependentCostTuple dependentCostTuple2 = DependentCostTuple.newBuilder()
                .setDependentResourceType(TestUtils.IOPS.getType())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(IOPS_MIN)
                        .setEndRange(IOPS_MIN)
                        .setPrice(IOPS_COST)
                        .build())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setAbsoluteIncrement(1)
                        .setEndRange(8000)
                        .setPrice(IOPS_COST)
                        .build())
                .build();

        CostTuple costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(licenseAccessCommBaseType)
                .setPrice(DBS_TIER_COST)
                .addDependentCostTuples(dependentCostTuple1)
                .addDependentCostTuples(dependentCostTuple2)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .build();
        return DatabaseServerTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .build();
    }


    private DatabaseServerTierCostDTO createDBSCostDTOWithNoIncrement() {
        DependentCostTuple dependentCostTuple1 = DependentCostTuple.newBuilder()
                .setDependentResourceType(TestUtils.ST_AMT.getType())
                .addDependentResourceOptions(DependentCostTuple.DependentResourceOption.newBuilder()
                        .setEndRange(2048)
                        .setPrice(STORAGE_COST_PER_GB)
                        .build())
                .build();
        CostTuple costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(licenseAccessCommBaseType)
                .setPrice(DBS_TIER_COST)
                .addDependentCostTuples(dependentCostTuple1)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .build();
        return DatabaseServerTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .build();
    }
}
