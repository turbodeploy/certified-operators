package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple.DependentResourceOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
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
    private static ShoppingList linuxComputeSL;
    private static ShoppingList windowsComputeSL;
    public static final long DB_REGION_ID = 11L;
    public static final long DB_BUSINESS_ACCOUNT_ID = 33L;
    public static final int DB_LICENSE_COMMODITY_TYPE = 200;
    public static final int DB_STORAGE_AMOUNT_TYPE = 3;
    public static final double DB_S3_DTU_PRICE = 150.0;
    public static final double DELTA = 0.01;
    private static Trader databaseTier;
    private static ShoppingList databaseSL1;
    private static ShoppingList databaseSL2;
    private static ShoppingList databaseSL3;

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
        // DB tet setup
        CommoditySpecification dbLicense =
                new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, licenseAccessCommBaseType);
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
     * Test calculateCost for compute tier when the VM with no region no business account(on prem VM).
     * Expected: linux VM will get cheapest cost across all regions all business accounts
     * considering linux license. Windows VM will get cheapest cost across all regions all
     * business accounts considering windows license.
     */
    @Test
    public void testOnPremMCPComputeAndDatabaseCost() {
        CostFunction computeCostFunction = CostFunctionFactory
                .createCostFunctionForComputeTier(createComputeCost());
        MutableQuote linuxQuote = computeCostFunction.calculateCost(linuxComputeSL, computeTier, true, economy);
        MutableQuote windowsQuote = computeCostFunction.calculateCost(windowsComputeSL, computeTier, true, economy);
        assertEquals(5, linuxQuote.getQuoteValue(), 0.00000);
        assertEquals(50, windowsQuote.getQuoteValue(), 0.00000);
    }

    /**
     * Test retrieveCbtpCostTuple for VM with no region no business account(on prem VM).
     */
    @Test
    public void testOnPremMCPRetriveCbtpCostTuple() {
        CbtpCostDTO cbtpCostDTO = createCbtpCost();
        CostTable costTable = new CostTable(cbtpCostDTO.getCostTupleListList());
        CostTuple tuple = CostFunctionFactory.retrieveCbtpCostTuple(null, cbtpCostDTO, costTable,
                TestUtils.WINDOWS_COMM_TYPE);
        assertEquals(50, tuple.getPrice(), 0.00000);
        assertEquals(TestUtils.WINDOWS_COMM_TYPE, tuple.getLicenseCommodityType());

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

    private DatabaseTierCostDTO createDBCostDTO() {
        DependentCostTuple dependentCostTuple = DependentCostTuple.newBuilder()
                .setDependentResourceType(DB_STORAGE_AMOUNT_TYPE)
                .addDependentResourceOptions(DependentResourceOption.newBuilder()
                        .setIncrement(250)
                        .setEndRange(250)
                        .setPrice(0.0)
                        .build())
                .addDependentResourceOptions(DependentResourceOption.newBuilder()
                        .setIncrement(50)
                        .setEndRange(300)
                        .setPrice(0.2)
                        .build())
                .addDependentResourceOptions(DependentResourceOption.newBuilder()
                        .setIncrement(100)
                        .setEndRange(500)
                        .setPrice(0.2)
                        .build())
                .addDependentResourceOptions(DependentResourceOption.newBuilder()
                        .setIncrement(250)
                        .setEndRange(750)
                        .setPrice(0.2)
                        .build())
                .addDependentResourceOptions(DependentResourceOption.newBuilder()
                        .setIncrement(274)
                        .setEndRange(1024)
                        .setPrice(0.2)
                        .build())
                .build();
        CostTuple costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .setRegionId(DB_REGION_ID)
                .setLicenseCommodityType(licenseAccessCommBaseType)
                .setPrice(DB_S3_DTU_PRICE)
                .addDependentCostTuples(dependentCostTuple)
                .setBusinessAccountId(DB_BUSINESS_ACCOUNT_ID)
                .build();
        return DatabaseTierCostDTO.newBuilder()
                .addCostTupleList(costTuple)
                .setCouponBaseType(couponBaseType)
                .build();
    }

}
