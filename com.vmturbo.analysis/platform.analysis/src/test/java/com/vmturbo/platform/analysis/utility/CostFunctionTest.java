package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper;
import com.vmturbo.platform.analysis.utilities.CostTable;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityCloudQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InitialInfiniteQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

public class CostFunctionTest {

    private static final long zoneId = 0L;
    private static final long regionId = 10L;

    private static final Logger logger = LogManager.getLogger(CostFunctionTest.class);

    /**
     * Case: AWS IO1 storage tier as a seller. Storage amount unit price is 2, IOPS unit price is 10.
     * VM1 asks for 3GB, 90 IOPS, VM2 asks for 5GB, 500IOPS, VM3 asks for 10GB, 200IOPS
     * Expected result:
     * VM2 infinity cost because IOPS is more than max ratio of 50 storage amount,
     * VM3 cost is (10*0.125+200*0.065)
     */
    @Test
    public void testCostFunction_CalculateCost_AWSIO1CostFunction() {
        Economy economy = new Economy();
        BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        Trader io1 = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        io1.getSettings().setContext(new Context(10L, zoneId, ba));
        CostFunction io1Function = TestUtils.setUpIO1CostFunction();
        io1.getSettings().setCostFunction(io1Function);

        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm2, new double[] {5, 500},
                        null);
        sl2.setDemandScalable(false);
        // get quote as if in Savings mode with penalty
        assertTrue(io1Function.calculateCost(sl2, io1, true, economy)
                .getQuoteValue() > CostFunctionFactoryHelper.REVERSIBILITY_PENALTY_COST);

        // create VM3 and get its cost from io1
        Trader vm3 = TestUtils.createVM(economy);
        vm3.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm3,
                        new double[] {10, 200}, null);
        assertEquals((10 * 0.125 + 200 * 0.065),
            io1Function.calculateCost(sl3, io1, true, economy).getQuoteValue(),
            TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Case: AWS GP2 storage tier as a seller. Storage amount unit price is 0.1.
     * VM asks for 5GB, 120 IOPS, VM2 asks for 5GB, 80IOPS.
     * Expected result:
     * VM infinity cost because IOPS is more than 5 * maxRatio and more than minIops(100)
     * VM2 cost is (5 * 0.01) because 80 is less than minIops of GP2 so gp2 is actually qualified,
     * though 80 is greater than 5 * 3.
     */
    @Test
    public void testCostFunction_CalculateCost_AWSGP2CostFunction() {
        Economy economy = new Economy();
        BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        Trader gp2 = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        CostFunction gp2Function = TestUtils.setUpGP2CostFunction();
        gp2.getSettings().setCostFunction(gp2Function);

        // create VM2 and get its cost from io1
        Trader vm = TestUtils.createVM(economy);
        vm.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm, new double[] {5, 120},
                null);
        sl.setDemandScalable(false);
        // get quote as if in Savings mode with penalty
        assertTrue(gp2Function.calculateCost(sl, gp2, true, economy).getQuoteValue()
                > CostFunctionFactoryHelper.REVERSIBILITY_PENALTY_COST);

        // create VM3 and get its cost from io1
        Trader vm3 = TestUtils.createVM(economy);
        vm3.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm3,
                new double[] {5, 80}, null);
        assertEquals((5 * 0.1),
            gp2Function.calculateCost(sl2, gp2, true, economy).getQuoteValue(),
            TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Case: Azure premium managed storage tier as a seller. Storage amount price is 5.28 if 0~32GB,
     * 10.21 if 32~64 GB
     * VM1 asks for 0.5GB,  VM2 asks for 64GB
     * Expected result:
     * VM2 cost is 10.21
     */
    @Test
    public void testCostFunction_CalculateCost_AzurePremiumManagedCostFunction() {
        Economy economy = new Economy();
        BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        Trader premiumManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        premiumManaged.getSettings().setContext(new Context(10L, zoneId, ba));
        CostFunction premiumManagedFunction = TestUtils.setUpPremiumManagedCostFunction();
        premiumManaged.getSettings().setCostFunction(premiumManagedFunction);


        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[] {64}, null);
        assertEquals(10.21, premiumManagedFunction.calculateCost(sl2, premiumManaged, true, economy).getQuoteValue(),
                        TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Case: AWS t2.nano as a seller. Assuming its price is as below:
     * Business account 1, Linux license, 1.5
     * Business account 1, Windows license, 2.5
     * VM1 has business account 1 and asks for Linux license
     * VM2 has business account 1 and asks for Windows license
     * Expected result:
     * VM1 cost is 1.5, VM2 cost is 2.5.
     */
    @Test
    public void testCostFunction_CalculateCost_AWSComputeTierCostFunction() {
        Economy economy = new Economy();
        BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        CommoditySpecification linuxComm =
                new CommoditySpecification(TestUtils.LINUX_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, false);
        CommoditySpecification windowsComm =
                new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, false);
        CommoditySpecification regionComm =
                new CommoditySpecification(TestUtils.REGION_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, false);
        Trader t2Nano = economy.addTrader(1, TraderState.ACTIVE, new Basket(linuxComm, windowsComm, regionComm),
                                          Arrays.asList(0l));
        t2Nano.getSettings().setContext(new Context(10L, zoneId, ba));
        t2Nano.getCommoditiesSold().get(t2Nano.getBasketSold().indexOf(linuxComm)).setCapacity(1000);
        t2Nano.getCommoditiesSold().get(t2Nano.getBasketSold().indexOf(windowsComm)).setCapacity(1000);
        t2Nano.getCommoditiesSold().get(t2Nano.getBasketSold().indexOf(regionComm)).setCapacity(1000);
        CostFunction t2NanoFunction = TestUtils.setUpT2NanoCostFunction();
        t2Nano.getSettings().setCostFunction(t2NanoFunction);

        Trader vm1 = TestUtils.createVM(economy);
        vm1.getSettings().setContext(new Context(10L, zoneId, ba));
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList vm1Sl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(linuxComm), vm1,
                new double[]{1}, new double[]{1}, t2Nano);
        ShoppingList vm2Sl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(windowsComm), vm2,
                new double[]{1}, new double[]{1}, t2Nano);
        assertEquals(1.5, t2NanoFunction.calculateCost(vm1Sl, t2Nano, false, economy).getQuoteValue(),
                        TestUtils.FLOATING_POINT_DELTA);
        assertEquals(2.5, t2NanoFunction.calculateCost(vm2Sl, t2Nano, false, economy).getQuoteValue(),
                     TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Create 2 CBTPs - t3_cbtp and t3a_cbtp.
     * t3_cbtp has 2 TPs underlying it - t3_nano and t3_2xlarge.
     * t3a_cbtp has 2 TPs underlying it - t3a_nano and t3a_2xlarge.
     *
     * <p>t3_cbtp is cheaper than t3a_cbtp (because largest in t3 - t3_2xlarge is cheaper than
     * largest in t3a - t3a_2xlarge). But the VM we create can fit in nano size.
     * So, for this VM, t3a_cbtp gets a lower quote.</p>
     */
    @Test
    public void testRiPricing() {

        // Template Prices used to create CostDTOs
        final double riDeprecationFactor = 0.00001;
        final double priceT3Nano = 0.0052;
        final double priceT32xlarge = 0.3008;

        final double priceT3aNano = 0.0047;
        final double priceT3a2xlarge = 0.3328;

        BiMap<Trader, Long> traderOids = HashBiMap.create();
        // Create a new Economy
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0, 0L);
        vm.getSettings().setContext(new Context(regionId, zoneId, ba));

        // Create t3 CBTP
        Trader t3Cbtp = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU), new double[] {25, 3000},
                true, true, "t3Cbtp");
        traderOids.forcePut(t3Cbtp, 1L);
        t3Cbtp.getSettings().setContext(new Context(regionId, zoneId, ba));
        t3Cbtp.getSettings().setQuoteFunction(QuoteFunctionFactory
            .budgetDepletionRiskBasedQuoteFunction());
        CbtpCostDTO.Builder cbtpBundleBuilder = TestUtils.createCbtpBundleBuilder(
            TestUtils.COUPON_COMMODITY.getBaseType(), priceT32xlarge * riDeprecationFactor, 10, true, regionId);
        t3Cbtp.getSettings().setCostFunction(CostFunctionFactory
            .createResourceBundleCostFunctionForCbtp(cbtpBundleBuilder.build()));

        // Create t3a CBTP
        Trader t3aCbtp = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU), new double[] {25, 3000},
                true, true, "t3aCbtp");
        traderOids.forcePut(t3aCbtp, 2L);
        t3aCbtp.getSettings().setContext(new Context(regionId, zoneId, ba));
        t3aCbtp.getSettings().setQuoteFunction(QuoteFunctionFactory
                .budgetDepletionRiskBasedQuoteFunction());
        CbtpCostDTO.Builder cbtpBundleBuilder2 = TestUtils.createCbtpBundleBuilder(
            TestUtils.COUPON_COMMODITY.getBaseType(), priceT3a2xlarge * riDeprecationFactor, 50, true, regionId);
        t3aCbtp.getSettings().setCostFunction(CostFunctionFactory
                .createResourceBundleCostFunctionForCbtp(cbtpBundleBuilder2.build()));

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();

        final boolean forTraderIncomeStatement = true;

        CommoditySpecification t3_templateAccess = TestUtils.createNewCommSpec();
        CommoditySpecification t3a_templateAccess = TestUtils.createNewCommSpec();
        Trader t3NanoTp = createTp(economy, traderOids, "t3NanoTp", priceT3Nano, 3L, t3_templateAccess);
        Trader t32xlargeTp = createTp(economy, traderOids, "t32xlargeTp", priceT32xlarge, 3L, t3_templateAccess);
        Trader t3aNanoTp = createTp(economy, traderOids, "t3aNanoTp", priceT3aNano, 3L, t3a_templateAccess);
        Trader t3a2xlargeTp = createTp(economy, traderOids, "t3a2xlargeTp", priceT3a2xlarge, 3L, t3a_templateAccess);

        // Create VM shopping lists
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, t3NanoTp);

        // This is not really required but it helps to negate through various if conditions and go into the loop
        // where we use RI pricing as the differentiator between cbtps
        vmSL.setGroupFactor(1L);

        // Create shopping lists for cbtps to shop from the tp
        ShoppingList t3CbtpSl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY, t3_templateAccess), t3Cbtp, new double[] {25, 1, 1}, null);
        ShoppingList t3aCbtpSl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY, t3a_templateAccess), t3aCbtp, new double[] {25, 1, 1}, null);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        try {
            Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOidField.set(topo, traderOids);
            Field unmodifiableTraderOidField = Topology.class
                    .getDeclaredField("unmodifiableTraderOids_");
            unmodifiableTraderOidField.setAccessible(true);
            unmodifiableTraderOidField.set(topo, traderOids);
        } catch (Exception e) {
            logger.error("Error setting up topology.");
        }

        // Test to check that cbtp2 gives a cheaper quote than cbtp1
        double t3_quote = EdeCommon.quote(economy, vmSL, t3Cbtp, bestQuoteSoFar.getQuoteValue(),
            forTraderIncomeStatement).getQuoteValue();
        double t3a_quote = EdeCommon.quote(economy, vmSL, t3aCbtp, bestQuoteSoFar.getQuoteValue(),
            forTraderIncomeStatement).getQuoteValue();
        assertTrue(t3_quote > t3a_quote);
        assertEquals(priceT3aNano * riDeprecationFactor, t3a_quote, 0.00000000001);
        assertEquals(priceT3Nano * riDeprecationFactor, t3_quote, 0.00000000001);
    }

    private Trader createTp(Economy economy, BiMap<Trader, Long> traderOids, String debugInfo,
                            double price, long traderOid, CommoditySpecification templateAccess) {
        // Create a Trader TP which serves as seller for cbtps
        Trader tp = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0L),
            Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY, templateAccess),
            new double[] {1, 3000, 20, 1}, true,
            true, debugInfo);
        traderOids.forcePut(tp, traderOid);

        // Set the cost dto on the TP
        ComputeTierCostDTO.Builder costBundleBuilder =
            ComputeTierCostDTO.newBuilder();
        costBundleBuilder.addComputeResourceDepedency(ComputeResourceDependency.newBuilder()
            .setBaseResourceType(CommoditySpecificationTO.newBuilder()
                .setBaseType(0)
                .setType(0))
            .setDependentResourceType(CommoditySpecificationTO.newBuilder()
                .setBaseType(0)
                .setType(0)));
        costBundleBuilder.setLicenseCommodityBaseType(3);
        costBundleBuilder.setCouponBaseType(68);
        CostDTO tpCostDto = CostDTO.newBuilder().setComputeTierCost(costBundleBuilder.addCostTupleList(CostTuple.newBuilder()
            .setLicenseCommodityType(-1)
            .setPrice(price)
            .setBusinessAccountId(24)
            .setRegionId(10L)
            .build()).build()).build();

        tp.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        tp.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(tpCostDto));
        return tp;
    }

    @Test
    public void test_DiscountedComputeCostFactor() {

        float discounted_compute_costFactor = 5;
        // Costs for template
        double t2NanoCost = 0.0063;
        // Cost greater than discounted_compute_costFactor * costOnCurrentSupplier (in this case t2Nano)
        double m5Large = 0.19200;
        BiMap<Trader, Long> traderOids = HashBiMap.create();

        // Create economy with discounted compute cost factor
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);
        economy.getSettings().setDiscountedComputeCostFactor(discounted_compute_costFactor);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "vm-buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0, 0L);
        vm.getSettings().setContext(new Context(10L, zoneId, ba));

        // Create a TP for this VM
        Trader t2NanoTP = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.CPU),
                new double[] {3000}, true,
                true, "t2Nano");
        traderOids.put(t2NanoTP, 1L);

        // Set the cost dto on the TP
        CostDTO t2NanoTP_CostDto = CostDTO.newBuilder().setComputeTierCost(
            TestUtils.getComputeTierCostDTOBuilder().addCostTupleList(CostTuple.newBuilder()
                .setLicenseCommodityType(-1)
                .setPrice(t2NanoCost)
                    .setRegionId(10L)
                .setBusinessAccountId(24)
                .build()).build()).build();

        t2NanoTP.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        t2NanoTP.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(t2NanoTP_CostDto));

        // Place VM on t2Nano TP
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, t2NanoTP);
        vmSL.setGroupFactor(1L);

        // Create CBTP
        Trader cbtp_m5Large = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy, true,
                regionId, 0, 0);
        traderOids.put(cbtp_m5Large, 2L);

        // Create a new TP which sells to CBTP
        Trader m5LargeTP = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY),
                new double[] {8, 3000, 1}, true,
                true, "m5Large");
        traderOids.put(m5LargeTP, 3L);

        // Set the cost dto on the TP
        CostDTO m5LargeTP_CostDto = CostDTO.newBuilder().setComputeTierCost(
            TestUtils.getComputeTierCostDTOBuilder().addCostTupleList(CostTuple.newBuilder()
                .setLicenseCommodityType(-1)
                .setPrice(m5Large)
                    .setRegionId(10L)
                .setBusinessAccountId(24)
                .build()).build()).build();

        m5LargeTP.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        m5LargeTP.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(m5LargeTP_CostDto));

        // Place CBTP on TP
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.SEGMENTATION_COMMODITY),
                        cbtp_m5Large, new double[] {2, 1}, m5LargeTP);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        try {
            Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOidField.set(topo, traderOids);
            Field unmodifiableTraderOidField = Topology.class
                    .getDeclaredField("unmodifiableTraderOids_");
            unmodifiableTraderOidField.setAccessible(true);
            unmodifiableTraderOidField.set(topo, traderOids);
        } catch (Exception e) {
            logger.error("Error setting up topology.");
        }

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();
        boolean forTraderIncomeStatement = true;

        // Act
        double q1 = EdeCommon.quote(economy, vmSL, cbtp_m5Large, bestQuoteSoFar.getQuoteValue(),
                        forTraderIncomeStatement).getQuoteValue();

        // Infinite quote because on demand cost of cbtP > discounted_compute_costFactor * costOnCurrentSupplier
        assertTrue(Double.isInfinite(q1));

        // Change cost Factor : 0.0192/0.00063 ~31
        economy.getSettings().setDiscountedComputeCostFactor(31);

        q1 = EdeCommon.quote(economy, vmSL, cbtp_m5Large, bestQuoteSoFar.getQuoteValue(),
                        forTraderIncomeStatement).getQuoteValue();

        assertTrue(Double.isFinite(q1));
    }

    @Test
    public void test_DiscountedComputeCostFactor_NoSupplier() {
        // Very big cost factor (for a VM with no supplier this factor (no matter how big)
        // should return infinite quote.
        float discounted_compute_costFactor = 100000;

        double m5Large = 0.19200;

        // Create economy with discounted compute cost factor
        Economy economy = new Economy();
        economy.getSettings().setDiscountedComputeCostFactor(discounted_compute_costFactor);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "vm-buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0);
        vm.getSettings().setContext(new Context(10L, zoneId, ba));

        // VM with no supplier
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, null);
        vmSL.setGroupFactor(1L);

        // Create CBTP
        Trader cbtp_m5Large = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy, true,
                regionId, 0, 0);

        // Create a new TP which sells to CBTP
        Trader m5LargeTP = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY),
                new double[] {8, 3000, 1}, true,
                true, "m5Large");

        // Set the cost dto on the TP
        CostDTO m5LargeTP_CostDto = CostDTO.newBuilder().setComputeTierCost(
            TestUtils.getComputeTierCostDTOBuilder().addCostTupleList(CostTuple.newBuilder()
                .setLicenseCommodityType(-1)
                .setPrice(m5Large)
                .setBusinessAccountId(24)
                .build()).build()).build();

        m5LargeTP.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        m5LargeTP.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(m5LargeTP_CostDto));

        // Place CBTP on TP
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.SEGMENTATION_COMMODITY),
                        cbtp_m5Large, new double[] {2, 1}, m5LargeTP);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();
        boolean forTraderIncomeStatement = true;

        // Act
        double q1 = EdeCommon.quote(economy, vmSL, cbtp_m5Large, bestQuoteSoFar.getQuoteValue(),
                        forTraderIncomeStatement).getQuoteValue();

        // Infinite quote because no supplier
        assertTrue(Double.isInfinite(q1));
    }

    /**
     * Test that if a VM belongs to a zone different from a discounted tier's zone scope then an
     * infinite quote is returned and if a VM belongs to the same zone as a discounted tier's
     * zone scope then a non-infinity quote is returned.
     */
    @Test
    public void testZonalLocationDiscountedComputeCost() {
        final long zone11 = 11L;
        final long zone13 = 13L;
        double m5Large = 0.19200;
        BiMap<Trader, Long> traderOids = HashBiMap.create();

        // Create economy with discounted compute cost factor
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "vm-buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0, 0L);
        vm.getSettings().setContext(new Context(10L, zone13, ba));

        // VM with no supplier
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, null);
        vmSL.setGroupFactor(1L);

        // Create a new TP which sells to CBTPs
        Trader m5LargeTP = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY),
                new double[] {8, 3000, 1}, true,
                true, "m5Large");
        traderOids.put(m5LargeTP, 1L);

        // Set the cost dto on the TP
        CostDTO m5LargeTP_CostDto = CostDTO.newBuilder().setComputeTierCost(
                TestUtils.getComputeTierCostDTOBuilder().addCostTupleList(CostTuple.newBuilder()
                        .setLicenseCommodityType(-1)
                        .setRegionId(10L)
                        .setPrice(m5Large)
                        .setBusinessAccountId(24)
                        .build()).build()).build();

        // CBTP with zone scope 11
        Trader m5LargeCbtpZone11 = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy,
                false, zone11, 0, 0);
        traderOids.put(m5LargeCbtpZone11, 2L);
        // CBTP with zone scope 13
        Trader m5LargetCbtpZone13 = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy,
                false, zone13, 0, 0);
        traderOids.put(m5LargetCbtpZone13, 3L);

        m5LargeTP.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        m5LargeTP.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(m5LargeTP_CostDto));

        // Place CBTPs on TP
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.SEGMENTATION_COMMODITY),
                m5LargeCbtpZone11, new double[] {2, 1}, m5LargeTP);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.SEGMENTATION_COMMODITY),
                m5LargetCbtpZone13, new double[] {2, 1}, m5LargeTP);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        try {
            Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOidField.set(topo, traderOids);
            Field unmodifiableTraderOidField = Topology.class
                    .getDeclaredField("unmodifiableTraderOids_");
            unmodifiableTraderOidField.setAccessible(true);
            unmodifiableTraderOidField.set(topo, traderOids);
        } catch (Exception e) {
            logger.error("Error setting up topology.");
        }

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();
        boolean forTraderIncomeStatement = true;

        // Quote for VM from CBTP scoped to zone 11
        double q1 = EdeCommon.quote(economy, vmSL, m5LargeCbtpZone11, bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue();

        // Infinite quote because the VM is on a different zone (zone 13)
        assertTrue(Double.isInfinite(q1));

        // Quote for VM from CBTP scoped to zone 13
        double q2 = EdeCommon.quote(economy, vmSL, m5LargetCbtpZone13,
                bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // Non-infinite quote since the VM and CBTP are both on zone 13
        Assert.assertFalse(Double.isInfinite(q2));
    }

    /**
     * Test that if a VM has a price id and business account id, then it gets an infinite quote
     * from a CBTP scoped to an account which is different from the VM's account.
     */
    @Test
    public void testAccountScopeDiscountedComputeCost() {
        final long priceId = 1111L;
        final long vmBusinessAccountId = 24L;
        final long cbtpBusinessAccountId = 3333L;

        // Create economy with discounted compute cost factor
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "vm-buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, vmBusinessAccountId, priceId,
                priceId);
        vm.getSettings().setContext(new Context(10L, 0L, ba));

        // VM with no supplier
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy,
                Collections.singletonList(TestUtils.CPU), vm, new double[] {1000}, null);
        vmSL.setGroupFactor(1L);

        // Create a new TP which sells to CBTPs
        Trader m5LargeTP = TestUtils.createTrader(economy, TestUtils.PM_TYPE,
                Collections.singletonList(0L), Arrays.asList(TestUtils.COUPON_COMMODITY,
                        TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY), new double[] {8, 3000, 1},
                true, true, "m5Large");
        BiMap<Trader, Long> traderOids = HashBiMap.create();
        traderOids.put(m5LargeTP, 1L);
        double m5LargePrice = 0.19200;

        // CBTP with Account id 3333L
        Trader m5LargetCbtpAccount1 = TestUtils.setAndGetCBTP(m5LargePrice, "cbtp_m5Large", economy,
                false, 0, cbtpBusinessAccountId, priceId);
        traderOids.put(m5LargetCbtpAccount1, 2L);
        // CBTP with Account id 24L and priceId 1111L
        Trader m5LargetCbtpAccount2 = TestUtils.setAndGetCBTP(m5LargePrice, "cbtp_m5Large", economy,
                false, 0, vmBusinessAccountId, priceId);
        traderOids.put(m5LargetCbtpAccount2, 3L);

        m5LargeTP.getSettings().setQuoteFunction(QuoteFunctionFactory
                .budgetDepletionRiskBasedQuoteFunction());
        // Set the cost dto on the TP
        CostDTO m5LargeTP_CostDto = CostDTO.newBuilder().setComputeTierCost(
                TestUtils.getComputeTierCostDTOBuilder().addCostTupleList(CostTuple.newBuilder()
                        .setLicenseCommodityType(-1)
                        .setPrice(m5LargePrice)
                        .setRegionId(10L)
                        .setBusinessAccountId(vmBusinessAccountId)
                        )).build();
        m5LargeTP.getSettings().setCostFunction(CostFunctionFactory
                .createCostFunction(m5LargeTP_CostDto));

        // Place CBTPs on TP
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), m5LargetCbtpAccount1, new double[] {2, 1},
                m5LargeTP);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), m5LargetCbtpAccount2, new double[] {2, 1},
                m5LargeTP);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        try {
            final Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOidField.set(topo, traderOids);
            final Field unmodifiableTraderOidField = Topology.class
                    .getDeclaredField("unmodifiableTraderOids_");
            unmodifiableTraderOidField.setAccessible(true);
            unmodifiableTraderOidField.set(topo, traderOids);
        } catch (Exception e) {
            logger.error("Error setting up topology.");
        }

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();
        final boolean forTraderIncomeStatement = true;

        // Quote for VM from CBTP scoped to account 3333L
        final double q1 = EdeCommon.quote(economy, vmSL, m5LargetCbtpAccount1,
                bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue();

        // Infinite quote because the VM is on a account (24)
        assertTrue(Double.isInfinite(q1));

        // Quote for VM from CBTP scoped to account 24
        final double q2 = EdeCommon.quote(economy, vmSL, m5LargetCbtpAccount2,
                bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // Non-infinite quote since the VM and CBTP are both on account 24
        Assert.assertFalse(Double.isInfinite(q2));
    }

    /**
     * Create a CostTable and make sure that given an Account Id and License Type,
     * the first RegionCost is the one with lowest cost.
     */
    @Test
    public void test_CostTable() {
        // Create a CostTable with multiple Accounts and Regions cost
        List<CostTuple> tuples = TestUtils.setUpMultipleRegionsCostTuples();
        CostTable costTable = new CostTable(tuples);

        // Assert: Account 1, Linux license, cheapest Region
        CostTuple regionCost = costTable.getTuple(TestUtils.NO_TYPE, 1, TestUtils.LINUX_COMM_TYPE);
        assertEquals(1.5d, regionCost.getPrice(), 0);
        assertEquals(300, regionCost.getRegionId());

        // Assert: Account 1, Windows license, cheapest Region
        regionCost = costTable.getTuple(TestUtils.NO_TYPE, 1, TestUtils.WINDOWS_COMM_TYPE);
        assertEquals(1.7d, regionCost.getPrice(), 0);
        assertEquals(300, regionCost.getRegionId());

        // Assert: Region 2, Account 1, Linux license
        CostTuple costTuple = costTable.getTuple(TestUtils.DC2_COMM_TYPE, 1, TestUtils.LINUX_COMM_TYPE);
        assertEquals(2.5d, costTuple.getPrice(), 0);

        // Assert: Region 3, Account 2, Windows license
        costTuple = costTable.getTuple(TestUtils.DC3_COMM_TYPE, 2, TestUtils.WINDOWS_COMM_TYPE);
        assertEquals(3.9d, costTuple.getPrice(), 0);
    }

    /**
     * Create a CostFunction with License and Region defined using a CostDTO with multiple Regions,
     * the resulting Quote contains the RegionId that has the lowest cost.
     */
    @Test
    public void testDatabaseTierComputeFunctionWithLicenseAndRegion() {
        final CostFunction cf = CostFunctionFactory.createCostFunction(TestUtils.setUpDatabaseTierCostDTO(
                TestUtils.LICENSE_COMM_BASE_TYPE,
                TestUtils.COUPON_COMM_BASE_TYPE,
                TestUtils.DC_COMM_BASE_TYPE));
        final MutableQuote quote = calculateCost(TestUtils.DC4_COMM_TYPE, cf);

        // Assert: the quote contains the cost for WINDOWS License and DC4 Region
        assertTrue(quote instanceof CommodityCloudQuote);
        assertTrue(quote.getContext().isPresent());
        assertEquals(TestUtils.DC4_COMM_TYPE, quote.getContext().get().getRegionId());
        assertEquals(4.7d, quote.getQuoteValue(), 0);
    }

    /**
     * Create a CostFunction with only License defined using a CostDTO with multiple Regions,
     * We should get an infinite quote
     */
    @Test
    public void testDatabaseTierComputeFunctionWithLicenseOnly() {
        final CostFunction cf = CostFunctionFactory.createCostFunction(TestUtils.setUpDatabaseTierCostDTO(
                TestUtils.LICENSE_COMM_BASE_TYPE,
                TestUtils.COUPON_COMM_BASE_TYPE,
                TestUtils.NO_TYPE));
        final MutableQuote quote = calculateCost(TestUtils.DC5_COMM_TYPE, cf);

        assertFalse(quote.getContext().isPresent());
        // Assert: the quote contains an infinite cost
        assertTrue(quote.isInfinite());
    }

    /**
     * Create a CostFunction with only Region defined using a CostDTO with multiple Regions,
     * the resulting Quote contains the RegionId that has the lowest cost.
     */
    @Test
    public void testDatabaseTierComputeFunctionWithRegionOnly() {
        final CostFunction cf = getCostFunctionWithRegionOnly();
        final MutableQuote quote = calculateCost(TestUtils.DC4_COMM_TYPE, cf);

        // Assert: the quote contains the cost for the no License
        assertTrue(quote instanceof CommodityCloudQuote);
        assertTrue(quote.getContext().isPresent());
        assertEquals(TestUtils.DC4_COMM_TYPE, quote.getContext().get().getRegionId());
        assertEquals(4.5d, quote.getQuoteValue(), 0);

    }

    private static CostFunction getCostFunctionWithRegionOnly() {
        return CostFunctionFactory.createCostFunction(TestUtils.setUpDatabaseTierCostDTO(
                TestUtils.NO_TYPE,
                TestUtils.COUPON_COMM_BASE_TYPE,
                TestUtils.DC_COMM_BASE_TYPE));
    }

    /**
     * Create a CostFunction with only Region defined using a CostDTO with multiple Regions,
     * the resulting Quote contains the RegionId that has the lowest cost.
     * Region has infinite cost.
     */
    @Test
    public void testDatabaseTierComputeFunctionWithRegionOnlyInfinitCost() {
        final CostFunction cf = getCostFunctionWithRegionOnly();
        final MutableQuote quote = calculateCost(TestUtils.DC3_COMM_TYPE, cf);

        // Assert: the quote contains the cost for the no License
        assertTrue(quote instanceof CommodityCloudQuote);
        assertTrue(quote.getContext().isPresent());
        assertEquals(TestUtils.DC3_COMM_TYPE, quote.getContext().get().getRegionId());
        assertEquals(Double.POSITIVE_INFINITY, quote.getQuoteValue(), 0);
    }

    /**
     * Setup a DBS with Windows license and Account 1 and calculate cost.
     *
     * @param dcCommType region type.
     * @param cf cost function
     * @return cost as mutable quote.
     */
    private static MutableQuote calculateCost(int dcCommType, CostFunction cf) {
        final Economy economy = new Economy();
        final BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        final Trader dbs1 = TestUtils.createDBS(economy);
        dbs1.getSettings().setContext(new Context(dcCommType, zoneId, ba));
        final CommoditySpecification winComm =
                new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, false);
        final CommoditySpecification dcComm =
                new CommoditySpecification(dcCommType, TestUtils.DC_COMM_BASE_TYPE, false);
        final Trader trader = economy.addTrader(1, TraderState.ACTIVE, new Basket(winComm, dcComm),
                Arrays.asList(0L));
        trader.getSettings().setContext(new Context(dcCommType, zoneId, ba));
        trader.getCommoditiesSold().get(trader.getBasketSold().indexOf(winComm)).setCapacity(1000);
        final ShoppingList buyer = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(winComm, dcComm), dbs1,
                new double[]{1, 1}, new double[]{1, 1}, trader);

        return cf.calculateCost(buyer, trader, false, economy);
    }
}
