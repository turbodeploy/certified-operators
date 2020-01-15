package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
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
import com.vmturbo.platform.analysis.utilities.CostTable;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityCloudQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InitialInfiniteQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CostFunctionTest {

    private static final long zoneId = 0L;
    private static final long regionId = 10L;

    static final Logger logger = LogManager.getLogger(CostFunctionTest.class);
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
        assertEquals(Double.POSITIVE_INFINITY,
            io1Function.calculateCost(sl2, io1, true, economy).getQuoteValue(),
            TestUtils.FLOATING_POINT_DELTA);

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
        assertTrue(Double.isInfinite(gp2Function.calculateCost(sl, gp2, true, economy).getQuoteValue()));

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
     * Case: Azure standard unmanaged storage tier as a seller. Storage amount price is 0.05 per GB if 1~1024GB in LRS,
     * 0.10 per GB if 1024 ~ 50*1024 in LRS, VM1 asks for 2000GB
     * Expected result: VM1 cost is 0.05 * 1024 + 0.10 * (2000 - 1024)
     */
    @Test
    public void testCostFunction_CalculateCost_AzureStandardUnmanagedCostFunction() {
        Economy economy = new Economy();
        BalanceAccount ba = new BalanceAccount(100, 10000, 1, 0);
        Trader standardUnManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        standardUnManaged.getSettings().setContext(new Context(10L, zoneId, ba));
        CostFunction standardUnManagedFunction = TestUtils.setUpStandardUnmanagedCostFunction();
        standardUnManaged.getSettings().setCostFunction(standardUnManagedFunction);

        // create VM1 and get its cost
        Trader vm1 = TestUtils.createVM(economy);
        vm1.getSettings().setContext(new Context(10L, zoneId, ba));
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[] {2000}, null);
        assertEquals(0.05 * 1024 + 0.10 * (2000 - 1024),
                        standardUnManagedFunction.calculateCost(sl1,
                        standardUnManaged, true, economy).getQuoteValue(), TestUtils.FLOATING_POINT_DELTA);

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
     * Test to check whether low RI pricing may introduce equal quotes from 2 cbtps. When RI pricing
     * is too low, we might run into floating point issues where 2 cbtps may return the same cost.
     * This test covers cases where we test various scenarios which may result by varying RI prices and
     * the budget. For example T2.large and T3.large have onDemandCost of 0.12 and 0.11 respectively,
     * but the scaled down RiPricing we send makes sure that they will both get a quote of 1.0000000000000002
     * which makes random assignment of these cbtps across multiple market cycles possible.
     */
    @Test
    public void testRI_Pricing() {

        // Template Prices used to create CostDTOs
        double riDeprecationFactor = 0.00001;
        double m5Large = 0.19200;
        double r4Large = 0.131600;
        double m4Large = 0.019200;
        double c4Large = 0.0131600;
        double t2Large = 0.12;
        double t3Large = 0.11;
        BiMap<Trader, Long> traderOids = HashBiMap.create();
        // Create a new Economy
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0);
        vm.getSettings().setContext(new Context(regionId, zoneId, ba));

        // Create CBTPs
        Trader cbtp1 = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU),new double[] {25, 3000},
                true, true, "cbtp");
        traderOids.forcePut(cbtp1, 1L);

        Trader cbtp2 = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU),new double[] {25, 3000},
                true, true, "cbtp2");
        traderOids.forcePut(cbtp2, 2L);

        cbtp1.getSettings().setContext(new Context(regionId, zoneId, ba));
        cbtp1.getSettings().setQuoteFunction(QuoteFunctionFactory
                .budgetDepletionRiskBasedQuoteFunction());

        cbtp2.getSettings().setContext(new Context(regionId, zoneId, ba));
        cbtp2.getSettings().setQuoteFunction(QuoteFunctionFactory
                .budgetDepletionRiskBasedQuoteFunction());

        CbtpCostDTO.Builder cbtpBundleBuilder = TestUtils.createCbtpBundleBuilder(0,
                m5Large * riDeprecationFactor, 10, true, regionId);
        cbtp1.getSettings().setCostFunction(CostFunctionFactory
                .createResourceBundleCostFunctionForCbtp(cbtpBundleBuilder.build()));
        CbtpCostDTO.Builder cbtpBundleBuilder2 = TestUtils.createCbtpBundleBuilder(0,
                r4Large * riDeprecationFactor, 50, true, regionId);
        cbtp2.getSettings().setCostFunction(CostFunctionFactory
                .createResourceBundleCostFunctionForCbtp(cbtpBundleBuilder2.build()));

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();

        boolean forTraderIncomeStatement = true;

        // Create a Trader TP which serves as seller for cbtps
        Trader tp = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY),
                new double[] {1, 3000, 20}, true,
                true, "tp");
        traderOids.forcePut(tp, 3L);

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
                .setPrice(0.0341)
                .setBusinessAccountId(24)
                .setRegionId(10L)
                .build()).build()).build();

        tp.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        tp.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(tpCostDto));

        // Create VM shopping lists
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, tp);

        // This is not really required but it helps to negate through various if conditions and go into the loop
        // where we use RI pricing as the differentiator between cbtps
        vmSL.setGroupFactor(1L);

        // Create shopping lists for cbtps to shop from the top
        ShoppingList cbtpSl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), cbtp1, new double[] {25, 1}, tp);
        ShoppingList cbtpSl2 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), cbtp2, new double[] {25, 1}, tp);
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
        Assert.assertTrue(EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue() > EdeCommon.quote(economy, vmSL, cbtp2,
                bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue());

        // Test to check if we scale the previous costs down by a factor of 10, we get the same quote
        CbtpCostDTO.Builder cbtpBundleBuilder3 = TestUtils.createCbtpBundleBuilder(0,m4Large*riDeprecationFactor,
                10, true, regionId);

        CostDTO costDTOcbtp3 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder3.build()).build();
        CbtpCostDTO cdDTo3 = costDTOcbtp3.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo3));

        CbtpCostDTO.Builder cbtpBundleBuilder4 = TestUtils.createCbtpBundleBuilder(0, c4Large*riDeprecationFactor,
                50, true, regionId);

        CostDTO costDTOcbtp4 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder4.build()).build();
        CbtpCostDTO cdDTo4 = costDTOcbtp4.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo4));

        double quoteCbtp1 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        // Test to check that we get a quote of 1.0 from cbtp1
        assertEquals(quoteCbtp1, 1.0, TestUtils.FLOATING_POINT_DELTA);

        double quoteCbtp2= EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue();
        // Test to check that quotes are equal ie both are 1.0
        assertEquals(quoteCbtp1, quoteCbtp2, TestUtils.FLOATING_POINT_DELTA);

        // Set the price for a t2.large cbtp
        CbtpCostDTO.Builder cbtpBundleBuilder5 = TestUtils.createCbtpBundleBuilder(0,t2Large*riDeprecationFactor,
                10, true, regionId);

        CostDTO costDTOcbtp5 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder5.build()).build();
        CbtpCostDTO cdDTo5 = costDTOcbtp5.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo5));

        // Set the price for a t3.large cbtp
        CbtpCostDTO.Builder cbtpBundleBuilder6 = TestUtils.createCbtpBundleBuilder(0, t3Large*riDeprecationFactor,
                50, true, regionId);

        CostDTO costDTOcbtp6 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder6.build()).build();
        CbtpCostDTO cdDTo6 = costDTOcbtp6.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo6));

        double quoteCbtp3 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        double quoteCbtp4 = EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // t2.large and t3.large give same quote
        assertEquals(quoteCbtp3, quoteCbtp4, TestUtils.FLOATING_POINT_DELTA);

        // Now a test by changing budget on business account and increasing the riDeprecationFactor
        // to 10^-5 from 10^-7
        // Update the budget on balance account
        BalanceAccount ba2 = new BalanceAccount(0.0, 10000d, 24, 0);
        cbtp1.getSettings().setContext(new Context(10L, zoneId, ba2));
        cbtp2.getSettings().setContext(new Context(10L, zoneId, ba2));

        double updatedriDprecationFactor = 0.00001;
        // Set the price for a t2.large cbtp with updated riDeprecationFactor
        CbtpCostDTO.Builder cbtpBundleBuilder7 = TestUtils.createCbtpBundleBuilder(0,t2Large*updatedriDprecationFactor,
                10, true, regionId);

        CostDTO costDTOcbtp7 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder7.build()).build();
        CbtpCostDTO cdDTo7 = costDTOcbtp7.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo7));

        // Set the price for a t3.large cbtp with updated riDeprecationFactor
        CbtpCostDTO.Builder cbtpBundleBuilder8 = TestUtils.createCbtpBundleBuilder(0, t3Large*updatedriDprecationFactor,
                50, true, regionId);

        CostDTO costDTOcbtp8 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder8.build()).build();
        CbtpCostDTO cdDTo8 = costDTOcbtp8.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo8));

        double quoteCbtp5 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        double quoteCbtp6 = EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // t2.large gives higher quote than t3.large which should be the case
        assert(quoteCbtp5 > quoteCbtp6);
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
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0);
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
                regionId);
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
                regionId);

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
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24, 0);
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
                        .setPrice(m5Large)
                        .setBusinessAccountId(24)
                        .build()).build()).build();

        // CBTP with zone scope 11
        Trader m5LargeCbtpZone11 = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy,
                false, zone11);
        traderOids.put(m5LargeCbtpZone11, 2L);
        // CBTP with zone scope 13
        Trader m5LargetCbtpZone13 = TestUtils.setAndGetCBTP(m5Large, "cbtp_m5Large", economy,
                false, zone13);
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
     * the resulting Quote contains the RegionId that has the lowest cost.
     */
    @Test
    public void testDatabaseTierComputeFunctionWithLicenseOnly() {
        final CostFunction cf = CostFunctionFactory.createCostFunction(TestUtils.setUpDatabaseTierCostDTO(
                TestUtils.LICENSE_COMM_BASE_TYPE,
                TestUtils.COUPON_COMM_BASE_TYPE,
                TestUtils.NO_TYPE));
        final MutableQuote quote = calculateCost(TestUtils.DC5_COMM_TYPE, cf);

        // Assert: the quote contains the cost for WINDOWS License and cheapest Region
        assertTrue(quote instanceof CommodityCloudQuote);
        assertTrue(quote.getContext().isPresent());
        assertEquals(TestUtils.DC1_COMM_TYPE, quote.getContext().get().getRegionId());
        assertEquals(1.5d, quote.getQuoteValue(), 0);
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
