package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Basket;
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
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.Quote.InitialInfiniteQuote;

public class CostFunctionTest {

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
        BalanceAccount ba = new BalanceAccount(100, 10000, 1);
        Trader io1 = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        io1.getSettings().setBalanceAccount(ba);
        CostFunction io1Function = TestUtils.setUpIO1CostFunction();
        io1.getSettings().setCostFunction(io1Function);

        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setBalanceAccount(ba);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm2, new double[] {5, 500},
                        null);
        assertEquals(Double.POSITIVE_INFINITY,
            io1Function.calculateCost(sl2, io1, true, economy).getQuoteValue(),
            TestUtils.FLOATING_POINT_DELTA);

        // create VM3 and get its cost from io1
        Trader vm3 = TestUtils.createVM(economy);
        vm3.getSettings().setBalanceAccount(ba);
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
        BalanceAccount ba = new BalanceAccount(100, 10000, 1);
        Trader gp2 = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        CostFunction gp2Function = TestUtils.setUpGP2CostFunction();
        gp2.getSettings().setCostFunction(gp2Function);

        // create VM2 and get its cost from io1
        Trader vm = TestUtils.createVM(economy);
        vm.getSettings().setBalanceAccount(ba);
        ShoppingList sl = TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm, new double[] {5, 120},
                null);
        assertTrue(Double.isInfinite(gp2Function.calculateCost(sl, gp2, true, economy).getQuoteValue()));

        // create VM3 and get its cost from io1
        Trader vm3 = TestUtils.createVM(economy);
        vm3.getSettings().setBalanceAccount(ba);
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
        BalanceAccount ba = new BalanceAccount(100, 10000, 1);
        Trader premiumManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        premiumManaged.getSettings().setBalanceAccount(ba);
        CostFunction premiumManagedFunction = TestUtils.setUpPremiumManagedCostFunction();
        premiumManaged.getSettings().setCostFunction(premiumManagedFunction);


        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setBalanceAccount(ba);
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
        BalanceAccount ba = new BalanceAccount(100, 10000, 1);
        Trader standardUnManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        standardUnManaged.getSettings().setBalanceAccount(ba);
        CostFunction standardUnManagedFunction = TestUtils.setUpStandardUnmanagedCostFunction();
        standardUnManaged.getSettings().setCostFunction(standardUnManagedFunction);

        // create VM1 and get its cost
        Trader vm1 = TestUtils.createVM(economy);
        vm1.getSettings().setBalanceAccount(ba);
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
        BalanceAccount ba = new BalanceAccount(100, 10000, 1);
        CommoditySpecification linuxComm =
                new CommoditySpecification(TestUtils.LINUX_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, 0, 100000, false);
        CommoditySpecification windowsComm =
                new CommoditySpecification(TestUtils.WINDOWS_COMM_TYPE, TestUtils.LICENSE_COMM_BASE_TYPE, 0, 100000, false);
        Trader t2Nano = economy.addTrader(1, TraderState.ACTIVE, new Basket(linuxComm, windowsComm),
                                          Arrays.asList(0l));
        t2Nano.getSettings().setBalanceAccount(ba);
        t2Nano.getCommoditiesSold().get(t2Nano.getBasketSold().indexOf(linuxComm)).setCapacity(1000);
        t2Nano.getCommoditiesSold().get(t2Nano.getBasketSold().indexOf(windowsComm)).setCapacity(1000);
        CostFunction t2NanoFunction = TestUtils.setUpT2NanoCostFunction();
        t2Nano.getSettings().setCostFunction(t2NanoFunction);

        Trader vm1 = TestUtils.createVM(economy);
        vm1.getSettings().setBalanceAccount(ba);
        Trader vm2 = TestUtils.createVM(economy);
        vm2.getSettings().setBalanceAccount(ba);
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
        double riDeprecationFactor = 0.0000001;
        double m5Large = 0.19200;
        double r4Large = 0.131600;
        double m4Large = 0.019200;
        double c4Large = 0.0131600;
        double t2Large = 0.12;
        double t3Large = 0.11;
        // Create a new Economy
        Economy economy = new Economy();

        // Create a VM buyer
        Trader vm = TestUtils.createVM(economy, "buyer");
        BalanceAccount ba = new BalanceAccount(0.0, 100000000d, 24);
        vm.getSettings().setBalanceAccount(ba);

        // Create CBTPs
        Trader cbtp1 = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU),new double[] {25, 3000},
                true, true, "cbtp");

        Trader cbtp2 = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU),new double[] {25, 3000},
                true, true, "cbtp2");

        cbtp1.getSettings().setBalanceAccount(ba);
        cbtp1.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());

        cbtp2.getSettings().setBalanceAccount(ba);
        cbtp2.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());

        CbtpCostDTO.Builder cbtpBundleBuilder = createCbtpBundleBuilder(0,m5Large*riDeprecationFactor,
                10 );
        CostDTO costDTOcbtp = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder.build()).build();
        CbtpCostDTO cdDTo = costDTOcbtp.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo));

        CbtpCostDTO.Builder cbtpBundleBuilder2 = createCbtpBundleBuilder(0, r4Large*riDeprecationFactor,
                50);
        CostDTO costDTOcbtp2 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder2.build()).build();
        CbtpCostDTO cdDTo2 = costDTOcbtp2.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo2));

        final InitialInfiniteQuote bestQuoteSoFar = new InitialInfiniteQuote();

        boolean forTraderIncomeStatement = true;

        // Create a Trader TP which serves as seller for cbtps
        Trader tp = TestUtils.createTrader(economy, TestUtils.VM_TYPE, Arrays.asList(0l),
                Arrays.asList(TestUtils.COUPON_COMMODITY, TestUtils.CPU, TestUtils.SEGMENTATION_COMMODITY),
                new double[] {1, 3000, 20}, true,
                true, "tp");

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
        CostDTO tpCostDto = CostDTO.newBuilder().setComputeTierCost(costBundleBuilder.addCostTupleList(CostTuple.newBuilder()
                .setLicenseCommodityType(-1)
                .setPrice(0.0341)
                .setBusinessAccountId(24)
                .build()).build()).build();

        tp.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        tp.getSettings().setCostFunction(CostFunctionFactory.createCostFunction(tpCostDto));

        // Create VM shopping lists
        ShoppingList vmSL = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.CPU),
                vm, new double[] {1000}, tp);

        // This is not really required but it helps to negate through various if conditions and go into the loop
        // where we use RI pricing as the differentiator between cbtps
        vmSL.setGroupFactor((long) 0.5);

        // Create shopping lists for cbtps to shop from the top
        ShoppingList cbtpSl = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), cbtp1, new double[] {25, 1}, tp);
        ShoppingList cbtpSl2 = TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(TestUtils.COUPON_COMMODITY,
                TestUtils.SEGMENTATION_COMMODITY), cbtp2, new double[] {25, 1}, tp);
        economy.populateMarketsWithSellers();

        // Test to check that cbtp2 gives a cheaper quote than cbtp1
        Assert.assertTrue(EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue() > EdeCommon.quote(economy, vmSL, cbtp2,
                bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue() );

        // Test to check if we scale the previous costs down by a factor of 10, we get the same quote
        CbtpCostDTO.Builder cbtpBundleBuilder3 = createCbtpBundleBuilder(0,m4Large*riDeprecationFactor,
                10 );

        CostDTO costDTOcbtp3 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder3.build()).build();
        CbtpCostDTO cdDTo3 = costDTOcbtp3.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo3));

        CbtpCostDTO.Builder cbtpBundleBuilder4 = createCbtpBundleBuilder(0, c4Large*riDeprecationFactor,
                50);

        CostDTO costDTOcbtp4 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder4.build()).build();
        CbtpCostDTO cdDTo4 = costDTOcbtp4.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo4));

        double quoteCbtp1 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        // Test to check that we get a quote of 1.0 from cbtp1
        assertEquals(quoteCbtp1, 1.0, TestUtils.FlOATING_POINT_DELTA2);

        double quoteCbtp2= EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(),
                forTraderIncomeStatement).getQuoteValue();
        // Test to check that quotes are equal ie both are 1.0
        assertEquals(quoteCbtp1, quoteCbtp2, TestUtils.FlOATING_POINT_DELTA2);

        // Set the price for a t2.large cbtp
        CbtpCostDTO.Builder cbtpBundleBuilder5 = createCbtpBundleBuilder(0,t2Large*riDeprecationFactor,
                10 );

        CostDTO costDTOcbtp5 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder5.build()).build();
        CbtpCostDTO cdDTo5 = costDTOcbtp5.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo5));

        // Set the price for a t3.large cbtp
        CbtpCostDTO.Builder cbtpBundleBuilder6 = createCbtpBundleBuilder(0, t3Large*riDeprecationFactor,
                50);

        CostDTO costDTOcbtp6 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder6.build()).build();
        CbtpCostDTO cdDTo6 = costDTOcbtp6.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo6));

        double quoteCbtp3 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        double quoteCbtp4 = EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // t2.large and t3.large give same quote
        assertEquals(quoteCbtp3, quoteCbtp4, TestUtils.FlOATING_POINT_DELTA2);

        // Now a test by changing budget on business account and increasing the riDeprecationFactor
        // to 10^-5 from 10^-7
        // Update the budget on balance account
        BalanceAccount ba2 = new BalanceAccount(0.0, 10000d, 24);
        cbtp1.getSettings().setBalanceAccount(ba2);
        cbtp2.getSettings().setBalanceAccount(ba2);

        double updatedriDprecationFactor = 0.00001;
        // Set the price for a t2.large cbtp with updated riDeprecationFactor
        CbtpCostDTO.Builder cbtpBundleBuilder7 = createCbtpBundleBuilder(0,t2Large*updatedriDprecationFactor,
                10 );

        CostDTO costDTOcbtp7 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder7.build()).build();
        CbtpCostDTO cdDTo7 = costDTOcbtp7.getCbtpResourceBundle();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo7));

        // Set the price for a t3.large cbtp with updated riDeprecationFactor
        CbtpCostDTO.Builder cbtpBundleBuilder8 = createCbtpBundleBuilder(0, t3Large*updatedriDprecationFactor,
                50);

        CostDTO costDTOcbtp8 = CostDTO.newBuilder().setCbtpResourceBundle(cbtpBundleBuilder8.build()).build();
        CbtpCostDTO cdDTo8 = costDTOcbtp8.getCbtpResourceBundle();
        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo8));

        double quoteCbtp5 = EdeCommon.quote(economy, vmSL, cbtp1, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();
        double quoteCbtp6 = EdeCommon.quote(economy, vmSL, cbtp2, bestQuoteSoFar.getQuoteValue(), forTraderIncomeStatement).getQuoteValue();

        // t2.large gives higher quote than t3.large which should be the case
        assert(quoteCbtp5 > quoteCbtp6);
    }

    /**
     * Create the cbtp bundle builder
     *
     * @param couponBaseType  Coupon value we want to set on the cbtp
     * @param price   The price of the RI we want to set on the cbtp
     * @param averageDiscount  the discount we want to set
     * @return  the CBTPCostDTO.Builder
     */
    public CbtpCostDTO.Builder createCbtpBundleBuilder(int couponBaseType, double price, double averageDiscount) {
        CbtpCostDTO.Builder cbtpBundleBuilder = CbtpCostDTO.newBuilder();
        cbtpBundleBuilder.setCouponBaseType(couponBaseType);
        cbtpBundleBuilder.setPrice(price);
        cbtpBundleBuilder.setDiscountPercentage(averageDiscount);
        return cbtpBundleBuilder;
    }
}
