package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.*;

import java.util.Arrays;
import org.junit.Test;

import com.vmturbo.platform.analysis.economy.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunction;

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
}
