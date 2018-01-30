package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.CostFunction;

public class CostFunctionTest {

    /**
     * Case: AWS IO1 storage tier as a seller. Storage amount unit price is 2, IOPS unit price is 10.
     * VM1 asks for 3GB, 90 IOPS, VM2 asks for 5GB, 500IOPS, VM3 asks for 10GB, 200IOPS
     * Expected result: VM1 infinity cost because it is less than min storage amount capacity
     * VM2 infinity cost because IOPS is more than max ratio of 50 storage amount,
     * VM3 cost is (10*0.125+200*0.065)
     */
    @Test
    public void testCostFunction_CalculateCost_AWSIO1CostFunction() {
        Economy economy = new Economy();
        Trader io1 = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        CostFunction io1Function = TestUtils.setUpIO1CostFunction();
        io1.getSettings().setCostFunction(io1Function);

        // create VM1 and get its cost from io1
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm1, new double[] {3, 90},
                        null);
        assertTrue(Double.isInfinite(io1Function.calculateCost(sl1, io1, true, economy)));

        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm2, new double[] {5, 500},
                        null);
        assertTrue(Double.isInfinite(io1Function.calculateCost(sl2, io1, true, economy)));

        // create VM3 and get its cost from io1
        Trader vm3 = TestUtils.createVM(economy);
        ShoppingList sl3 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT, TestUtils.IOPS), vm3,
                        new double[] {10, 200}, null);
        assertEquals((10 * 0.125 + 200 * 0.065),
                        io1Function.calculateCost(sl3, io1, true, economy), 0.0);
    }

    /**
     * Case: Azure premium managed storage tier as a seller. Storage amount price is 5.28 if 0~32GB,
     * 10.21 if 32~64 GB
     * VM1 asks for 0.5GB,  VM2 asks for 64GB
     * Expected result: VM1 infinity cost because it is less than min storage amount capacity
     * VM2 cost is 10.21
     */
    @Test
    public void testCostFunction_CalculateCost_AzurePremiumManagedCostFunction() {
        Economy economy = new Economy();
        Trader premiumManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        CostFunction premiumManagedFunction = TestUtils.setUpPremiumManagedCostFunction();
        premiumManaged.getSettings().setCostFunction(premiumManagedFunction);

        // create VM1 and get its cost
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[] {0.5}, null);
        assertTrue(Double.isInfinite(premiumManagedFunction.calculateCost(sl1, premiumManaged, true, economy)));

        // create VM2 and get its cost from io1
        Trader vm2 = TestUtils.createVM(economy);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm2, new double[] {64}, null);
        assertEquals(10.21, premiumManagedFunction.calculateCost(sl2, premiumManaged, true, economy),
                        0.0);
    }

    /**
     * Case: Azure standard unmanaged storage tier as a seller. Storage amount price is 0.05 per GB if 1~1024GB in LRS,
     * 0.10 per GB if 1024 ~ 50*1024 in LRS, VM1 asks for 2000GB
     * Expected result: VM1 cost is 0.05 * 1024 + 0.10 * (2000 - 1024)
     */
    @Test
    public void testCostFunction_CalculateCost_AzureStandardUnmanagedCostFunction() {
        Economy economy = new Economy();
        Trader standardUnManaged = TestUtils.createStorage(economy, Arrays.asList(0l), 4, false);
        CostFunction standardUnManagedFunction = TestUtils.setUpStandardUnmanagedCostFunction();
        standardUnManaged.getSettings().setCostFunction(standardUnManagedFunction);

        // create VM1 and get its cost
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.ST_AMT), vm1, new double[] {2000}, null);
        assertEquals(0.05 * 1024 + 0.10 * (2000 - 1024),
                        standardUnManagedFunction.calculateCost(sl1,
                        standardUnManaged, true, economy), 0);

    }
}
