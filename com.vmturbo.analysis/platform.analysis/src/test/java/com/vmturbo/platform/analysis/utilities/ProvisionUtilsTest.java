package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

public class ProvisionUtilsTest {

    /**
     * Test cases where buyer fits in seller and buyer does not fit in seller.
     */
    @Test
    public void test_CanBuyerFitInSeller(){
        Economy economy = new Economy();
        // create one pm with smaller capacity than VM requirement
        Trader pm1 = economy.addTrader(TestUtils.PM_TYPE, TraderState.ACTIVE, new Basket(TestUtils.CPU, TestUtils.MEM),
                new HashSet<>(Arrays.asList(0l)));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(TestUtils.CPU)).setCapacity(40);
        Trader vm1 = economy.addTrader(TestUtils.VM_TYPE, TraderState.ACTIVE, new Basket());
        ShoppingList sl1 = economy.addBasketBought(vm1, new Basket(TestUtils.CPU, TestUtils.MEM));
        //Buyer does not fit in seller
        sl1.setQuantity(sl1.getBasket().indexOf(TestUtils.CPU), 50);
        assertFalse(ProvisionUtils.canBuyerFitInSeller(sl1, pm1, economy));
        //Buyer fits in seller
        sl1.setQuantity(sl1.getBasket().indexOf(TestUtils.CPU), 30);
        assertTrue(ProvisionUtils.canBuyerFitInSeller(sl1, pm1, economy));
    }
}