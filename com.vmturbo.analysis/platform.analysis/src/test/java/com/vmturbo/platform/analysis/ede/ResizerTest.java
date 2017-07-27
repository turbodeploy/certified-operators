package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class ResizerTest {

    TestCommon testEconomy;

    @Before
    public void setUp() throws Exception {
        testEconomy = new TestCommon();
    }

    /**
     * In this scenario, the used value of VMem (150) sold by the VM is over capacity (100).
     * The VM is expected to resize to the used value. Resize decision depends solely on the
     * commodity sold however topology has to be setup for integration test.
     */
    @Test
    public void testResizeUpGreaterThanCapacity() {
        Economy economy = testEconomy.getEconomy();
        Trader vm = testEconomy.getVm();
        Trader[] pms = testEconomy.getPms();
        Trader pm1 = pms[0];
        Trader pm2 = pms[1];
        Trader [] stgs = testEconomy.getStgs();
        Trader st1 = stgs[0];
        Trader st2 = stgs[1];
        Trader app1 = testEconomy.getApps();

        ShoppingList[] shoppingLists = economy.getMarketsAsBuyer(vm)
                                            .keySet().toArray(new ShoppingList[3]);
        shoppingLists[0].setQuantity(0, 42);
        shoppingLists[0].setPeakQuantity(0, 42);
        shoppingLists[0].setQuantity(1, 150);
        shoppingLists[0].setPeakQuantity(1, 100);
        shoppingLists[0].setQuantity(2, 1);
        shoppingLists[0].setPeakQuantity(2, 1);
        shoppingLists[0].setQuantity(3, 1);
        shoppingLists[0].setPeakQuantity(3, 1);

        shoppingLists[1].setQuantity(0, 1000);
        shoppingLists[1].setPeakQuantity(0, 1000);
        shoppingLists[1].setQuantity(0, 1);
        shoppingLists[1].setPeakQuantity(0, 1);

        shoppingLists[2].setQuantity(0, 1000);
        shoppingLists[2].setPeakQuantity(0, 1000);
        shoppingLists[2].setQuantity(0, 1);
        shoppingLists[2].setPeakQuantity(0, 1);

        pm1.getCommoditySold(TestCommon.CPU).setCapacity(100);
        pm1.getCommoditySold(TestCommon.MEM).setCapacity(200);
        pm1.getCommoditySold(TestCommon.CPU).setQuantity(10);
        pm1.getCommoditySold(TestCommon.MEM).setQuantity(20);
        pm1.getCommoditySold(TestCommon.CPU).setPeakQuantity(10);
        pm1.getCommoditySold(TestCommon.MEM).setPeakQuantity(20);

        shoppingLists[0].move(pm1);
        shoppingLists[1].move(st1);
        shoppingLists[2].move(st2);

        economy.getCommodityBought(shoppingLists[0],TestCommon.CPU).setQuantity(42);
        economy.getCommodityBought(shoppingLists[0],TestCommon.MEM).setQuantity(150);

        ShoppingList[] appShoppingList = economy.getMarketsAsBuyer(app1)
                        .keySet().toArray(new ShoppingList[1]);
        appShoppingList[0].setQuantity(0, 42);
        appShoppingList[0].setPeakQuantity(0, 42);
        appShoppingList[0].setQuantity(1, 150);
        appShoppingList[0].setPeakQuantity(1, 100);

        vm.getCommoditySold(TestCommon.VMEM).setCapacity(100);
        vm.getCommoditySold(TestCommon.VCPU).setCapacity(100);
        vm.getCommoditySold(TestCommon.VMEM).setQuantity(150);
        vm.getCommoditySold(TestCommon.VCPU).setQuantity(42);
        vm.getCommoditySold(TestCommon.VMEM).setPeakQuantity(80);
        vm.getCommoditySold(TestCommon.VCPU).setPeakQuantity(42);

        economy.getCommodityBought(appShoppingList[0],TestCommon.VMEM).setQuantity(150);
        economy.getCommodityBought(appShoppingList[0],TestCommon.VCPU).setQuantity(42);

        appShoppingList[0].move(vm);

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(1, actions.size());
    }

    /**
     * In this scenario, the used value of VMem (90) sold by the VM is less than capacity (100).
     * The max utilization is configured to 80. The VM is expected to resize to the used value.
     * Resize decision depends solely on the commodity sold however topology has to be setup for
     * integration test.
     */
    @Test
    public void testResizeUpLessThanCapacity() {
        Economy economy = testEconomy.getEconomy();
        Trader vm = testEconomy.getVm();
        Trader[] pms = testEconomy.getPms();
        Trader pm1 = pms[0];
        Trader pm2 = pms[1];
        Trader [] stgs = testEconomy.getStgs();
        Trader st1 = stgs[0];
        Trader st2 = stgs[1];
        Trader app1 = testEconomy.getApps();

        ShoppingList[] shoppingLists = economy.getMarketsAsBuyer(vm)
                                            .keySet().toArray(new ShoppingList[3]);
        shoppingLists[0].setQuantity(0, 42);
        shoppingLists[0].setPeakQuantity(0, 42);
        shoppingLists[0].setQuantity(1, 150);
        shoppingLists[0].setPeakQuantity(1, 100);
        shoppingLists[0].setQuantity(2, 1);
        shoppingLists[0].setPeakQuantity(2, 1);
        shoppingLists[0].setQuantity(3, 1);
        shoppingLists[0].setPeakQuantity(3, 1);

        shoppingLists[1].setQuantity(0, 1000);
        shoppingLists[1].setPeakQuantity(0, 1000);
        shoppingLists[1].setQuantity(0, 1);
        shoppingLists[1].setPeakQuantity(0, 1);

        shoppingLists[2].setQuantity(0, 1000);
        shoppingLists[2].setPeakQuantity(0, 1000);
        shoppingLists[2].setQuantity(0, 1);
        shoppingLists[2].setPeakQuantity(0, 1);

        pm1.getCommoditySold(TestCommon.CPU).setCapacity(100);
        pm1.getCommoditySold(TestCommon.MEM).setCapacity(200);
        pm1.getCommoditySold(TestCommon.CPU).setQuantity(10);
        pm1.getCommoditySold(TestCommon.MEM).setQuantity(90);
        pm1.getCommoditySold(TestCommon.CPU).setPeakQuantity(10);
        pm1.getCommoditySold(TestCommon.MEM).setPeakQuantity(90);

        shoppingLists[0].move(pm1);
        shoppingLists[1].move(st1);
        shoppingLists[2].move(st2);

        economy.getCommodityBought(shoppingLists[0],TestCommon.CPU).setQuantity(42);
        economy.getCommodityBought(shoppingLists[0],TestCommon.MEM).setQuantity(150);

        ShoppingList[] appShoppingList = economy.getMarketsAsBuyer(app1)
                        .keySet().toArray(new ShoppingList[1]);
        appShoppingList[0].setQuantity(0, 40);
        appShoppingList[0].setPeakQuantity(0, 40);
        appShoppingList[0].setQuantity(1, 150);
        appShoppingList[0].setPeakQuantity(1, 150);

        vm.getSettings().setMaxDesiredUtil(0.80);
        vm.getCommoditySold(TestCommon.VMEM).setCapacity(100);
        vm.getCommoditySold(TestCommon.VCPU).setCapacity(100);
        vm.getCommoditySold(TestCommon.VMEM).setQuantity(90);
        vm.getCommoditySold(TestCommon.VCPU).setQuantity(40);
        vm.getCommoditySold(TestCommon.VMEM).setPeakQuantity(90);
        vm.getCommoditySold(TestCommon.VCPU).setPeakQuantity(40);

        economy.getCommodityBought(appShoppingList[0],TestCommon.VMEM).setQuantity(90);
        economy.getCommodityBought(appShoppingList[0],TestCommon.VCPU).setQuantity(40);

        appShoppingList[0].move(vm);

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(1, actions.size());
    }

}
