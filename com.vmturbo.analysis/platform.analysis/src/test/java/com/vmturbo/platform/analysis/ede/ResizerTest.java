package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class ResizerTest {

    TestCommon testEconomy;
    Trader app, app1, app2, app3;
    Trader appserver1, appserver2, dbserver1;
    Trader vm, vm1, vm2;
    Trader cont, pod;
    Trader pm;
    Trader namespace;
    Ledger ledger;
    public static final Set<CommoditySpecification> EXPECTED_COMM_SPECS_TO_BE_RESIZED =
                    Collections.unmodifiableSet(
                                    new HashSet<CommoditySpecification>(
                                                    Arrays.asList(TestUtils.VCPU, TestUtils.VMEM)));
    private static final double RIGHT_SIZE_LOWER = 0.3;
    private static final double RIGHT_SIZE_UPPER = 0.7;

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
        Trader[] stgs = testEconomy.getStgs();
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

        economy.getCommodityBought(shoppingLists[0], TestCommon.CPU).setQuantity(42);
        economy.getCommodityBought(shoppingLists[0], TestCommon.MEM).setQuantity(150);

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

        economy.getCommodityBought(appShoppingList[0], TestCommon.VMEM).setQuantity(150);
        economy.getCommodityBought(appShoppingList[0], TestCommon.VCPU).setQuantity(42);

        appShoppingList[0].move(vm);

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(1, actions.size());
    }

    /**
     * Tests that a VM doesn't scale up its commodity capacity greater than the capacity that
     * its provider has.
     */
    @Test
    public void testNoResizeUpGreaterThanProviderCapacity() {
        Economy economy = testEconomy.getEconomy();
        Trader vm = testEconomy.getVm();
        Trader[] pms = testEconomy.getPms();
        Trader pm1 = pms[0];

        ShoppingList[] shoppingLists = economy.getMarketsAsBuyer(vm)
                                            .keySet().toArray(new ShoppingList[1]);
        ShoppingList sl = shoppingLists[0];
        sl.setQuantity(0, 90).setPeakQuantity(0, 90);

        pm1.getCommoditySold(TestCommon.CPU).setCapacity(100).setQuantity(40).setPeakQuantity(40);

        shoppingLists[0].move(pm1);

        economy.getSettings().setRightSizeLower(0.3).setRightSizeUpper(0.7);
        economy.getCommodityBought(sl, TestCommon.CPU).setQuantity(40).setPeakQuantity(40);

        vm.getCommoditySold(TestCommon.VCPU).setCapacity(100).setQuantity(90).setPeakQuantity(90);
        vm.getCommoditySold(TestCommon.VCPU).getSettings().setCapacityIncrement(1);
        vm.getSettings().setMinDesiredUtil(0.6).setMaxDesiredUtil(0.7);

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(0, actions.size());
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
        Trader[] stgs = testEconomy.getStgs();
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

        economy.getCommodityBought(shoppingLists[0], TestCommon.CPU).setQuantity(42);
        economy.getCommodityBought(shoppingLists[0], TestCommon.MEM).setQuantity(150);

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

        economy.getCommodityBought(appShoppingList[0], TestCommon.VMEM).setQuantity(90);
        economy.getCommodityBought(appShoppingList[0], TestCommon.VCPU).setQuantity(40);

        appShoppingList[0].move(vm);

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(1, actions.size());
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * So, the VM's VCPU and VMEM have low ROI.
     * CommodityResizeDependencyMap is setup such that an increase in VCPU
     * leads to increase in CPU and vice versa.
     * Increase in VMEM leads to increase in MEM and vice versa.
     * Expected result: Resize down the VM's VCPU. Quantity of CPU and MEM
     * used in the PM also decreases.
     */
    @Test
    public void testResizeDecisions_resizeDownWithDependencyForPlan() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 10, 70, 10, 10, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        final double cpuUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        final double memUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        // setting historicalQnty on the VM.
        for (CommoditySold cs : vm.getCommoditiesSold()) {
            cs.setHistoricalQuantity(cs.getQuantity());
        }
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        //Assert that VCPU and VMEM of VM were resized down.
        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertTrue(resize1.getOldCapacity() > resize1.getNewCapacity());
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertTrue(resize2.getOldCapacity() > resize2.getNewCapacity());
        Set<CommoditySpecification> actualCommSpecsResized = new HashSet<>(
                        Arrays.asList(resize1.getResizedCommoditySpec(),
                                        resize2.getResizedCommoditySpec()));
        assertEquals(EXPECTED_COMM_SPECS_TO_BE_RESIZED, actualCommSpecsResized);
        //Check that the quantites of the dependent commodities changed appropriately.
        double cpuUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        // when the usage on the underlying provider is lower than the newCap, we should not increase the usage
        assertEquals(cpuUsedOnCommSoldBeforeResize, cpuUsedOnCommSoldAfterResize, 0);
        double memUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        assertTrue(memUsedOnCommSoldBeforeResize > memUsedOnCommSoldAfterResize);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * So, the VM's VCPU and VMEM have low ROI.
     * CommodityResizeDependencyMap is setup such that an increase in VCPU
     * leads to increase in CPU and vice versa.
     * Increase in VMEM leads to increase in MEM and vice versa.
     * Expected result: Resize down the VM's VCPU. Quantity of CPU and MEM
     * used in the PM does not change.
     */
    @Test
    public void testResizeDecisions_resizeDownWithDependencyForRT() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                100, 100, 70, 70, 20, 20, 0.65, 0.8,
                RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        final double cpuUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        final double memUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        economy.getSettings().setResizeDependentCommodities(false);
        TestUtils.setupHistoryBasedResizeDependencyMap(economy);
        // setting historicalQnty on the VM.
        for (CommoditySold cs : vm.getCommoditiesSold()) {
            cs.setHistoricalQuantity(cs.getQuantity());
        }

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        //Assert that VCPU and VMEM of VM were resized down.
        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertTrue(resize1.getOldCapacity() > resize1.getNewCapacity());
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertTrue(resize2.getOldCapacity() > resize2.getNewCapacity());
        Set<CommoditySpecification> actualCommSpecsResized = new HashSet<>(
                Arrays.asList(resize1.getResizedCommoditySpec(),
                        resize2.getResizedCommoditySpec()));
        assertEquals(EXPECTED_COMM_SPECS_TO_BE_RESIZED, actualCommSpecsResized);
        //Check that the quantites of the dependent commodities did not decrease.
        double cpuUsedOnCommSoldAfterResize =
                pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        assertEquals(cpuUsedOnCommSoldBeforeResize, cpuUsedOnCommSoldAfterResize, 0);
        double memUsedOnCommSoldAfterResize =
                pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        assertEquals(memUsedOnCommSoldBeforeResize, memUsedOnCommSoldAfterResize, 0);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 200, VM buys 40 from it. App buys 80 of VM's VCPU.
     * PM MEM capacity = 200, VM buys 40 from it. App buys 80 of VM's VMEM.
     * So, the VM's VCPU and VMEM have high ROI.
     * CommodityResizeDependencyMap is setup such that an increase in VCPU
     * leads to increase in CPU and vice versa.
     * Increase in VMEM leads to increase in MEM and vice versa.
     * Expected result: Resize up the VM. Quantity of CPU and MEM used
     * in the PM also increases.
     */
    @Test
    public void testResizeDecisions_resizeUpWithDependency() {
        Economy economy = setupTopologyForResizeTest(200, 200,
                        100, 100, 40, 40, 80, 80, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        final double cpuUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        final double memUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        //Assert that VCPU and VMEM of VM1 were resized up.
        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertTrue(resize1.getOldCapacity() < resize1.getNewCapacity());
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertTrue(resize2.getOldCapacity() < resize2.getNewCapacity());
        Set<CommoditySpecification> actualCommSpecsResized = new HashSet<>(
                        Arrays.asList(resize1.getResizedCommoditySpec(),
                                        resize2.getResizedCommoditySpec()));
        assertEquals(EXPECTED_COMM_SPECS_TO_BE_RESIZED, actualCommSpecsResized);
        //Check that the quantites of the dependent commodities increased.
        double cpuUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        assertTrue(cpuUsedOnCommSoldBeforeResize < cpuUsedOnCommSoldAfterResize);
        double memUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        assertTrue(memUsedOnCommSoldBeforeResize < memUsedOnCommSoldAfterResize);
    }

    /**
     * Setup economy with 1 PM, 2 VMs and 2 applications.
     * PM CPU capacity = 100, each VM has capacity 50 and buys 40 from it. Each app buys 49 of VM's VCPU.
     * PM MEM capacity = 100, each VM has capacity 50 and buys 40 from it. Each app buys 49 of VM's VMEM.
     * So, the VM's VCPU and VMEM have high ROI.
     * The desired capacity increase is 71 units - result of calling calculateDesiredCapacity.
     * But, the increase in memory and CPU will be limited to oldCapacity + 20.
     * (20 is the remaining capacity => PM MEM capacity - PM MEM quantity. Same for CPU.).
     */
    @Test
    public void testResizeDecisions_resizeUpGreaterThanUnderlyingProviderAllows() {
        Economy economy = setupTopologyForResizeTestAlternative(100, 100,
                        50, 50, 50, 50, 40, 40, 40, 40, 49, 49, 49, 49, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, false);
        vm1.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(40));
        vm2.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(40));

        vm1.getCommoditiesSold().stream().forEach(c -> c.getSettings().setUtilizationUpperBound(0.5));
        vm2.getCommoditiesSold().stream().forEach(c -> c.getSettings().setUtilizationUpperBound(0.5));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        //Assert that VCPU and VMEM of VM were resized up
        //and limited to the remaining capacity of underlying PM.
        assertEquals(4, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm1);
        assertTrue(resize1.getOldCapacity() < resize1.getNewCapacity());
        double estimatedNewCapacity = resize1.getOldCapacity() + 20;
        assertEquals(resize1.getNewCapacity(), estimatedNewCapacity, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm1);
        assertTrue(resize2.getOldCapacity() < resize2.getNewCapacity());
        estimatedNewCapacity = resize2.getOldCapacity() + 20;
        assertEquals(resize2.getNewCapacity(), estimatedNewCapacity, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(2).getType());
        Resize resize3 = (Resize)actions.get(2);
        assertEquals(resize3.getActionTarget(), vm2);
        assertTrue(resize3.getOldCapacity() < resize2.getNewCapacity());
        estimatedNewCapacity = resize3.getOldCapacity() + 20;
        assertEquals(resize3.getNewCapacity(), estimatedNewCapacity, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(3).getType());
        Resize resize4 = (Resize)actions.get(3);
        assertEquals(resize4.getActionTarget(), vm2);
        assertTrue(resize4.getOldCapacity() < resize2.getNewCapacity());
        estimatedNewCapacity = resize4.getOldCapacity() + 20;
        assertEquals(resize4.getNewCapacity(), estimatedNewCapacity, TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's CPU and MEM have maxQuantity of 90.
     * The desired capacity is 25.85 - result of calling calculateDesiredCapacity.
     * But, the decrease in memory and CPU will be limited to oldCapacity - 10.
     * (10 comes from VM VMEM capacity - VM VMEM maxQuantity. Same for VCPU).
     */
    @Test
    public void testResizeDecisions_resizeDownLowerThanMaxQuantity() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals("new capacity (" + resize1.getNewCapacity() + ") cannot resize below max quantity of 90",
            resize1.getNewCapacity(), resize1.getOldCapacity() - 10, TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals("new capacity (" + resize1.getNewCapacity() + ") cannot resize below max quantity of 90",
            resize2.getNewCapacity(),  resize2.getOldCapacity() - 10, TestUtils.FLOATING_POINT_DELTA);

    }

    /**
     * When historical and max quantity or both set, resize should go below max quantity, but not
     * below historical quantity.
     */
    @Test
    public void testResizeDownLowerThanMaxQuantityAndHistoricalUsage() {
        final float maxQuantity = 90;
        final float historicalQuantity = 25;
        Economy economy = setupTopologyForResizeTest(100, 100,
            100, 100, 1, 1, 1, 1, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(maxQuantity));
        vm.getCommoditiesSold().stream().forEach(c -> c.setHistoricalQuantity(historicalQuantity));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(resize1.getOldCapacity(),  100, TestUtils.FLOATING_POINT_DELTA);
        assertTrue("new capacity (" + resize1.getNewCapacity() + ") should resize below max quantity (" + maxQuantity + ").",
            resize1.getNewCapacity() < 90);
        assertTrue("new capacity (" + resize1.getNewCapacity() + ") should resize above historical quantity (" + historicalQuantity + ")",
            resize1.getNewCapacity() >= 25);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(resize2.getOldCapacity(),  100, TestUtils.FLOATING_POINT_DELTA);
        assertTrue("new capacity (" + resize2.getNewCapacity() + ") should resize below max quantity (" + maxQuantity + ").",
            resize2.getNewCapacity() < 90);
        assertTrue("new capacity (" + resize2.getNewCapacity() + ") should resize above historical quantity (" + historicalQuantity + ")",
            resize1.getNewCapacity() >= 25);
    }

    /**
     * We want to make sure that when rate of resize is set to the lowest setting (1) in UI which sets
     * the value internally to (10^10) so only 1 increment will be done for an action, we still
     * provide the actions during resize down.
     */
    @Test
    public void testResizeDecisions_resizeLowRateOfResize() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        economy.getSettings().setDefaultRateOfResize((float)Math.pow(10, 10));
        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(2, actions.size());
    }

    /**
     * Setup economy with one PM, one VM and one application,
     * and VM's commodities sold have high ROI.
     * But VM's commodities sold are not resizable.
     * Expected result: No actions are generated.
     */
    @Test
    public void testResizeDecisions_noActionsWhenNoResizableCommodities() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 40, 40, 80, 80, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, false);
        vm.getCommoditiesSold().stream()
            .forEach(c -> c.getSettings().setResizable(false));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertTrue(actions.isEmpty());
    }

    /**
     * Setup economy with one PM, one VM and one application,
     * and VM has its commodities sold with utilization
     * in acceptable range - between rightSizeLower (0.3) and rightSizeUpper(0.7).
     * Expected result: No actions.
     */
    @Test
    public void testResizeDecisions_noActionsWhenCommodityUtilizationBetweenRightSizeLowerAndUpper() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 40, 40, 40, 40, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, false);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertTrue(actions.isEmpty());
    }

    /**
     * Setup economy with one PM, one VM and one application,
     * and VM has its commodities sold with low ROI.
     * VM is not eligible for resize down.
     * Expected result: No resize actions are generated.
     */
    @Test
    public void testResizeDecisions_noActionsWhenTraderNotEligibleForResize() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 70, 70, 20, 20, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, false);
        //VM is not eligible for resize down
        vm.getSettings().setIsEligibleForResizeDown(false);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertTrue(actions.isEmpty());
    }

    /**
     * Setup economy with one PM, one VM and one application,
     * and VM has its commodities sold with low ROI.
     * VM is inactive.
     * Expected result: No resize actions are generated.
     */
    @Test
    public void testResizeDecisions_noActionsWhenTraderInactive() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 70, 70, 20, 20, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, false);
        //VM is not eligible for resize down
        vm.changeState(TraderState.INACTIVE);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertTrue(actions.isEmpty());
    }

    /**
     * Regression test for OM-40189: downward resize can exceed seller's capacity.
     *
     * <p>Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 150 each, which exceeds PM's capacity.
     * VM's CPU and MEM have maxQuantity of 120.
     * The capacity increment is 1.
     * The desired capacity is 25.92 - result of calling calculateDesiredCapacity.
     * That capacity will be bumped to 120 because of the maxQuantity value.
     * But in the end, we have no action because resize value exceeds sellers's capacity.
     * */
    @Test
    public void testResizeDecisions_resizeDownExceedsSellerCapacity() {
        Economy economy = setupTopologyForResizeTest(100, 100,
            150, 150, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(120));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 150 each, which exceeds PM's capacity.
     * VM's CPU and MEM have maxQuantity of 90.
     * The capacity increment is 1.
     * The desired capacity is 25.92 - result of calling calculateDesiredCapacity.
     * That capacity will be bumped to 90 because of the maxQuantity value.
     * We have 2 actions, because we don't exceed the raw material capacity.
     * */
    @Test
    public void testResizeDecisions_resizeDownDoesNotExceedSellerCapacity() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                150, 150, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(90, resize1.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(90, resize2.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 150 each, which exceeds PM's capacity.
     * VM's CPU and MEM have capacity lower bound of 90.
     * The capacity increment is 1.
     * The desired capacity is 25.92 - result of calling calculateDesiredCapacity.
     * That capacity will be bumped to 90 because of the capacity lower bound value.
     * */
    @Test
    public void testResizeDownRespectsCapacityLowerBound() {
        Economy economy = setupTopologyForResizeTest(100, 100,
            150, 150, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityLowerBound(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(90, resize1.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(90, resize2.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 90 each.
     * VM's CPU and MEM have lower bound of 90.
     * The capacity increment is 1.
     * If the current capacities is already at the lower bound, then we
     * should not resize down any further.
     * */
    @Test
    public void testDoNotResizeDownCurrentCapacityBelowCapacityLowerBound() {
        Economy economy = setupTopologyForResizeTest(100, 100,
            90, 90, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityLowerBound(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 70 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 70 of VM's VMEM.
     * VM's VCPU and VMEM have capacities of 80 each.
     * The capacity increment is 1.
     * The desired capacity is 90.72 - result of calling calculateDesiredCapacity.
     * That capacity increase will be limited to 85 because of the capacity upper bound value.
     * */
    @Test
    public void testResizeUpRespectsCapacityUpperBound() {
        Economy economy = setupTopologyForResizeTest(100, 100,
            80, 80, 70, 70, 70, 70, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityUpperBound(85));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(85, resize1.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(85, resize2.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 70 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 70 of VM's VMEM.
     * VM's VCPU and VMEM have capacities of 80 each.
     * The capacity increment is 1.
     * That capacity upper bound value is 75. If the current capacity is already above
     * the upper bound, then the upper bound does not matter. And we will allow the generation
     * of actions.
     * */
    @Test
    public void testResizeUpWhenCapacityGreaterThanUpperBound() {
        Economy economy = setupTopologyForResizeTest(100, 100,
            80, 80, 70, 70, 70, 70, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityUpperBound(75));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
    }

    /**
     * Resize down when the currentCapacity is above rawMaterial capacity.
     *
     * <p>Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 150 each, which exceeds PM's capacity.
     * The desired capacity is 26.66 - result of calling calculateDesiredCapacity.
     * The capacity increment is 30.
     * The rate of resize is set to low and so capacity is only decremented by one decrement to make
     * the newCapacity 120.
     * But this is still above the rawMaterial capacity of 100. So we end up with no action
     * */
    @Test
    public void testResizeDecisions_resizeDownExceedsSellerCapacityWithCapacityIncrement() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                150, 150, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityIncrement(30));
        // set the rate of resize to low
        economy.getSettings().setDefaultRateOfResize(1000000);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    /**
     * Resize down when the currentCapacity is above rawMaterial capacity.
     *
     * <p>Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's VCPU and VMEM have capacities of 150 each, which exceeds PM's capacity.
     * The desired capacity is 26.66 - result of calling calculateDesiredCapacity.
     * The capacity increment is 30.
     * The rate of resize is set to high and so capacity is decremented by four decrements to make
     * the newCapacity 30, the least possible above the desired capacity in order to have an
     * integer number of decrements.
     * */
    @Test
    public void testResizeDecisions_resizeDownDoesNotExceedSellerCapacityWithCapacityIncrement() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                150, 150, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityIncrement(30));
        // set the rate of resize to high
        economy.getSettings().setDefaultRateOfResize(1);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(30, resize1.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(30, resize2.getNewCapacity(), TestUtils.FLOATING_POINT_DELTA);
    }

    /**
     * Resize down when the currentCapacity is above rawMaterial capacity. This test has a really
     * big increment greater than the capacity of the raw material. Its an extreme case.
     *
     * <p>Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 70 from it. App buys 20 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 70 from it. App buys 20 of VM's VMEM.
     * VM's VMEM and VCPU have low ROI.
     * VM's CPU and MEM have capacities of 1500 each, which exceeds PM's capacity.
     * The desired capacity is 40 - result of calling calculateDesiredCapacity.
     * The capcity increment is 500.
     * The rate of resize is set to low and so capacity is only decremented by one decrement to make
     * the newCapacity 1000.
     * But this is still above the rawMaterial capacity of 100. So we try to decrease by
     * Match.ceil((1500 - 100)/500) = 3 decrements to make the newCapacity 0.
     * But we should not size down to 0, and so we don't produce an action.
     * */
    @Test
    public void testResizeDecisions_noActionsWhenResizeDownExceedsSellerCapacityWithBigCapacityIncrement() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                1500, 1500, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.getSettings().setCapacityIncrement(500));
        // set the rate of resize to low
        economy.getSettings().setDefaultRateOfResize(1000000);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    /**
     * When we try to resize up a VM when its current capacity is already above the raw material's
     * capacity, then we should not produce an action.
     **/
    @Test
    public void testResizeDecisions_resizeUpWhenCurrentCapacityGreaterThanRawMaterialCapacity() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                101, 101, 80, 80,
                95, 95, 0.65, 0.75,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: ResizeDecisionsForContainersTest({0}, {1}, {2}, {3}, {4}, {5})")
    public final void testResizeDecisionsForContainers(double nsAndPodVmemLimitCapacity,
                                                       double contVmemCapacity,
                                                       double vmemUsedByApp,
                                                       double vmemUsedByCont,
                                                       double vmemLimitUsedByPod,
                                                       double numActions,
                                                       double up) {
        Economy economy = new Economy();
        setupContainerTopologyForResizeTest(economy, nsAndPodVmemLimitCapacity, contVmemCapacity,
                vmemUsedByApp, vmemUsedByCont, vmemLimitUsedByPod, 0.65, 0.75,
                RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertTrue(numActions == actions.size());
        if (numActions == 1) {
            double change = ((Resize) actions.get(0)).getNewCapacity() - ((Resize) actions.get(0)).getOldCapacity();
            assertTrue(change * up > 0);
            // make sure the namespaceQnty increased by the change observed while resizing up
            if (up == 1) {
                assertEquals(change, namespace.getCommoditiesSold().get(0).getQuantity() - vmemLimitUsedByPod, 0);
            } else if (up == -1) {
                assertEquals(((Resize) actions.get(0)).getNewCapacity(), namespace.getCommoditiesSold().get(0).getQuantity(), 0);
            }
        }
    }

    /**
    * Create SG1 SG2 containing 2 containers each. The usages of the containers are such that they all want to resize UP.
    * There is a Namespace capacity of 100 units and a usage of 80 units.
    *
    * Evaluate that the total increase in capacity is less than the available headroom on the Namespace.
    **/
    @Test
    public final void testConsistentResizeDecisionsForContainers() {
        Economy economy = new Economy();
        double namespaceCapacity = 100;
        double namespaceUsage = 80;
        setupContainerTopologyForResizeTest(economy, namespaceCapacity, 80,
                70, 80, namespaceUsage, 0.65, 0.75,
                RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER);
        Trader container2 = setupContainerAndApp(economy, 80, 70, 80, 0.65, 0.75, 2);
        cont.setScalingGroupId("SG1");
        container2.setScalingGroupId("SG1");
        economy.populatePeerMembersForScalingGroup(cont, "SG1");
        economy.populatePeerMembersForScalingGroup(container2, "SG1");
        Trader container3 = setupContainerAndApp(economy, 80, 70, 80, 0.65, 0.75, 3);
        Trader container4 = setupContainerAndApp(economy, 80, 70, 80, 0.65, 0.75, 4);
        container3.setScalingGroupId("SG2");
        container4.setScalingGroupId("SG2");
        economy.populatePeerMembersForScalingGroup(container3, "SG2");
        economy.populatePeerMembersForScalingGroup(container4, "SG2");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        double totalIncrease = actions.stream().filter(Resize.class::isInstance)
                .map(Resize.class::cast)
                .mapToDouble(r -> r.getNewCapacity() - r.getOldCapacity())
                .sum();
        assertTrue(totalIncrease <= namespaceCapacity - namespaceUsage);
    }

    /**
     * Create SG1 containing 2 containers. The usages of the containers are such that they all want to resize DOWN.
     *
     * Evaluate that the total increase in capacity is less than the available headroom on the Namespace.
     **/
    @Test
    public final void testConsistentResizeDownDecisionsForContainers() {
        Economy economy = new Economy();
        double namespaceCapacity = 102400; // 100GB
        double namespaceUsage = 1536; // 1.5GB
        setupContainerTopologyForResizeTest(economy, namespaceCapacity, 1024 /*1GB*/,
                49 /*49MB*/, 1024 /*1GB*/, namespaceUsage, 0.65, 0.75,
                RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER);
        Trader container2 = setupContainerAndApp(economy, 512 /*0.5GB*/, 30 /*30MB*/, 512 /*0.5GB*/, 0.65, 0.75, 2);
        cont.setScalingGroupId("SG1");
        container2.setScalingGroupId("SG1");
        economy.populatePeerMembersForScalingGroup(cont, "SG1");
        economy.populatePeerMembersForScalingGroup(container2, "SG1");
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        double totalDecrease = actions.stream().filter(Resize.class::isInstance)
                .map(Resize.class::cast)
                .mapToDouble(r -> r.getOldCapacity() - r.getNewCapacity())
                .sum();

        assertTrue(actions.stream().filter(Resize.class::isInstance).allMatch(a -> ((Resize) a).getNewCapacity()%10 == 0));
        assertEquals(totalDecrease, namespaceUsage - namespace.getCommoditiesSold().get(0).getQuantity(), 0);

    }

    private Trader setupContainerAndApp(Economy economy,
                                       double contVmemCapacity,
                                       double vmemUsedByApp,
                                       double vmemUsedByCont,
                                       double minDesiredUtil, double maxDesiredUtil, int index) {
        Trader container = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VMEM),
                new double[]{contVmemCapacity}, false, false);
        container.setDebugInfoNeverUseInCode("CONTAINER" + index);
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEMLIMITQUOTA), container,
                new double[]{vmemUsedByCont}, pod).setMovable(false);
        //Create app and place on container
        Trader application = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        application.setDebugInfoNeverUseInCode("APP" + index);
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEM), application,
                new double[]{vmemUsedByApp}, container);
        container.getSettings().setMinDesiredUtil(minDesiredUtil).setMaxDesiredUtil(maxDesiredUtil);
        return container;
    }

    /**
     * Make sure that a single container part of a scalingGroup will not be consistently scaled.
     **/
    @Test
    public final void testIsSingleContainerNotConsistentlyScaled() {
        Economy economy = new Economy();
        cont = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE,
                Arrays.asList(0L), Collections.EMPTY_LIST,
                new double[]{}, false, false);
        cont.setScalingGroupId("sg1");
        economy.populatePeerMembersForScalingGroup(cont, "sg1");
        // make sure single container is not treated as part of a scalingGp
        assertFalse(cont.isInScalingGroup(economy));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestResizeDecisionsForContainers() {
        return new Object[][] {
                // no resize up even with high usage on container because the container is over VM capacity
                {100, 101, 101, 80, 80, 0, 0},
                // high usage on container and hence resize up
                {100, 80, 70, 80, 80, 1, 1},
                // low usage on container and hence resize down
                {100, 80, 20, 80, 80, 1, -1},
                // high usage on container but cant resize up because of unavailability of capacity on VM
                {100, 80, 70, 80, 97, 0, 0}
        };
    }

    /**
     * Sets up topology with one PM, one VM placed on the PM, and one app placed on the VM.
     * @param pmCpuCapacity - The PM's CPU capacity
     * @param pmMemCapacity - The PM's Memory capacity
     * @param vmVcpuCapacity - The VM's VCPU capacity
     * @param vmVmemCapacity - The VM's VMEM capacity
     * @param cpuUsedByVm - The quantity of CPU used by the VM
     * @param memUsedByVm - The quantity of MEM used by the VM
     * @param vcpuUsedByApp - The quantity of VCPU used by the app.
     * @param vmemUsedByApp - The quantity of the VMEM used by the app.
     * @param vmMinDesiredUtil - The VM's minimum desired utilization.
     * @param vmMaxDesiredUtil - The VM's maximum desired utilization.
     * @param economyRightSizeLower - Economy's right size lower limit
     * @param economyRightSizeUpper - Economy's right size upper limit
     * @param shouldSetupCommodityResizeDependencyMap -
     *        should the commodity resize dependency map
     *        be setup for the economy passed in.
     * @return Economy with the topology setup.
     */
    private Economy setupTopologyForResizeTest(
                    double pmCpuCapacity, double pmMemCapacity,
                    double vmVcpuCapacity, double vmVmemCapacity,
                    double cpuUsedByVm, double memUsedByVm,
                    double vcpuUsedByApp, double vmemUsedByApp,
                    double vmMinDesiredUtil, double vmMaxDesiredUtil,
                    double economyRightSizeLower, double economyRightSizeUpper,
                    boolean shouldSetupCommodityResizeDependencyMap) {
        Economy economy = new Economy();
        Topology topo = new Topology();
        economy.setTopology(topo);
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM),
                        new double[]{pmCpuCapacity, pmMemCapacity}, true, false);
        pm.setDebugInfoNeverUseInCode("PM1");
        // Create VM and place on PM
        vm = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                        Arrays.asList(0L), Arrays.asList(TestUtils.VCPU, TestUtils.VMEM),
                        new double[]{vmVcpuCapacity, vmVmemCapacity}, false, false);
        vm.setDebugInfoNeverUseInCode("VM1");
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm,
                        new double[]{cpuUsedByVm, memUsedByVm}, pm);

        //Create app and place on VM
        app = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                        Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        app.setDebugInfoNeverUseInCode("APP1");
        TestUtils.createAndPlaceShoppingList(economy,
                        Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app,
                        new double[]{vcpuUsedByApp, vmemUsedByApp}, vm);
        vm.getSettings().setMinDesiredUtil(vmMinDesiredUtil);
        vm.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil);
        economy.getSettings().setRightSizeLower(economyRightSizeLower);
        economy.getSettings().setRightSizeUpper(economyRightSizeUpper);
        TestUtils.setupRawCommodityMap(economy);
        if (shouldSetupCommodityResizeDependencyMap) {
            TestUtils.setupCommodityResizeDependencyMap(economy);
        }
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);
        return economy;
    }

    /**
     * Sets up topology with one PM, one VM placed on the PM, and one app placed on the VM.
     * @param economy in which all the traders are present.
     * @param nsAndPodVmemLimitCapacity - The VM's VMEM capacity.
     * @param contVmemCapacity - The Container's VMEM capacity.
     * @param vmemUsedByApp - The quantity of VMEM used by the app.
     * @param vmemUsedByCont - The quantity of VMEM used by the container.
     * @param vmemLimitUsedByPod - The quantity of VMEM used by the pod.
     * @param minDesiredUtil - The minimum desired utilization.
     * @param maxDesiredUtil - The maximum desired utilization.
     * @param economyRightSizeLower - Economy's right size lower limit.
     * @param economyRightSizeUpper - Economy's right size upper limit.
     * @return Economy with the topology setup.
     */
    private void setupContainerTopologyForResizeTest(
            Economy economy,
            double nsAndPodVmemLimitCapacity,
            double contVmemCapacity,
            double vmemUsedByApp,
            double vmemUsedByCont,
            double vmemLimitUsedByPod,
            double minDesiredUtil, double maxDesiredUtil,
            double economyRightSizeLower, double economyRightSizeUpper) {

        // Create Namespace
        namespace = TestUtils.createTrader(economy, TestUtils.NAMESPACE_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VMEMLIMITQUOTA),
                new double[]{nsAndPodVmemLimitCapacity}, false, false);
        namespace.setDebugInfoNeverUseInCode("NS1");
        namespace.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));
        //Create pod and place on namespace
        pod = TestUtils.createTrader(economy, TestUtils.POD_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VMEMLIMITQUOTA),
                new double[]{nsAndPodVmemLimitCapacity}, false, false);
        pod.setDebugInfoNeverUseInCode("POD1");
        pod.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResold(true).setResizable(false));
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEMLIMITQUOTA), pod,
                new double[]{vmemLimitUsedByPod}, namespace).setMovable(false);
        //Create cont and place on pod
        cont = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VMEM),
                new double[]{contVmemCapacity}, false, false);
        cont.setDebugInfoNeverUseInCode("CONTAINER1");
        cont.getCommoditiesSold().get(0).getSettings().setCapacityIncrement(10);
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEMLIMITQUOTA), cont,
                new double[]{vmemUsedByCont}, pod).setMovable(false);
        //Create app and place on container
        app = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        app.setDebugInfoNeverUseInCode("APP1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEM), app,
                new double[]{vmemUsedByApp}, cont);
        namespace.getSettings().setMinDesiredUtil(minDesiredUtil).setMaxDesiredUtil(maxDesiredUtil);
        cont.getSettings().setMinDesiredUtil(minDesiredUtil).setMaxDesiredUtil(maxDesiredUtil);
        economy.getSettings().setRightSizeLower(economyRightSizeLower);
        economy.getSettings().setRightSizeUpper(economyRightSizeUpper);
        TestUtils.setupRawCommodityMap(economy);
        TestUtils.setupCommodityResizeDependencyMap(economy);
        ledger = new Ledger(economy);
    }

    /**
     * There is a VM that is underutilized. This hosts appServers and dbServers that are over utilized.
     * The VM tries to scale-down its vMem and the servers try to scale-up their heap and dbMem.
     * calculateTotalEffectiveCapacityOnCoConsumersForResizeDown and calculateTotalEffectiveCapacityOnCoConsumersForResizeUp
     * validates capacities and returns a value that prevents the resizes.
     */
    @Test
    public void testResizesWithCoDependancies() {
        double pmMemCapacity = 220;
        double memUsedByVm = 65;
        double vmVmemCapacity = 500;
        double vmemUsedByAppServer1 = 50;
        double vmemUsedByDbServer1 = 50;
        double heapUsedByApp = 60;
        double vmMinDesiredUtil = 0.65;
        double vmMaxDesiredUtil = 0.75;
        double economyRightSizeLower = 1;
        double economyRightSizeUpper = 0.7;
        double appServerHeapCapacity = 100;
        double dbServerDbMemCapacity = 100;

        Economy economy = new Economy();
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.MEM),
                new double[]{pmMemCapacity}, false, false);
        pm.setDebugInfoNeverUseInCode("PM1");
        // Create VM and place on PM
        vm = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VMEM),
                new double[]{vmVmemCapacity}, false, false);
        vm.setDebugInfoNeverUseInCode("VM1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.MEM), vm,
                new double[]{memUsedByVm}, pm);
        // Create APP_SERVER and place on VM
        appserver1 = TestUtils.createTrader(economy, TestUtils.APP_SERVER_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.HEAP),
                new double[]{appServerHeapCapacity}, false, false);
        appserver1.setDebugInfoNeverUseInCode("APPS1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEM), appserver1,
                new double[]{vmemUsedByAppServer1}, vm);
        // Create APP and place on APP_SERVER
        app1 = TestUtils.createTrader(economy, TestUtils.APP_TYPE, Arrays.asList(0L),
                Arrays.asList(),
                new double[]{}, false, false);
        app1.setDebugInfoNeverUseInCode("APP1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.HEAP), app1,
                new double[]{heapUsedByApp}, appserver1);
        // Create APP_SERVER and place on VM
        appserver2 = TestUtils.createTrader(economy, TestUtils.APP_SERVER_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.HEAP),
                new double[]{appServerHeapCapacity}, false, false);
        appserver2.setDebugInfoNeverUseInCode("APPS2");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEM), appserver2,
                new double[]{vmemUsedByAppServer1}, vm);
        // Create APP and place on APP_SERVER
        app2 = TestUtils.createTrader(economy, TestUtils.APP_TYPE, Arrays.asList(0L),
                Arrays.asList(),
                new double[]{}, false, false);
        app2.setDebugInfoNeverUseInCode("APP2");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.HEAP), app2,
                new double[]{heapUsedByApp}, appserver2);
        // Create DB_SERVER and place on VM
        dbserver1 = TestUtils.createTrader(economy, TestUtils.DBS_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.DBMEM),
                new double[]{dbServerDbMemCapacity}, false, false);
        dbserver1.setDebugInfoNeverUseInCode("DBS1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VMEM), dbserver1,
                new double[]{vmemUsedByDbServer1}, vm);
        // Create DB and place on DB_SERVER
        app3 = TestUtils.createTrader(economy, TestUtils.APP_TYPE, Arrays.asList(0L),
                Arrays.asList(),
                new double[]{}, false, false);
        app3.setDebugInfoNeverUseInCode("DB1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.DBMEM), app3,
                new double[]{heapUsedByApp}, dbserver1);

        vm.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil).setMinDesiredUtil(vmMinDesiredUtil);
        appserver1.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil).setMinDesiredUtil(vmMinDesiredUtil);
        appserver1.getCommoditiesSold().get(0).getSettings().setUtilizationUpperBound(0.7);
        appserver2.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil).setMinDesiredUtil(vmMinDesiredUtil);
        appserver2.getCommoditiesSold().get(0).getSettings().setUtilizationUpperBound(0.7);
        dbserver1.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil).setMinDesiredUtil(vmMinDesiredUtil);
        dbserver1.getCommoditiesSold().get(0).getSettings().setUtilizationUpperBound(0.7);
        economy.getSettings().setRightSizeLower(economyRightSizeLower);
        economy.getSettings().setRightSizeUpper(economyRightSizeUpper);

        TestUtils.setupRawCommodityMap(economy);
        TestUtils.setupProducesDependancyMap(economy);
        TestUtils.setupCommodityResizeDependencyMap(economy);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);

        // VM tries resizing vMem down (to 215) but this resize is prevented by the appServers cumulatively selling high
        // heap/dbMem(300 total and 210 effective). Disallow appServer and dbServer resizes UPs when the resizes pushes
        // the cumulative capacity over rawMaterialCap. But in this case, since the resizes remain below vMemcapacity,
        // we allow them.
        // NOTE: We would have had resize of vMem if we considered the effectiveCap on the appServers and dbServers instead
        // of the actualCapacity
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(3, actions.size());
    }

    /**
     * Sets up topology with one PM, two VMs placed on the PM, and two apps placed on the VMs.
     * @param pmCpuCapacity - The PM's CPU capacity
     * @param pmMemCapacity - The PM's Memory capacity
     * @param vm1VcpuCapacity - The VM's VCPU capacity
     * @param vm1VmemCapacity - The VM's VMEM capacity
     * @param vm2VcpuCapacity - The VM's VCPU capacity
     * @param vm2VmemCapacity - The VM's VMEM capacity
     * @param cpuUsedByVm1 - The quantity of CPU used by the VM
     * @param memUsedByVm1 - The quantity of MEM used by the VM
     * @param cpuUsedByVm2 - The quantity of CPU used by the VM
     * @param memUsedByVm2 - The quantity of MEM used by the VM
     * @param vcpuUsedByApp1 - The quantity of VCPU used by the app.
     * @param vmemUsedByApp1 - The quantity of the VMEM used by the app.
     * @param vcpuUsedByApp2 - The quantity of VCPU used by the app.
     * @param vmemUsedByApp2 - The quantity of the VMEM used by the app.
     * @param vmMinDesiredUtil - The VM's minimum desired utilization.
     * @param vmMaxDesiredUtil - The VM's maximum desired utilization.
     * @param economyRightSizeLower - Economy's right size lower limit
     * @param economyRightSizeUpper - Economy's right size upper limit
     * @param shouldSetupCommodityResizeDependencyMap -
     *        should the commodity resize dependency map
     *        be setup for the economy passed in.
     * @return Economy with the topology setup.
     */
    private Economy setupTopologyForResizeTestAlternative(
            double pmCpuCapacity, double pmMemCapacity,
            double vm1VcpuCapacity, double vm1VmemCapacity,
            double vm2VcpuCapacity, double vm2VmemCapacity,
            double cpuUsedByVm1, double memUsedByVm1,
            double cpuUsedByVm2, double memUsedByVm2,
            double vcpuUsedByApp1, double vmemUsedByApp1,
            double vcpuUsedByApp2, double vmemUsedByApp2,
            double vmMinDesiredUtil, double vmMaxDesiredUtil,
            double economyRightSizeLower, double economyRightSizeUpper,
            boolean shouldSetupCommodityResizeDependencyMap) {
        Economy economy = new Economy();
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.CPU, TestUtils.MEM),
                new double[]{pmCpuCapacity, pmMemCapacity}, true, false);
        pm.setDebugInfoNeverUseInCode("PM1");

        // Create VMs and place on PM
        vm1 = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VCPU, TestUtils.VMEM),
                new double[]{vm1VcpuCapacity, vm1VmemCapacity}, false, false);
        vm1.setDebugInfoNeverUseInCode("VM1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1,
                new double[]{cpuUsedByVm1, memUsedByVm1}, pm);
        vm2 = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                Arrays.asList(0L), Arrays.asList(TestUtils.VCPU, TestUtils.VMEM),
                new double[]{vm2VcpuCapacity, vm2VmemCapacity}, false, false);
        vm2.setDebugInfoNeverUseInCode("VM2");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm2,
                new double[]{cpuUsedByVm2, memUsedByVm2}, pm);

        //Create apps and place on VMs
        app1 = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        app1.setDebugInfoNeverUseInCode("APP1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app1,
                new double[]{vcpuUsedByApp1, vmemUsedByApp1}, vm1);
        app2 = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        app2.setDebugInfoNeverUseInCode("APP2");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app2,
                new double[]{vcpuUsedByApp2, vmemUsedByApp2}, vm2);

        vm1.getSettings().setMinDesiredUtil(vmMinDesiredUtil);
        vm1.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil);
        vm2.getSettings().setMinDesiredUtil(vmMinDesiredUtil);
        vm2.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil);
        economy.getSettings().setRightSizeLower(economyRightSizeLower);
        economy.getSettings().setRightSizeUpper(economyRightSizeUpper);
        TestUtils.setupRawCommodityMap(economy);
        if (shouldSetupCommodityResizeDependencyMap) {
            TestUtils.setupCommodityResizeDependencyMap(economy);
        }
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);
        return economy;
    }

    @Test
    public void testResizeDecisions_application() {
        List<String> actual;
        /*
         * Setup economy with one PM, one VM and two applications.
         * Both the apps are highly utlized and they resize up.
         * Since the VM shops later it also resizes up.
         * Expected result: 3 resize actions are generated. two for apps and 1 for vm
         */

        //APP buys DBMEM
        actual = setupTopologyForDBHeapResizeTestAndRunResize(150, 150,
                100, 100, 40, 40,
                35, 35, 35, 34,
                0.65, 0.8, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true, true, true);
        assertEquals(3, actual.size());
        actual.removeAll(Arrays.asList("APP1", "APP2", "VM1"));
        assertEquals(0, actual.size());

        //APP buys HEAP
        actual = setupTopologyForDBHeapResizeTestAndRunResize(150, 150,
                100, 100, 40, 40,
                35, 35, 35, 34,
                0.65, 0.8, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true, true, false);
        assertEquals(3, actual.size());
        actual.removeAll(Arrays.asList("APP1", "APP2", "VM1"));
        assertEquals(0, actual.size());

        /*
         * Setup economy with one PM, one VM and two applications.
         * Both the apps are highly utlized and they resize up.
         * Since the VM shops before it never gets a chance to resize up.
         * Expected result: 2 resize actions are generated. two for apps and none for vm
         */
        //APP buys DBMEM
        actual = setupTopologyForDBHeapResizeTestAndRunResize(100, 100,
                100, 100, 40, 40,
                35, 35, 35, 34,
                0.65, 0.8, 0.2, 0.3,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true, false, true);
        assertEquals(2, actual.size());
        actual.removeAll(Arrays.asList("APP1", "APP2"));
        assertEquals(0, actual.size());

        //APP buys HEAP
        actual = setupTopologyForDBHeapResizeTestAndRunResize(100, 100,
                100, 100, 40, 40,
                35, 35, 35, 34,
                0.65, 0.8, 0.2, 0.3,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true, false, false);
        assertEquals(2, actual.size());
        actual.removeAll(Arrays.asList("APP1", "APP2"));
        assertEquals(0, actual.size());


    }

    /**
     * Sets up topology with one PM, one VM placed on the PM, and one app placed on the VM.
     * @param pmCpuCapacity - The PM's CPU capacity
     * @param pmMemCapacity - The PM's Memory capacity
     * @param vmVcpuCapacity - The VM's VCPU capacity
     * @param vmVmemCapacity - The VM's VMEM capacity
     * @param cpuUsedByVm - The quantity of CPU used by the VM
     * @param memUsedByVm - The quantity of MEM used by the VM
     * @param vcpuUsedByApp - The quantity of VCPU used by the app.
     * @param vmemUsedByApp - The quantity of the VMEM used by the app.
     * @param AppMemCapacity - The db mem capacity of app
     * @param memUsedByApp - The quantity of dbmem used by the app
     * @param vmMinDesiredUtil - The VM's minimum desired utilization.
     * @param vmMaxDesiredUtil - The VM's maximum desired utilization.
     * @param appMinDesiredUtil - The app's minimum desired utilization.
     * @param appMaxDesiredUtil - The app's maximum desired utilization.
     * @param economyRightSizeLower - Economy's right size lower limit
     * @param economyRightSizeUpper - Economy's right size upper limit
     * @param shouldSetupCommodityResizeDependencyMap -
     *        should the commodity resize dependency map
     *        be setup for the economy passed in.
     * @param appfirst - if true the app shops first.
     * @param isdbMem - if true the app shops dbmem. false app shops heap
     * @return Economy with the topology setup.
     */
    private List<String>  setupTopologyForDBHeapResizeTestAndRunResize(
            double pmCpuCapacity, double pmMemCapacity,
            double vmVcpuCapacity, double vmVmemCapacity,
            double cpuUsedByVm, double memUsedByVm,
            double vcpuUsedByApp, double vmemUsedByApp,
            double AppMemCapacity, double memUsedByApp,
            double vmMinDesiredUtil, double vmMaxDesiredUtil,
            double appMinDesiredUtil, double appMaxDesiredUtil,
            double economyRightSizeLower, double economyRightSizeUpper,
            boolean shouldSetupCommodityResizeDependencyMap,
            boolean appfirst,
            boolean isdbMem) {
        Economy economy = new Economy();
        pm = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0L),
                Arrays.asList(TestUtils.CPU, TestUtils.MEM),
                new double[]{pmCpuCapacity, pmMemCapacity}, true, false);
        pm.setDebugInfoNeverUseInCode("PM1");
        if (!appfirst) {
            vm = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                    Arrays.asList(0L), Arrays.asList(TestUtils.VCPU, TestUtils.VMEM),
                    new double[]{vmVcpuCapacity, vmVmemCapacity}, false, false);
        }
        // Create VM and place on PM
        CommoditySpecification commoditySpecification;
        if (isdbMem) {
            commoditySpecification = TestUtils.DBMEM;
        } else {
            commoditySpecification = TestUtils.HEAP;
        }
        app1 = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(commoditySpecification), new double[]{AppMemCapacity}, false, false);
        app2 = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(commoditySpecification), new double[]{AppMemCapacity}, false, false);
        if (appfirst) {
            vm = TestUtils.createTrader(economy, TestUtils.VM_TYPE,
                    Arrays.asList(0L), Arrays.asList(TestUtils.VCPU, TestUtils.VMEM),
                    new double[]{vmVcpuCapacity, vmVmemCapacity}, false, false);
        }
        vm.setDebugInfoNeverUseInCode("VM1");
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm,
                new double[]{cpuUsedByVm, memUsedByVm}, pm);
        //Create app and place on VM
        app1.setDebugInfoNeverUseInCode("APP1");
        app1.getCommoditiesSold().get(0).setQuantity(memUsedByApp);
        app1.getSettings().setMinDesiredUtil(appMinDesiredUtil);
        app1.getSettings().setMaxDesiredUtil(appMaxDesiredUtil);
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app1,
                new double[]{vcpuUsedByApp, vmemUsedByApp}, vm);
        app2.setDebugInfoNeverUseInCode("APP2");
        app2.getCommoditiesSold().get(0).setQuantity(memUsedByApp);
        app2.getSettings().setMinDesiredUtil(appMinDesiredUtil);
        app2.getSettings().setMaxDesiredUtil(appMaxDesiredUtil);
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app2,
                new double[]{vcpuUsedByApp, vmemUsedByApp}, vm);
        vm.getSettings().setMinDesiredUtil(vmMinDesiredUtil);
        vm.getSettings().setMaxDesiredUtil(vmMaxDesiredUtil);
        economy.getSettings().setRightSizeLower(economyRightSizeLower);
        economy.getSettings().setRightSizeUpper(economyRightSizeUpper);
        TestUtils.setupRawCommodityMap(economy);
        if (shouldSetupCommodityResizeDependencyMap) {
            TestUtils.setupCommodityResizeDependencyMap(economy);
        }
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        List<String> actual = new ArrayList<>();
        for (Action a : actions) {
            if (a.getType() == ActionType.RESIZE) {
                actual.add(a.getActionTarget().getDebugInfoNeverUseInCode());
            }
        }
        return actual;
    }

    @Test
    public void testResizeDecisionsUsingHistoricalQuantityResizeUp() {
        double historicalQuantity = 9;
        double currentQuantity = 6.5;
        Economy economy = setUpEconomyWithHistoricalQuantity(currentQuantity, historicalQuantity);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(1, actions.size());

        Resize resizeAction = (Resize)actions.get(0);

        assertEquals(14d, resizeAction.getNewCapacity(), 0.01);
    }

    @Test
    public void testResizeDecisionsUsingHistoricalQuantityResizeDown() {
        double historicalQuantity = 2;
        double currentQuantity = 6.5;
        Economy economy = setUpEconomyWithHistoricalQuantity(currentQuantity, historicalQuantity);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(1, actions.size());

        Resize resizeAction = (Resize)actions.get(0);

        assertEquals(7d, resizeAction.getNewCapacity(), 0.01);
    }

    @Test
    public void testResizeDecisionsUsingHistoricalQuantityNoResize() {
        double historicalQuantity = 6.5;
        double currentQuantity = 9;
        Economy economy = setUpEconomyWithHistoricalQuantity(currentQuantity, historicalQuantity);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(0, actions.size());
    }

    /**
     * This method create an economy where a consumer is consuming from a provider. In
     * this economy, the historical quantity for the commodity sold of the consumer is
     * populated based on the input parameter. The purpose of this method is verifying
     * the amount the market recommend resizing to based on historical quantity.
     *
     * @param currentQuantity The current quantity of the commodity sold by consumer.
     * @param historicalQuantity The historical quantity of the commodity sold by consumer.
     *
     * @return the constructed economy.
     */
    private Economy setUpEconomyWithHistoricalQuantity(double currentQuantity, double historicalQuantity) {
        Economy economy = new Economy();
        final CommoditySpecification cpuSpec =
                        new CommoditySpecification(0, 1000);
        final CommoditySpecification vcpuSpec =
                        new CommoditySpecification(1, 1000);
        Basket basketSoldBySeller = new Basket(Collections.singleton(cpuSpec));
        Basket basketSoldByBuyer = new Basket(Collections.singleton(vcpuSpec));

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSoldBySeller);
        seller
            .getCommoditiesSold()
            .get(0)
                .setQuantity(20)
                .setPeakQuantity(25)
                .setCapacity(100);
        seller.setDebugInfoNeverUseInCode("Seller");

        // create consumer
        Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSoldByBuyer);
        economy.addBasketBought(buyer, basketSoldBySeller)
            .setQuantity(0, currentQuantity)
            .setPeakQuantity(0, currentQuantity)
            .setMovable(true)
            .move(seller);
        buyer.setDebugInfoNeverUseInCode("Buyer");

        buyer
            .getCommoditiesSold()
            .get(0)
                .setQuantity(currentQuantity)
                .setHistoricalQuantity(historicalQuantity)
                .setPeakQuantity(currentQuantity)
                .setCapacity(10);

        economy.getModifiableRawCommodityMap().put(vcpuSpec.getBaseType(),
                new RawMaterials(Collections.singletonList(CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                        .newBuilder().setCommodityType(cpuSpec.getBaseType()).build())));

        buyer.getSettings().setMinDesiredUtil(0.6);
        buyer.getSettings().setMaxDesiredUtil(0.7);

        seller.getSettings().setMinDesiredUtil(0.6);
        seller.getSettings().setMaxDesiredUtil(0.7);
        economy.getSettings().setRightSizeLower(0.5);
        economy.getSettings().setRightSizeUpper(0.8);
        return economy;
    }

    /**
     * We want to test that when the rate of resize is set to the lowest setting (1) in UI which sets
     * the value internally to (10^10), only 1 decrement will be done for the resize down action.
     */
    @Test
    public void testResizeDownAmount() {
        double E = 0.00001;
        Economy economy = setupTopologyForResizeTest(100, 100,
                100, 100, 70, 70, 20, 20, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        economy.getSettings().setDefaultRateOfResize((float)Math.pow(10, 10));
        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(2, actions.size());

        Resize resizeAction1 = (Resize)actions.get(0);
        Resize resizeAction2 = (Resize)actions.get(1);
        assertEquals(resizeAction1.getResizedCommodity().getSettings().getCapacityIncrement(), resizeAction1.getOldCapacity() - resizeAction1.getNewCapacity(), E);
        assertEquals(resizeAction2.getResizedCommodity().getSettings().getCapacityIncrement(), resizeAction2.getOldCapacity() - resizeAction2.getNewCapacity(), E);
    }

    /**
     * We want to test that when the rate of resize is set to the lowest setting (1) in UI which sets
     * the value internally to (10^10), only 1 increment will be done for the resize up action.
     */
    @Test
    public void testResizeUpAmount() {
        double E = 0.00001;
        Economy economy = setupTopologyForResizeTest(150, 150,
                100, 100, 70, 70, 95, 95, 0.65, 0.8,
            RIGHT_SIZE_LOWER, RIGHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        economy.getSettings().setDefaultRateOfResize((float)Math.pow(10, 10));
        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(2, actions.size());

        Resize resizeAction1 = (Resize)actions.get(0);
        Resize resizeAction2 = (Resize)actions.get(1);
        assertEquals(resizeAction1.getResizedCommodity().getSettings().getCapacityIncrement(), resizeAction1.getNewCapacity() - resizeAction1.getOldCapacity(), E);
        assertEquals(resizeAction2.getResizedCommodity().getSettings().getCapacityIncrement(), resizeAction2.getNewCapacity() - resizeAction2.getOldCapacity(), E);
    }

    /**
     * Create an economy with two consumers that can optionally belong to the same scaling group.
     *
     * @param configs array of triplets specifying configuration for consumers:
     *                { buyer capacity, historical quantity, current quantity }
     * @param consistentScaling true if the consumers belong to the same scaling group.
     * @return the constructed economy.
     */
    private Economy createConsistentScalingEconomy(final double[][] configs,
                                                   final boolean consistentScaling) {
        Economy economy = new Economy();
        final CommoditySpecification cpuSpec = new CommoditySpecification(0, 1000);
        final CommoditySpecification vcpuSpec = new CommoditySpecification(1, 1000);
        Basket basketSoldBySeller = new Basket(Collections.singleton(cpuSpec));
        Basket basketSoldByBuyer = new Basket(Collections.singleton(vcpuSpec));

        // create supplier
        Trader seller = economy.addTrader(0, TraderState.ACTIVE, basketSoldBySeller);
        seller
            .getCommoditiesSold()
            .get(0)
            .setQuantity(20)
            .setPeakQuantity(25)
            .setCapacity(100);
        seller.setDebugInfoNeverUseInCode("Seller");

        // create consumers
        int n = 1;
        for (double[]config : configs) {
            Trader buyer = economy.addTrader(1, TraderState.ACTIVE, basketSoldByBuyer);
            economy.addBasketBought(buyer, basketSoldBySeller)
                .setQuantity(0, config[2])
                .setPeakQuantity(0, config[2])
                .setMovable(true)
                .move(seller);
            buyer.setDebugInfoNeverUseInCode("Buyer-" + n);
            if (consistentScaling) {
                buyer.setScalingGroupId("scaling-group-1");
                economy.populatePeerMembersForScalingGroup(buyer, "scaling-group-1");
            }

            buyer
                .getCommoditiesSold()
                .get(0)
                .setQuantity(config[2])
                .setHistoricalQuantity(config[1])
                .setPeakQuantity(config[2])
                .setCapacity(config[0]);

            economy.getModifiableRawCommodityMap().put(vcpuSpec.getBaseType(),
                    new RawMaterials(Collections.singletonList(CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                            .newBuilder().setCommodityType(cpuSpec.getBaseType()).build())));

            buyer.getSettings().setMinDesiredUtil(0.6);
            buyer.getSettings().setMaxDesiredUtil(0.7);
            n += 1;
        }

        seller.getSettings().setMinDesiredUtil(0.6);
        seller.getSettings().setMaxDesiredUtil(0.7);
        economy.getSettings().setRightSizeLower(0.5);
        economy.getSettings().setRightSizeUpper(0.8);
        return economy;
    }

    /**
     * Ensure that no-op resizes are not being generated due to consistent scaling. If scaled
     * independently, we would generate two actions:
     *
     * <p>- Buyer-2: 10 -> 7
     * - Buyer-3: 10 -> 14
     *
     * <p>As a scaling group, we should see three actions:
     * - Buyer-1, Buyer-2, Buyer-3: 10 -> 14
     *
     * <p>Buyer-4 should not generate an action, because it is already at 14.  Buyer-5 should not
     * generate an actiom because it is at 13.5, and the delta of 0.5 is less than the capacity
     * increment of 1.0.
     */

    @Test
    public void testConsistentResize() {
        // buyerCapacity, historicalQuantity, currentQuantity
        double[][] config = {
            { 10.0, 6.5, 9.0 },  // No resize, .65 is in .5 to .8 range
            { 10.0, 2.0, 6.5 },  // Old = 10, new = 7
            { 10.0, 9.0, 6.5 },  // Old = 10, new = 14
            { 14.0, 9.0, 14.0 }, // Old 14, new 14, engage, but old == new, so no action
            // Old 14, new 13.5, but delta is less than capacity increment of 1.0, so no action
            { 13.5, 9.0, 13.5 },
        };

        Economy economy = createConsistentScalingEconomy(config, true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        Ledger ledger = new Ledger(economy);
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(3, actions.size());
        assertTrue(actions.stream().allMatch(action -> ((Resize)action).getNewCapacity() == 14.0));
    }
}
