package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

public class ResizerTest {

    TestCommon testEconomy;
    Trader app;
    Trader vm;
    Trader pm;
    Ledger ledger;
    public final static Set<CommoditySpecification> EXPECTED_COMM_SPECS_TO_BE_RESIZED =
                    Collections.unmodifiableSet(
                                    new HashSet<CommoditySpecification>(
                                                    Arrays.asList(TestUtils.VCPU, TestUtils.VMEM)));
    private static final double RIHT_SIZE_LOWER = 0.3;
    private static final double RIHT_SIZE_UPPER = 0.7;

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
    public void testResizeDecisions_resizeDownWithDependency() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 70, 70, 20, 20, 0.65, 0.8,
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, true);
        final double cpuUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        final double memUsedOnCommSoldBeforeResize = pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();

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
        //Check that the quantites of the dependent commodities decreased.
        double cpuUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.CPU)).getQuantity();
        assertTrue(cpuUsedOnCommSoldBeforeResize > cpuUsedOnCommSoldAfterResize);
        double memUsedOnCommSoldAfterResize =
                        pm.getCommoditiesSold()
                        .get(pm.getBasketSold().indexOf(TestUtils.MEM)).getQuantity();
        assertTrue(memUsedOnCommSoldBeforeResize > memUsedOnCommSoldAfterResize);
    }

    /**
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 40 from it. App buys 80 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 40 from it. App buys 80 of VM's VMEM.
     * So, the VM's VCPU and VMEM have high ROI.
     * CommodityResizeDependencyMap is setup such that an increase in VCPU
     * leads to increase in CPU and vice versa.
     * Increase in VMEM leads to increase in MEM and vice versa.
     * Expected result: Resize up the VM. Quantity of CPU and MEM used
     * in the PM also increases.
     */
    @Test
    public void testResizeDecisions_resizeUpWithDependency() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 40, 40, 80, 80, 0.65, 0.75,
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, true);
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
     * Setup economy with one PM, one VM and one application.
     * PM CPU capacity = 100, VM buys 80 from it. App buys 98 of VM's VCPU.
     * PM MEM capacity = 100, VM buys 80 from it. App buys 98 of VM's VMEM.
     * So, the VM's VCPU and VMEM have high ROI.
     * The desired capacity increase is 21 units - result of calling calculateDesiredCapacity.
     * But, the increase in memory and CPU will be limited to oldCapacity + 20.
     * (20 is the remaining capacity => PM MEM capacity - PM MEM quantity. Same for CPU.).
     */
    @Test
    public void testResizeDecisions_resizeUpGreaterThanUnderlyingProviderAllows() {
        Economy economy = setupTopologyForResizeTest(100, 100,
                        100, 100, 80, 80, 98, 98, 0.65, 0.75,
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, false);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        //Assert that VCPU and VMEM of VM were resized up
        //and limited to the remaining capacity of underlying PM.
        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertTrue(resize1.getOldCapacity() < resize1.getNewCapacity());
        double estimatedNewCapacity = resize1.getOldCapacity() + 20;
        assertEquals(resize1.getNewCapacity(), estimatedNewCapacity, 0.001);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertTrue(resize2.getOldCapacity() < resize2.getNewCapacity());
        estimatedNewCapacity = resize2.getOldCapacity() + 20;
        assertEquals(resize2.getNewCapacity(), estimatedNewCapacity, 0.001);
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
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, true);
        vm.getCommoditiesSold().stream().forEach(c -> c.setMaxQuantity(90));

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertEquals(2, actions.size());
        assertEquals(ActionType.RESIZE, actions.get(0).getType());
        Resize resize1 = (Resize)actions.get(0);
        assertEquals(resize1.getActionTarget(), vm);
        assertEquals(resize1.getNewCapacity(), resize1.getOldCapacity() - 10, 0.001);
        assertEquals(ActionType.RESIZE, actions.get(1).getType());
        Resize resize2 = (Resize)actions.get(1);
        assertEquals(resize2.getActionTarget(), vm);
        assertEquals(resize2.getNewCapacity(),  resize2.getOldCapacity() - 10, 0.001);

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
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, false);
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
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, false);

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
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, false);
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
                        RIHT_SIZE_LOWER, RIHT_SIZE_UPPER, false);
        //VM is not eligible for resize down
        vm.changeState(TraderState.INACTIVE);

        List<Action> actions = Resizer.resizeDecisions(economy, ledger);

        assertTrue(actions.isEmpty());
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
        if(shouldSetupCommodityResizeDependencyMap){
            TestUtils.setupCommodityResizeDependencyMap(economy);
        }
        economy.populateMarketsWithSellers();
        ledger = new Ledger(economy);
        return economy;
    }
}