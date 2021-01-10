package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ReconfigureProviderAddition;
import com.vmturbo.platform.analysis.actions.ReconfigureProviderRemoval;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;

/**
 * Test class for Reconfigure integration tests.
 */
public class ReconfigureIntegrationTest {

    private static final Basket VMPMSoftwareLicense = new Basket(TestUtils.CPU, TestUtils.SOFTWARE_LICENSE_COMMODITY);
    private static final Basket VMPM = new Basket(TestUtils.CPU);
    private static final Basket VMApp = new Basket(TestUtils.VCPU);

    private Economy economy;
    private Topology topology;
    private Trader vm1;
    private Trader vm2;
    private Trader pm1;
    private Trader pm2;
    private ShoppingList sl1;
    private ShoppingList sl2;

    /**
     * Set up an economy with entities for test cases.
     */
    @Before
    public void setUp() {
        topology = new Topology();
        economy = (Economy)topology.getEconomy();
        economy.getSettings().getReconfigureableCommodities().add(TestUtils.SOFTWARE_LICENSE_COMMODITY.getType());

        vm1 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        vm2 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        pm1 = economy.addTrader(2, TraderState.ACTIVE, VMPM);
        pm1.setOid(77777);
        pm2 = economy.addTrader(2, TraderState.ACTIVE, VMPMSoftwareLicense);
        pm2.setOid(88888);

        sl1 = topology.addBasketBought(777, vm1, VMPM);
        sl2 = topology.addBasketBought(888, vm2, VMPMSoftwareLicense);

        vm1.setDebugInfoNeverUseInCode("VirtualMachine|1");
        vm2.setDebugInfoNeverUseInCode("VirtualMachine|2");
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|1");
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|2");

        sl1.setQuantity(0, 10).setPeakQuantity(0, 10).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl1, pm1, new double[]{10});

        sl2.setQuantity(0, 10).setPeakQuantity(0, 10).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl2, pm2, new double[]{10, 0});

        pm1.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(10).setPeakQuantity(10);
        pm2.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(10).setPeakQuantity(10);

        pm1.getSettings().setMaxDesiredUtil(0.75).setMinDesiredUtil(0.65).setCanAcceptNewCustomers(true)
                .setSuspendable(true).setCloneable(true).setReconfigurable(true);
        pm2.getSettings().setMaxDesiredUtil(0.75).setMinDesiredUtil(0.65).setCanAcceptNewCustomers(true)
                .setSuspendable(true).setCloneable(true).setReconfigurable(true);
    }

    /**
     * Test that will generate a ReconfigureProviderAddition action.
     */
    @Test
    public void testReconfigureProviderAddition() {
        Trader vm3 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        ShoppingList sl3 = topology.addBasketBought(444, vm3, VMPMSoftwareLicense);
        vm3.setDebugInfoNeverUseInCode("VirtualMachine|3");
        sl3.setQuantity(0, 40).setPeakQuantity(0, 40).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl3, pm2, new double[]{40, 0});

        Trader vm4 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        ShoppingList sl4 = topology.addBasketBought(555, vm4, VMPMSoftwareLicense);
        vm4.setDebugInfoNeverUseInCode("VirtualMachine|4");
        sl4.setQuantity(0, 40).setPeakQuantity(0, 40).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl4, pm2, new double[]{40, 0});

        pm2.getCommoditySold(TestUtils.CPU).setQuantity(90).setPeakQuantity(90);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);

        List<Action> actions = Reconfigure.reconfigureAdditionDecisions(economy, ledger);
        Optional<Action> action = actions.stream()
            .filter(a -> a instanceof ReconfigureProviderAddition).findFirst();
        assertTrue(action.isPresent());
        ReconfigureProviderAddition reconfigureAction = (ReconfigureProviderAddition)action.get();
        assertTrue(reconfigureAction.getActionTarget() == pm1);
        assertTrue(reconfigureAction.getReconfiguredCommodities()
            .get(TestUtils.SOFTWARE_LICENSE_COMMODITY) != null);
        assertTrue(pm1.getBasketSold().size() == 2);
        assertTrue(pm1.getCommoditiesSold().size() == 2);
        assertTrue(pm1.getCommoditySold(TestUtils.SOFTWARE_LICENSE_COMMODITY) != null);
    }

    /**
     * Test that will generate a ReconfigureProviderRemoval action.
     */
    @Test
    public void testReconfigureProviderRemoval() {
        Trader pm3 = economy.addTrader(2, TraderState.ACTIVE, VMPMSoftwareLicense);
        pm3.setOid(99999);
        pm3.setDebugInfoNeverUseInCode("PhysicalMachine|3");

        pm3.getSettings().setMaxDesiredUtil(0.75).setMinDesiredUtil(0.65).setCanAcceptNewCustomers(true)
                .setSuspendable(true).setCloneable(true).setReconfigurable(true);

        Trader vm3 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        ShoppingList sl3 = topology.addBasketBought(999, vm3, VMPMSoftwareLicense);
        vm3.setDebugInfoNeverUseInCode("VirtualMachine|3");
        sl3.setQuantity(0, 5).setPeakQuantity(0, 5).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl3, pm3, new double[]{5, 0});

        Trader vm4 = economy.addTrader(1, TraderState.ACTIVE, VMApp);
        ShoppingList sl4 = economy.addBasketBought(vm4, VMPM);
        vm4.setDebugInfoNeverUseInCode("VirtualMachine|4");
        sl4.setQuantity(0, 60).setPeakQuantity(0, 60).setMovable(true);
        TestUtils.moveSlOnSupplier(economy, sl4, pm3, new double[]{60});

        sl1.setQuantity(0, 65).setPeakQuantity(0, 65);
        sl2.setQuantity(0, 65).setPeakQuantity(0, 65);
        pm1.getCommoditySold(TestUtils.CPU).setQuantity(65).setPeakQuantity(65);
        pm2.getCommoditySold(TestUtils.CPU).setQuantity(65).setPeakQuantity(65);
        pm3.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(65).setPeakQuantity(65);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = new Ledger(economy);
        economy.simulationClone();
        List<Action> actions = Reconfigure.reconfigureRemovalDecisions(economy, ledger);

        Optional<Action> action = actions.stream()
            .filter(a -> a instanceof ReconfigureProviderRemoval).findFirst();
        assertTrue(action.isPresent());
        ReconfigureProviderRemoval reconfigureAction = (ReconfigureProviderRemoval)action.get();
        assertTrue(reconfigureAction.getActionTarget() == pm3);
        assertTrue(reconfigureAction.getReconfiguredCommodities()
            .get(TestUtils.SOFTWARE_LICENSE_COMMODITY) != null);
        assertTrue(pm3.getBasketSold().size() == 1);
        assertTrue(pm3.getCommoditiesSold().size() == 1);
        assertTrue(pm3.getCommoditySold(TestUtils.SOFTWARE_LICENSE_COMMODITY) == null);
        assertTrue(sl3.getSupplier() == pm2);

        // Covert the Move action after license removal.
        ActionTO actionto = AnalysisToProtobuf.actionTO(actions.get(1), topology.getShoppingListOids(), topology);
        assertTrue(actionto.getMove().getMoveExplanation().getEvacuation().getEvacuationExplanation().hasReconfigureRemoval());
    }
}