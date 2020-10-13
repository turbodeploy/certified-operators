package com.vmturbo.platform.analysis.economy;

import java.util.Collections;
import java.util.HashSet;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.ede.Placement;
import com.vmturbo.platform.analysis.ede.Provision;
import com.vmturbo.platform.analysis.ede.Resizer;
import com.vmturbo.platform.analysis.ede.Suspension;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * Tests ensuring we recovery from exceptions during analysis.
 */
public class ExceptionsTest {
    private static final Basket VMtoPM = new Basket(TestUtils.CPU);
    private static final Basket APPtoVM = new Basket(TestUtils.VCPU);

    private Economy economy;
    private Topology topo;
    Ledger ledger;

    /**
     * Set up for tests.
     */
    @Before
    public void setUp() {
        topo = new Topology();
        economy = topo.getEconomyForTesting();
        economy.setTopology(topo);
    }

    /**
     * Test shop alone Placement exception handling.
     */
    @Test
    public void testPlacementShopAloneRecovery() {
        Economy economy = Mockito.mock(Economy.class);
        Trader vm = topo.addTrader(1L, 0, TraderState.ACTIVE, APPtoVM,
            Collections.emptyList());
        vm.setDebugInfoNeverUseInCode("VirtualMachine");
        ShoppingList sl = topo.addBasketBought(100, vm, VMtoPM);
        Mockito.when(economy.getForceStop())
            .thenThrow(new RuntimeException("Test Shop Alone Placement Exception"));
        Mockito.when(economy.getExceptionTraders()).thenReturn(new HashSet<>());
        Placement.generateShopAlonePlacementDecisions(economy, sl);
    }

    /**
     * Test shop together Placement exception handling.
     */
    @Test
    public void testPlacementShopTogetherRecovery() {
        Economy economy = Mockito.mock(Economy.class);
        Trader vm = topo.addTrader(1L, 0, TraderState.ACTIVE, APPtoVM,
            Collections.emptyList());
        vm.setDebugInfoNeverUseInCode("VirtualMachine");
        Mockito.when(economy.getForceStop())
            .thenThrow(new RuntimeException("Test Shop Together Placement Exception"));
        Mockito.when(economy.getExceptionTraders()).thenReturn(new HashSet<>());
        Placement.generateShopTogetherDecisions(economy, Lists.newArrayList(vm));
    }

     /**
     * Test Resizer exception handling.
     */
    @Test
    public void testResizerRecovery() {
        Ledger ledger = Mockito.mock(Ledger.class);
        Trader vm2 = topo.addTrader(1L, 0, TraderState.ACTIVE, APPtoVM, Collections.emptyList());
        vm2.getCommoditySold(TestUtils.VCPU).setCapacity(0).getSettings().setResizable(true);
        vm2.setDebugInfoNeverUseInCode("VirtualMachine");
        Mockito.when(ledger.calculateCommodityExpensesAndRevenuesForTrader(economy, vm2))
            .thenThrow(new RuntimeException("Test Resizer Exception"));
        Resizer.resizeDecisions(economy, ledger);
    }

     /**
     * Test Provision exception handling.
     */
    @Test
    public void testProvisionRecovery() {
        Trader pm1 = topo.addTrader(1L, 0, TraderState.ACTIVE, VMtoPM,
            Collections.singletonList(0L));
        pm1.getSettings().setCanAcceptNewCustomers(true);
        pm1.getSettings().setCloneable(true);
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine");
        Trader vm1 = topo.addTrader(2L, 1, TraderState.ACTIVE, APPtoVM,
            Collections.emptyList());
        vm1.setDebugInfoNeverUseInCode("VirtualMachine");
        ShoppingList sl1 = topo.addBasketBought(100, vm1, VMtoPM);
        sl1.move(pm1);
        sl1.setMovable(true);
        Trader vm2 = topo.addTrader(3L, 2, TraderState.ACTIVE, APPtoVM,
            Collections.emptyList());
        vm1.setDebugInfoNeverUseInCode("VirtualMachine2");
        ShoppingList sl2 = topo.addBasketBought(101, vm2, VMtoPM);
        sl2.move(pm1);
        sl2.setMovable(true);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        Ledger ledger = Mockito.mock(Ledger.class);
        Mockito.when(ledger.calculateExpAndRevForSellersInMarket(economy, economy.getMarket(VMtoPM)))
            .thenThrow(new RuntimeException("Test Provision Exception"));
        Provision.provisionDecisions(economy, ledger);
    }

    /**
    * Test Suspension exception handling.
    */
   @Test
   public void testSuspensionRecovery() {
       MockedStatic<Suspension> s = Mockito.mockStatic(Suspension.class, "sellerHasNonDaemonCustomers");
       try {
           Trader pm1 = topo.addTrader(1L, 0, TraderState.ACTIVE, VMtoPM,
               Collections.singletonList(0L));
           pm1.getSettings().setCanAcceptNewCustomers(true);
           pm1.getSettings().setSuspendable(true);
           pm1.setDebugInfoNeverUseInCode("PhysicalMachine");
           Trader vm1 = topo.addTrader(2L, 1, TraderState.ACTIVE, APPtoVM,
               Collections.emptyList());
           vm1.setDebugInfoNeverUseInCode("VirtualMachine");
           ShoppingList sl1 = topo.addBasketBought(100, vm1, VMtoPM);
           sl1.move(pm1);
           sl1.setMovable(true);
           Trader vm2 = topo.addTrader(3L, 2, TraderState.ACTIVE, APPtoVM,
               Collections.emptyList());
           vm1.setDebugInfoNeverUseInCode("VirtualMachine2");
           ShoppingList sl2 = topo.addBasketBought(101, vm2, VMtoPM);
           sl2.move(pm1);
           sl2.setMovable(true);
           economy.populateMarketsWithSellersAndMergeConsumerCoverage();
           ledger = new Ledger(economy);
           Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
           Mockito.when(Suspension.sellerHasNonDaemonCustomers(pm1))
               .thenThrow(new RuntimeException("Test Suspension Exception"));
           suspension.suspensionDecisions(economy, ledger);
       } finally {
           s.close();
       }
   }
}