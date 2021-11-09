package com.vmturbo.platform.analysis.ede;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

public class EdeIntegrationTest {

    private static final Basket VMtoPM = new Basket(TestUtils.CPU);
    private static final Basket APPtoVM = new Basket(TestUtils.VCPU);

    private @NonNull Economy first;
    private @NonNull Topology firstTopology;
    private @Nonnull Trader vm1, vm2, pm1, pm2, pm3;
    ShoppingList shoppingListOfVm2, shoppingListOfVm1;

    /**
     * Initialize IdentityGenerator.
     */
    @BeforeClass
    public static void init() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() {
        firstTopology = new Topology();
        first = firstTopology.getEconomyForTesting();
        first.setTopology(firstTopology);

        vm1 = firstTopology.addTrader(1L, 0, TraderState.ACTIVE, APPtoVM,
                                        Collections.emptyList());
        shoppingListOfVm1 = firstTopology.addBasketBought(100, vm1, VMtoPM);
        vm2 = firstTopology.addTrader(2L, 0, TraderState.ACTIVE, new Basket(),
                                        Collections.emptyList());
        shoppingListOfVm2 = firstTopology.addBasketBought(101, vm2, VMtoPM);
        pm1 = firstTopology.addTrader(3L, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        pm2 = firstTopology.addTrader(4L, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        pm3 = firstTopology.addTrader(5L, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));

        vm1.setDebugInfoNeverUseInCode("VirtualMachine|1");
        vm2.setDebugInfoNeverUseInCode("VirtualMachine|2");
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|1");
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|2");
        pm3.setDebugInfoNeverUseInCode("PhysicalMachine|3");

        shoppingListOfVm1.setQuantity(0, 40).setPeakQuantity(0, 40).setMovable(true);
        shoppingListOfVm1.move(pm1);

        shoppingListOfVm2.setQuantity(0, 10).setPeakQuantity(0, 10).setMovable(true);
        shoppingListOfVm2.move(pm2);

        pm1.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(40);
        pm2.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(10);
        pm3.getCommoditySold(TestUtils.CPU).setCapacity(11).setQuantity(0);
        // to test resize of VCPU on VM1
        vm1.getCommoditySold(TestUtils.VCPU).setCapacity(70).setQuantity(65)
                .getSettings().setCapacityIncrement(1);

        pm1.getSettings().setMaxDesiredUtil(0.7).setMinDesiredUtil(0.6).setCanAcceptNewCustomers(true)
                .setSuspendable(true).setCloneable(true);
        pm2.getSettings().setMaxDesiredUtil(0.7).setMinDesiredUtil(0.6).setCanAcceptNewCustomers(true)
                .setSuspendable(true).setCloneable(true);
        pm3.getSettings().setMaxDesiredUtil(0.7).setMinDesiredUtil(0.6).setCanAcceptNewCustomers(true)
                .setSuspendable(false).setCloneable(false);
        vm1.getSettings().setMinDesiredUtil(0.6).setMaxDesiredUtil(0.7);

        first.getCommodityBought(shoppingListOfVm1, TestUtils.CPU).setQuantity(40);
        first.getCommodityBought(shoppingListOfVm2, TestUtils.CPU).setQuantity(10);
        first.getSettings().setRightSizeLower(0.3).setRightSizeUpper(0.7).setEstimatesEnabled(false);

        first.populateMarketsWithSellersAndMergeConsumerCoverage();

        TestUtils.setupRawCommodityMap(first);
        TestUtils.setupCommodityResizeDependencyMap(first);
    }

    @Test
    public void testProviderList() {
        Ede engine = new Ede();
        Set<ShoppingList> shoppingListSet = new HashSet<>();
        shoppingListSet.add(shoppingListOfVm1);
        shoppingListSet.add(shoppingListOfVm2);
        Map<Long, Set<Long>> providerList = engine.getProviderLists(shoppingListSet, first, -1);
        // pm3 can only fit shoppingListOfVm2
        assertEquals(2, providerList.get(1L).size());
        assertEquals(3, providerList.get(2L).size());
    }

    /**
     *
     * This test verifies that when resizes occur before replay, we do not provision and suspend
     * the same entity.
     */
    @Test
    public void testNoSuspensionWhenResizeBecauseOfReplay() {
        Ledger ledger = new Ledger(first);
        List<Action> resizes = Resizer.resizeDecisions(first, ledger);
        // assert presence of resizes
        assertFalse(resizes.isEmpty());

        Deactivate deactivate = new Deactivate(first, pm1, shoppingListOfVm1.getBasket());
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(),
                                                    ImmutableList.of(deactivate));
        // assert absence of replayed suspension
        assertTrue(replayActions.tryReplayReduceSupplyActions(first, ledger,
                                                 SuspensionsThrottlingConfig.DEFAULT).isEmpty());

        // assert absence of provision/activates
        List<Action> provisionActions = Provision.provisionDecisions(first, ledger);
        assertTrue(provisionActions.isEmpty());

        // assert absence of suspension
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        List<Action> suspendActions = suspension.suspensionDecisions(first, ledger);
        assertTrue(suspendActions.isEmpty());
    }

    /**
     *
     * This test verifies that when resizes occur after replay, we provision and suspend
     * the same entity.
     */
    @Test
    public void testSuspensionWhenResizeAfterOfReplay() {

        Deactivate deactivate = new Deactivate(first, pm1, shoppingListOfVm1.getBasket());
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(),
                                                    ImmutableList.of(deactivate));
        Ledger ledger = new Ledger(first);
        // validate that suspension was replayed
        assertFalse(replayActions.tryReplayReduceSupplyActions(first, ledger,
                                                 SuspensionsThrottlingConfig.DEFAULT).isEmpty());

        // validate that there is a resize
        List<Action> resizes = Resizer.resizeDecisions(first, ledger);
        assertFalse(resizes.isEmpty());

        // assert presence of 1 activate
        List<Action> provisionActions = Provision.provisionDecisions(first, ledger);
        assertEquals(1, provisionActions.stream().filter(Activate.class::isInstance).count());

        // assert absence of 1 suspension
        Suspension suspension = new Suspension(SuspensionsThrottlingConfig.DEFAULT);
        List<Action> suspendActions = suspension.suspensionDecisions(first, ledger);
        assertTrue(suspendActions.isEmpty());
    }

    /**
     * When replaying host suspend action and host is controllable true,
     * a deactivate host action and a move VM action should be generated.
     */
    @Test
    public void testReplayDeactivateWhenProviderControllableTrue() {
        pm1.getSettings().setControllable(true);
        Deactivate deactivate = new Deactivate(first, pm1, shoppingListOfVm1.getBasket());
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(),
            ImmutableList.of(deactivate));
        Ledger ledger = new Ledger(first);
        List<Action> actions = replayActions.tryReplayReduceSupplyActions(first, ledger,
            SuspensionsThrottlingConfig.DEFAULT);
        assertThat(actions.size(), is(2));
        assertTrue(actions.get(0) instanceof Deactivate);
        assertThat(((Deactivate)actions.get(0)).getTarget(), is(pm1));
        assertTrue(actions.get(1) instanceof Move);
        assertThat(((Move)actions.get(1)).getTarget(), is(shoppingListOfVm1));
    }

    /**
     * When replaying host suspend action and host is controllable false and the VM on it is not daemon,
     * only a deactivate host action should be generated.
     */
    @Test
    public void testReplayDeactivateWhenProviderControllableFalse() {
        pm1.getSettings().setControllable(false);
        vm1.getSettings().setDaemon(false);
        Deactivate deactivate = new Deactivate(first, pm1, shoppingListOfVm1.getBasket());
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(),
            ImmutableList.of(deactivate));
        Ledger ledger = new Ledger(first);
        List<Action> actions = replayActions.tryReplayReduceSupplyActions(first, ledger,
            SuspensionsThrottlingConfig.DEFAULT);
        assertThat(actions.size(), is(1));
        assertTrue(actions.get(0) instanceof Deactivate);
        assertThat(((Deactivate)actions.get(0)).getTarget(), is(pm1));
    }
}
