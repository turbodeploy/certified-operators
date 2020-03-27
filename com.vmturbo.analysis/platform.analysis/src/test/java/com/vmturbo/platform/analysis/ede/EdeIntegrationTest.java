package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

public class EdeIntegrationTest {

    private static final Basket PMtoVM = new Basket(TestUtils.CPU);
    private static final Basket VMtoAPP = new Basket(TestUtils.VCPU);

    private @NonNull Economy first;
    private @NonNull Topology firstTopology;
    private @Nonnull Trader vm1, vm2, pm1, pm2, pm3;
    ShoppingList shoppingListOfVm2, shoppingListOfVm1;

    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();

    @Before
    public void setUp() throws Exception {
        first = new Economy();
        firstTopology = new Topology();
        first.setTopology(firstTopology);
        vm1 = first.addTrader(0, TraderState.ACTIVE, VMtoAPP, PMtoVM);
        vm2 = first.addTrader(0, TraderState.ACTIVE, new Basket(), PMtoVM);
        pm1 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        pm2 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        pm3 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        vm1.setDebugInfoNeverUseInCode("VirtualMachine|1");
        vm2.setDebugInfoNeverUseInCode("VirtualMachine|2");
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|1");
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|2");
        pm3.setDebugInfoNeverUseInCode("PhysicalMachine|3");
        traderOids.put(vm1, 1L);
        traderOids.put(vm2, 2L);
        traderOids.put(pm1, 3L);
        traderOids.put(pm2, 4L);
        traderOids.put(pm3, 5L);

        shoppingListOfVm1 = first.getMarketsAsBuyer(vm1).keySet().iterator().next();
        shoppingListOfVm2 = first.getMarketsAsBuyer(vm2).keySet().iterator().next();

        shoppingListOfVm1.setQuantity(0, 40).setPeakQuantity(0, 40).setMovable(true);
        shoppingListOfVm1.move(pm1);

        shoppingListOfVm2.setQuantity(0, 10).setPeakQuantity(0, 10).setMovable(true);
        shoppingListOfVm2.move(pm2);

        pm1.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(40);
        pm2.getCommoditySold(TestUtils.CPU).setCapacity(100).setQuantity(10);
        pm3.getCommoditySold(TestUtils.CPU).setCapacity(11).setQuantity(0);
        // to test resize of VCPU on VM1
        vm1.getCommoditySold(TestUtils.VCPU).setCapacity(100).setQuantity(80)
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

        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("traderOids_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField = Topology.class
                .getDeclaredField("unmodifiableTraderOids_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);

    }

    @Test
    public void testProviderList() {
        Ede engine = new Ede();
        Set<ShoppingList> shoppingListSet = new HashSet<>();
        shoppingListSet.add(shoppingListOfVm1);
        shoppingListSet.add(shoppingListOfVm2);
        Map<Long, Set<Long>> providerList =
                engine.getProviderLists(shoppingListSet, first);
        // pm3 can only fit shoppingListOfVm2
        assertTrue(providerList.get(1L).size() == 2);
        assertTrue(providerList.get(2L).size() == 3);
    }

    /**
     *
     * This test verifies that when resizes occur before replay, we do not provision and suspend
     * the same entity.
     */
    @Test
    public void testNoSuspensionWhenResizeBecauseOfReplay() {

        List<Action> actions = new LinkedList<>();
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Deactivate deactivate = new Deactivate(first, pm1,
                first.getMarketsAsBuyer(vm1).values().iterator().next().getBasket());
        actions.add(deactivate);
        replayActions.setActions(actions);

        Ledger ledger = new Ledger(first);
        List<Action> resizes = Resizer.resizeDecisions(first, ledger);
        // assert presence of resizes
        assertTrue(!resizes.isEmpty());

        Ede engine = new Ede();
        engine.setReplayActions(replayActions);
        replayActions.replayActions(first, ledger);
        // assert absence of replayed suspension
        assertTrue(replayActions.getActions().isEmpty());

        // assert absence of provision/activates
        List<Action> provisionActions = Provision.provisionDecisions(first, ledger, engine);
        assertTrue(provisionActions.isEmpty());

        // assert absence of suspension
        Suspension suspension = new Suspension();
        List<Action> suspendActions = suspension.suspensionDecisions(first, ledger, engine);
        assertTrue(suspendActions.isEmpty());
    }

    /**
     *
     * This test verifies that when resizes occur after replay, we provision and suspend
     * the same entity.
     */
    @Test
    public void testSuspensionWhenResizeAfterOfReplay() {

        List<Action> actions = new LinkedList<>();
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Deactivate deactivate = new Deactivate(first, pm1,
                first.getMarketsAsBuyer(vm1).values().iterator().next().getBasket());
        actions.add(deactivate);
        replayActions.setActions(actions);

        Ede engine = new Ede();
        Ledger ledger = new Ledger(first);
        engine.setReplayActions(replayActions);
        replayActions.replayActions(first, ledger);
        // validate that suspension was replayed
        assertTrue(!replayActions.getActions().isEmpty());

        // validate that there is a resize
        List<Action> resizes = Resizer.resizeDecisions(first, ledger);
        assertTrue(!resizes.isEmpty());

        // assert presence of 1 activate
        List<Action> provisionActions = Provision.provisionDecisions(first, ledger, engine);
        assertTrue(provisionActions.stream().filter(Activate.class::isInstance).count() == 1);

        // assert absence of 1 suspension
        Suspension suspension = new Suspension();
        List<Action> suspendActions = suspension.suspensionDecisions(first, ledger, engine);
        assertTrue(suspendActions.isEmpty());
    }
}
