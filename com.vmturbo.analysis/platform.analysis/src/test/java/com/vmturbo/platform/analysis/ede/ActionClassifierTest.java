package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;

public class ActionClassifierTest {

    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final Basket VMtoPM = new Basket(CPU,
                            new CommoditySpecification(1), // MEM
                            new CommoditySpecification(2), // Datastore commodity with key 1
                            new CommoditySpecification(3));// Datastore commodity with key 2
    private static final Basket VMtoST = new Basket(
                            new CommoditySpecification(4), // Storage Amount (no key)
                            new CommoditySpecification(5));// DSPM access commodity with key A

    private @NonNull Economy first;
    private @NonNull Economy second;
    private @NonNull Topology firstTopology;
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;
    private @NonNull Trader app1;
    private @NonNull Trader container1;
    private @NonNull Trader pod1;

    ActionClassifier classifier;

    @Before
    public void setUp() throws Exception {
        firstTopology = new Topology();
        first = firstTopology.getEconomyForTesting();

        vm = firstTopology.addTrader(1L, 0, TraderState.ACTIVE, new Basket(),
                                        Collections.emptyList());
        final ShoppingList[] shoppingLists = {
            firstTopology.addBasketBought(100, vm, VMtoPM),
            firstTopology.addBasketBought(101, vm, VMtoST),
            firstTopology.addBasketBought(102, vm, VMtoST)
        };
        pm1 = firstTopology.addTrader(2L, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        pm2 = firstTopology.addTrader(3L, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        final Trader st1 = firstTopology.addTrader(4L, 2, TraderState.ACTIVE, VMtoST,
                                        Collections.singletonList(0L));
        final Trader st2 = firstTopology.addTrader(5L, 2, TraderState.ACTIVE, VMtoST,
                                        Collections.singletonList(0L));

        vm.setDebugInfoNeverUseInCode("VirtualMachine|1");
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|2");
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|3");
        st1.setDebugInfoNeverUseInCode("Storage|4");
        st2.setDebugInfoNeverUseInCode("Storage|5");

        shoppingLists[0].move(pm1);
        shoppingLists[0].setQuantity(0, 42);
        shoppingLists[0].setPeakQuantity(0, 42);
        shoppingLists[0].setQuantity(1, 100);
        shoppingLists[0].setPeakQuantity(1, 100);
        shoppingLists[0].setQuantity(2, 1);
        shoppingLists[0].setPeakQuantity(2, 1);
        shoppingLists[0].setQuantity(3, 1);
        shoppingLists[0].setPeakQuantity(3, 1);
        shoppingLists[0].setMovable(true);

        shoppingLists[1].move(st1);
        shoppingLists[1].setQuantity(0, 1000);
        shoppingLists[1].setPeakQuantity(0, 1000);
        shoppingLists[1].setQuantity(0, 1);
        shoppingLists[1].setPeakQuantity(0, 1);
        shoppingLists[1].setMovable(true);

        shoppingLists[2].move(st2);
        shoppingLists[2].setQuantity(0, 1000);
        shoppingLists[2].setPeakQuantity(0, 1000);
        shoppingLists[2].setQuantity(0, 1);
        shoppingLists[2].setPeakQuantity(0, 1);
        shoppingLists[2].setMovable(true);

        pm1.getCommoditySold(CPU).setCapacity(100);
        pm1.getCommoditySold(CPU).setCapacity(100);
        first.getCommodityBought(shoppingLists[0], CPU).setQuantity(42);

        pm1.getSettings().setCanAcceptNewCustomers(true);
        pm2.getSettings().setCanAcceptNewCustomers(true);
        st1.getSettings().setCanAcceptNewCustomers(true);
        st2.getSettings().setCanAcceptNewCustomers(true);

        // Deactivating pm1 for replay suspension test
        // Make sure suspendable is true on it
        pm1.getSettings().setSuspendable(true);

        first.setTopology(firstTopology);

        // providerMustClone segment testing

        pod1 = TestUtils.createContainerPod(first, new double[]{101, 102}, "pod-1");
        Trader pod2 = TestUtils.createContainerPod(first, new double[]{103, 104}, "pod-2");
        container1 = TestUtils.createContainer(first, new double[]{105, 106}, "container-1");
        Trader container2 = TestUtils.createContainer(first, new double[]{107, 108}, "container-2");
        app1 = TestUtils.createApplication(first, new double[]{109, 110}, "app-1");
        Trader app2 = TestUtils.createApplication(first, new double[]{111, 112}, "app-2");
        Trader vapp = TestUtils.createVirtualApplication(first, new double[]{113, 114}, "vapp");

        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.RESPONSE_TIME, TestUtils.TRANSACTION), vapp, new double[]{201, 202}, app1);
        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.RESPONSE_TIME, TestUtils.TRANSACTION), vapp, new double[]{203, 204}, app2);
        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app1, new double[]{205, 206}, container1);
        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), app2, new double[]{207, 208}, container2);
        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), container1, new double[]{209, 210}, pod1);
        TestUtils.createAndPlaceShoppingList(first,
                Arrays.asList(TestUtils.VCPU, TestUtils.VMEM), container2, new double[]{211, 212}, pod2);

        second = cloneEconomy(first);
        classifier = new ActionClassifier(first);
    }

    /**
     * This test captures https://vmturbo.atlassian.net/browse/OM-15496
     * where a host was being deactivated while there were VMs on it.
     * Initially VM vm is buying CPU from pm1. Move from pm1 to pm2 and
     * Deactivate pm1 actions are generated. The classifier should mark
     * Deactivate as non-executable. Next Move is taken and for this economy classifier is
     * run again. This time Deactivate should be marked as executable.
     *
     */
    @Test
    public void testClassifySuspension() {
        List<Action> actions = new LinkedList<>();
        ShoppingList pmShoppingList =
            first.getMarketsAsBuyer(vm).keySet().toArray(new ShoppingList[3])[0];

        pmShoppingList.setMovable(true);
        Move move = new Move(first, pmShoppingList, pm2);
        actions.add(move);
        Deactivate deactivate = new Deactivate(first, pm1, pmShoppingList.getBasket());
        actions.add(deactivate);
        actions.add(new Deactivate(first, app1, app1.getBasketSold()));
        actions.add(new Deactivate(first, container1, container1.getBasketSold()));
        actions.add(new Deactivate(first, pod1, pod1.getBasketSold()));

        classifier.classify(actions, first);
        assertTrue(actions.get(0).isExecutable());
        assertFalse(actions.get(1).isExecutable());
        assertFalse(actions.get(2).isExecutable());  // app1 suspend
        assertFalse(actions.get(3).isExecutable());  // container1 suspend
        assertTrue(actions.get(4).isExecutable());   // pod1 suspend
        move.take();
        try {
            @NonNull
            Economy third = cloneEconomy(first);
            Deactivate thirdDeactivate = new Deactivate(first, pm1, pmShoppingList.getBasket());
            third.populateMarketsWithSellersAndMergeConsumerCoverage();
            ReplayActions thirdReplayActions = new ReplayActions(ImmutableList.of(),
                                                                 ImmutableList.of(thirdDeactivate));
            assertEquals(1,
                thirdReplayActions.tryReplayDeactivateActions(third, new Ledger(third),
                                           SuspensionsThrottlingConfig.DEFAULT).size());
        } catch (ClassNotFoundException | IOException e) {
            fail();
        }
    }

    public @NonNull Economy cloneEconomy(@NonNull Economy economy)
                    throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(economy);
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                            ObjectInputStream in = new ObjectInputStream(bis)) {
                return (Economy)in.readObject();
            }
        }
    }

    @Test
    public void testTranslateTrader() {
        Trader newVm = ReplayActions.mapTrader(vm, second.getTopology());
        assertEquals(vm.getEconomyIndex(), newVm.getEconomyIndex());
    }

    @Test
    public void testTranslateMarket() {
        Trader newVm = ReplayActions.mapTrader(vm, second.getTopology());
        Map<ShoppingList, Market> buying = first.getMarketsAsBuyer(vm);
        Map<ShoppingList, Market> newBuying = first.getMarketsAsBuyer(newVm);
        assertEquals(buying.keySet().size(), newBuying.keySet().size());
    }

}
