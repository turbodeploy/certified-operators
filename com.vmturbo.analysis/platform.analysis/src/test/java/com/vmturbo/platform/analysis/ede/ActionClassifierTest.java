package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.topology.Topology;

public class ActionClassifierTest {

    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final Basket PMtoVM = new Basket(CPU, new CommoditySpecification(1), // MEM
                                                    new CommoditySpecification(2), // Datastore commodity with key 1
                                                    new CommoditySpecification(3));// Datastore commodity with key 2
    private static final Basket STtoVM = new Basket(new CommoditySpecification(4), // Storage Amount (no key)
                                                    new CommoditySpecification(5));// DSPM access commodity with key A

    private @NonNull Economy first;
    private @NonNull Economy second;
    private @NonNull Topology firstTopology;
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;

    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();
    ActionClassifier classifier;

    @Before
    public void setUp() throws Exception {
        first = new Economy();
        vm = first.addTrader(0, TraderState.ACTIVE, new Basket(), PMtoVM, STtoVM, STtoVM);
        pm1 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        pm2 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        Trader st1 = first.addTrader(2, TraderState.ACTIVE, STtoVM);
        Trader st2 = first.addTrader(2, TraderState.ACTIVE, STtoVM);
        traderOids.put(vm, 1L);
        traderOids.put(pm1, 2L);
        traderOids.put(pm2, 3L);
        traderOids.put(st1, 4L);
        traderOids.put(st2, 5L);
        vm.setDebugInfoNeverUseInCode("VirtualMachine|1");
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|2");
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|3");
        st1.setDebugInfoNeverUseInCode("Storage|4");
        st2.setDebugInfoNeverUseInCode("Storage|5");
        ShoppingList[] shoppingLists = first.getMarketsAsBuyer(vm).keySet()
                        .toArray(new ShoppingList[3]);
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

        firstTopology = new Topology();
        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("traderOids_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField =
                                         Topology.class.getDeclaredField("unmodifiableTraderOids_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);

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
        Map<ShoppingList, Market> buying = first.getMarketsAsBuyer(vm);
        ShoppingList pmShoppingList = null;
        for (ShoppingList sl : buying.keySet()) {
            pmShoppingList = sl;
            break;
        }
        pmShoppingList.setMovable(true);
        Move move = new Move(first, pmShoppingList, pm2);
        actions.add(move);
        Deactivate deactivate = new Deactivate(first, pm1, buying.get(pmShoppingList));
        actions.add(deactivate);
        classifier.classify(actions);
        assertEquals(true, actions.get(0).isExecutable());
        assertEquals(false, actions.get(1).isExecutable());
        move.take();
        try {
            @NonNull
            Economy third = cloneEconomy(first);
            List<Action> thirdActions = new LinkedList<>();
            ReplayActions thirdReplayActions = new ReplayActions();
            thirdReplayActions.setTraderOids(traderOids);
            thirdReplayActions.setActions(thirdActions);
            Deactivate thirdDeactivate = new Deactivate(first, pm1, buying.get(pmShoppingList));
            thirdActions.add(thirdDeactivate);
            third.populateMarketsWithSellers();
            thirdReplayActions.replayActions(third, new Ledger(third));
            assertEquals(1, thirdReplayActions.getActions().size());
        } catch (ClassNotFoundException | IOException e) {
            assertTrue(false);
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
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Trader newVm = replayActions.translateTrader(vm, second, "testTranslateTrader");
        assertEquals(vm.getEconomyIndex(), newVm.getEconomyIndex());
    }

    @Test
    public void testTranslateShoppingList() {
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Map<ShoppingList, Market> buying = first.getMarketsAsBuyer(vm);
        for (ShoppingList sl : buying.keySet()) {
            ShoppingList newSl = replayActions.translateShoppingList(sl, second,
                                                                     second.getTopology());
            assertEquals(true, sl.getBasket().equals(newSl.getBasket()));
        }
    }

    @Test
    public void testTranslateMarket() {
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Trader newVm = replayActions.translateTrader(vm, second, "testTranslateMarket");
        Map<ShoppingList, Market> buying = first.getMarketsAsBuyer(vm);
        Map<ShoppingList, Market> newBuying = first.getMarketsAsBuyer(newVm);
        assertEquals(buying.keySet().size(), newBuying.keySet().size());
    }

    @Test
    public void testTranslateCommoditySold() {
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        pm1.getCommoditySold(CPU).setCapacity(100);
        Trader newPm1 = replayActions.translateTrader(pm1, second, "testTranslateCommoditySold");
        CommoditySold newCommSold = replayActions
                        .translateCommoditySold(newPm1, CPU, pm1.getCommoditySold(CPU), second,
                                                second.getTopology());
        assertEquals(100, newCommSold.getCapacity(), .01);
    }

}
