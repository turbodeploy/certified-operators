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

public class ReplayActionsTest {

    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final Basket PMtoVM = new Basket(CPU,
                                new CommoditySpecification(1), // MEM
                                new CommoditySpecification(2), // Datastore commodity with key 1
                                new CommoditySpecification(3));// Datastore commodity with key 2
    private static final Basket STtoVM = new Basket(
                                new CommoditySpecification(4), // Storage Amount (no key)
                                new CommoditySpecification(5));// DSPM access commodity with key A

    private @NonNull Economy first;
    private @NonNull Economy second;
    private @NonNull Topology firstTopology;
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;

    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();

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
        ShoppingList[] shoppingLists = first.getMarketsAsBuyer(vm)
                                            .keySet().toArray(new ShoppingList[3]);
        shoppingLists[0].move(pm1);
        shoppingLists[0].setQuantity(0, 42);
        shoppingLists[0].setPeakQuantity(0, 42);
        shoppingLists[0].setQuantity(1, 100);
        shoppingLists[0].setPeakQuantity(1, 100);
        shoppingLists[0].setQuantity(2, 1);
        shoppingLists[0].setPeakQuantity(2, 1);
        shoppingLists[0].setQuantity(3, 1);
        shoppingLists[0].setPeakQuantity(3, 1);
        shoppingLists[1].move(st1);
        shoppingLists[1].setQuantity(0, 1000);
        shoppingLists[1].setPeakQuantity(0, 1000);
        shoppingLists[1].setQuantity(0, 1);
        shoppingLists[1].setPeakQuantity(0, 1);
        shoppingLists[2].move(st2);
        shoppingLists[2].setQuantity(0, 1000);
        shoppingLists[2].setPeakQuantity(0, 1000);
        shoppingLists[2].setQuantity(0, 1);
        shoppingLists[2].setPeakQuantity(0, 1);
        pm1.getCommoditySold(CPU).setCapacity(100);
        first.getCommodityBought(shoppingLists[0],CPU).setQuantity(42);

        // Shopping lists already on supplier need to be movable for this test
        // PM2 should accept new customers for this test
        // ReplayActions checks these before replaying action
        shoppingLists[0].setMovable(true);
        shoppingLists[1].setMovable(true);
        shoppingLists[2].setMovable(true);
        pm1.getSettings().setCanAcceptNewCustomers(true);
        pm2.getSettings().setCanAcceptNewCustomers(true);
        st1.getSettings().setCanAcceptNewCustomers(true);
        st2.getSettings().setCanAcceptNewCustomers(true);

        firstTopology = new Topology();
        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("traderOids_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField = Topology.class
                                                   .getDeclaredField("unmodifiableTraderOids_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);

        second = cloneEconomy(first);
    }

    public @NonNull Economy cloneEconomy(@NonNull Economy economy) throws IOException,
                    ClassNotFoundException {
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
        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        for (ShoppingList sl : buying.keySet()) {
            ShoppingList newSl = replayActions.translateShoppingList(sl, second, second.getTopology());
            assertEquals(true, sl.getBasket().equals(newSl.getBasket()));
        }
    }

    @Test
    public void testTranslateMarket() {
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        Trader newVm = replayActions.translateTrader(vm, second, "testTranslateMarket");
        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        Map<ShoppingList,Market> newBuying = first.getMarketsAsBuyer(newVm);
        assertEquals(buying.keySet().size(), newBuying.keySet().size());
    }

    @Test
    public void testTranslateCommoditySold() {
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        pm1.getCommoditySold(CPU).setCapacity(100);
        Trader newPm1 = replayActions.translateTrader(pm1, second, "testTranslateCommoditySold");
        CommoditySold newCommSold = replayActions.translateCommoditySold(newPm1,
                               CPU,
                               pm1.getCommoditySold(CPU),
                               second, second.getTopology());
        assertEquals(100, newCommSold.getCapacity(), .01);
    }

    @Test
    public void testReplayMove() {
        List<Action> actions = new LinkedList<>();
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        replayActions.setActions(actions);
        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        ShoppingList pmShoppingList = null;
        for (ShoppingList sl : buying.keySet()) {
            pmShoppingList = sl;
            break;
        }
        pmShoppingList.setMovable(true);
        Move move = new Move(first, pmShoppingList, pm2);
        actions.add(move);
        replayActions.replayActions(second, new Ledger(second));
        assertEquals(1, replayActions.getActions().size());
    }

    @Test
    public void testReplayMoveAlreadyTaken() {
        // simulate move happening in main market
        List<Action> actions = new LinkedList<>();
        ReplayActions replayActions = new ReplayActions();
        replayActions.setTraderOids(traderOids);
        replayActions.setActions(actions);
        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        ShoppingList pmShoppingList = null;
        for (ShoppingList sl : buying.keySet()) {
            pmShoppingList = sl;
            break;
        }
        Move move = new Move(first, pmShoppingList, pm2);
        actions.add(move);
        replayActions.replayActions(second, new Ledger(second));
        assertEquals(1, replayActions.getActions().size());

        // replay action which has already taken place
        try {
            @NonNull Economy third = cloneEconomy(second);
            ReplayActions replayActionsSecond = new ReplayActions();
            replayActionsSecond.setTraderOids(second.getTopology().getTraderOids());
            replayActionsSecond.setActions(actions);
            replayActionsSecond.replayActions(third, new Ledger(third));
            assertEquals(0, replayActionsSecond.getActions().size());
        } catch (ClassNotFoundException | IOException e) {
            assertTrue(false);
        }
    }

}
