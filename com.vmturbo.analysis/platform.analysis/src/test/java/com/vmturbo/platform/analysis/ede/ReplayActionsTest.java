package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.topology.Topology;

public class ReplayActionsTest {

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
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;

    private @NonNull Topology firstTopology = new Topology();

    @Before
    public void setUp() throws Exception {
        first = firstTopology.getEconomyForTesting();

        vm = firstTopology.addTrader(0, 0, TraderState.ACTIVE, new Basket(),
                                        Collections.emptyList());
        final ShoppingList[] shoppingLists = {
            firstTopology.addBasketBought(100, vm, VMtoPM),
            firstTopology.addBasketBought(101, vm, VMtoST),
            firstTopology.addBasketBought(102, vm, VMtoST)
        };
        pm1 = firstTopology.addTrader(1, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        pm2 = firstTopology.addTrader(2, 1, TraderState.ACTIVE, VMtoPM,
                                        Collections.singletonList(0L));
        final Trader st1 = firstTopology.addTrader(3, 2, TraderState.ACTIVE, VMtoST,
                                                    Collections.singletonList(0L));
        final Trader st2 = firstTopology.addTrader(4, 2, TraderState.ACTIVE, VMtoST,
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
        Trader newVm = ReplayActions.mapTrader(vm, firstTopology, second.getTopology());
        assertEquals(vm.getEconomyIndex(), newVm.getEconomyIndex());
    }

    @Test
    public void testTranslateMarket() {
        Trader newVm = ReplayActions.mapTrader(vm, firstTopology, second.getTopology());
        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        Map<ShoppingList,Market> newBuying = first.getMarketsAsBuyer(newVm);
        assertEquals(buying.keySet().size(), newBuying.keySet().size());
    }

    @Test
    public void testReplayMove() {
        ShoppingList pmShoppingList =
            first.getMarketsAsBuyer(vm).keySet().toArray(new ShoppingList[3])[0];

        pmShoppingList.setMovable(true);
        Move move = new Move(first, pmShoppingList, pm2);
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(move),
                                                        ImmutableList.of(), firstTopology);
        assertEquals(1, replayActions.replayActions(second).size());
    }

    @Test
    public void testReplayMoveAlreadyTaken() {
        // simulate move happening in main market
        ShoppingList pmShoppingList =
            first.getMarketsAsBuyer(vm).keySet().toArray(new ShoppingList[3])[0];

        Move move = new Move(first, pmShoppingList, pm2);
        ReplayActions replayActions = new ReplayActions(ImmutableList.of(move),
                                                        ImmutableList.of(), firstTopology);
        List<Action> actions = replayActions.replayActions(second);
        assertEquals(1, actions.size());

        // replay action which has already taken place
        try {
            @NonNull Economy third = cloneEconomy(second);
            ReplayActions replayActionsSecond = new ReplayActions(actions, ImmutableList.of(),
                                                                  second.getTopology());
            assertEquals(0, replayActionsSecond.replayActions(third).size());
        } catch (ClassNotFoundException | IOException e) {
            fail();
        }
    }

}
