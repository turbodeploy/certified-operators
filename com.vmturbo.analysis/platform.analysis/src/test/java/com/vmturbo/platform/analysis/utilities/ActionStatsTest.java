package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.Topology;

public class ActionStatsTest {
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final Basket PMtoVM = new Basket(CPU,
                                new CommoditySpecification(1), // MEM
                                new CommoditySpecification(2), // Datastore commodity with key 1
                                new CommoditySpecification(3));// Datastore commodity with key 2
    private static final Basket STtoVM = new Basket(
                                new CommoditySpecification(4), // Storage Amount (no key)
                                new CommoditySpecification(5));// DSPM access commodity with key A
    public static final long ANALYSIS_ID = 1234L;

    private @NonNull Economy first;
    private @NonNull Topology firstTopology;
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;

    private @NonNull Map<@NonNull Long, @NonNull Trader> traderOids = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        first = new Economy();
        vm = first.addTrader(0, TraderState.ACTIVE, new Basket(), PMtoVM, STtoVM, STtoVM);
        pm1 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|some-uuid-1");
        pm2 = first.addTrader(1, TraderState.ACTIVE, PMtoVM);
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|some-uuid-2");
        Trader st1 = first.addTrader(2, TraderState.ACTIVE, STtoVM);
        Trader st2 = first.addTrader(2, TraderState.ACTIVE, STtoVM);

        vm.setOid(1L);
        pm1.setOid(2L);
        pm2.setOid(3L);
        st1.setOid(4L);
        st2.setOid(5L);

        traderOids.put(1L, vm);
        traderOids.put(2L, pm1);
        traderOids.put(3L, pm2);
        traderOids.put(4L, st1);
        traderOids.put(5L, st2);
        ShoppingList[] shoppingLists = first.getMarketsAsBuyer(vm)
                                            .keySet().toArray(new ShoppingList[3]);
        shoppingLists[0].move(pm1);
        shoppingLists[1].move(st1);
        shoppingLists[2].move(st2);
        pm1.getCommoditySold(CPU).setCapacity(100);
        first.getCommodityBought(shoppingLists[0],CPU).setQuantity(42);

        firstTopology = new Topology();
        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("tradersByOid_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField = Topology.class
                                                   .getDeclaredField("unmodifiableTradersByOid_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);

    }

    @Test
    public void testMoveStats() {
        List<Action> actions = new ArrayList<>();
        ActionStats actionStats = new ActionStats(actions, ANALYSIS_ID);

        Map<ShoppingList,Market> buying = first.getMarketsAsBuyer(vm);
        ShoppingList pmShoppingList = null;
        for (ShoppingList sl : buying.keySet()) {
            pmShoppingList = sl;
            break;
        }

        Move move = new Move(first, pmShoppingList, pm2);
        actions.add(move);

        String logPhase1 = actionStats.phaseLogEntry("Phase1");
        assertEquals(true, logPhase1.contains("1 " + move.getType () + " (PhysicalMachine:1"));
        assertTrue(logPhase1.contains(Long.toString(ANALYSIS_ID)));

        actions.add(move);
        String logPhase2 = actionStats.phaseLogEntry("Phase2");
        assertEquals(true, logPhase2.contains("1 " + move.getType () + " (PhysicalMachine:1"));

        String finalEntry = actionStats.finalLogEntry();
        assertEquals(true, finalEntry.contains("2 " + move.getType () + " (PhysicalMachine:2"));
        assertTrue(finalEntry.contains(Long.toString(ANALYSIS_ID)));
    }
}
