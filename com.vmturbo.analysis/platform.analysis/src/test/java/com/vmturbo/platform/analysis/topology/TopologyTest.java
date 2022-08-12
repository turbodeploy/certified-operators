package com.vmturbo.platform.analysis.topology;

import static org.junit.Assert.*;

import java.util.List;
import java.util.ArrayList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utility.MapTests;

import junitparams.JUnitParamsRunner;

/**
 * A test case for the {@link Topology} class.
 */
@RunWith(JUnitParamsRunner.class)
public class TopologyTest {
    // Fields
    private Topology fixture_;

    // Methods

    @Before
    public void setUp() {
        fixture_ = new Topology();
    }

    @Test
    public final void testTopology() {
        @NonNull Topology topology = new Topology();
        assertTrue(topology.getEconomy().getTraders().isEmpty());
        assertTrue(topology.getEconomy().getMarkets().isEmpty());
        assertTrue(topology.getTradersByOid().isEmpty());
        assertTrue(topology.getShoppingListOids().isEmpty());
        assertTrue(topology.getDanglingShoppingLists().isEmpty());
    }

    @Test
    @Ignore
    public final void testAddTrader() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testAddBasketBoughtLongTraderBasket() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testAddBasketBoughtLongTraderBasketLong() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testGetEconomy() {
        assertNotNull(fixture_.getEconomy()); // It's essentially tested together with
            // the add* operations but, may think of something useful to test for the individual
            // method later.
    }

    @Test
    public final void testGetTraderOids() {
        @NonNull Trader trader = new Economy().addTrader(0, TraderState.ACTIVE, new Basket());

        MapTests.verifyUnmodifiableValidOperations(fixture_.getTradersByOid(), 0L, trader); // TODO: test bimap operations instead
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getTradersByOid(), 0L, trader);
    }

    @Test
    public final void testGetShoppingListOids() {
        @NonNull Economy unrelatedEconomy = new Economy();
        @NonNull ShoppingList shoppingList = unrelatedEconomy.addBasketBought(
            unrelatedEconomy.addTrader(0, TraderState.ACTIVE, new Basket()), new Basket());

        MapTests.verifyUnmodifiableValidOperations(fixture_.getShoppingListOids(), shoppingList, 0L); // TODO: test bimap operations instead
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getShoppingListOids(), shoppingList, 0L);
        MapTests.verifyUnmodifiableValidOperations(fixture_.getShoppingListOids().inverse(), 0L, shoppingList); // TODO: test bimap operations instead
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getShoppingListOids().inverse(), 0L, shoppingList);
    }

    @Test
    public final void testGetDanglingShoppingLists() {
        @NonNull List<ShoppingList> list = new ArrayList<>();

        MapTests.verifyUnmodifiableValidOperations(fixture_.getDanglingShoppingLists(), 0L, list);
        MapTests.verifyUnmodifiableInvalidOperations(fixture_.getDanglingShoppingLists(), 0L, list);
    }

    @Test
    @Ignore
    public final void testClear() {
        fail("Not yet implemented"); // TODO
    }

} // end TopologyTest class
