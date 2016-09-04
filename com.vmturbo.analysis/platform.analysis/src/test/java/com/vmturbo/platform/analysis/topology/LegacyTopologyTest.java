package com.vmturbo.platform.analysis.topology;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.BiMap;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utility.MapTests;

import junitparams.JUnitParamsRunner;

/**
 * A test case for the {@link LegacyTopology} class.
 */
@RunWith(JUnitParamsRunner.class)
public class LegacyTopologyTest {
    // Fields

    // Methods

    @Test
    public final void testLegacyTopology() {
        @NonNull LegacyTopology topology = new LegacyTopology();
        assertTrue(topology.getEconomy().getTraders().isEmpty());
        assertTrue(topology.getEconomy().getMarkets().isEmpty());
        assertTrue(topology.getUuids().isEmpty());
        assertTrue(topology.getNames().isEmpty());
        assertTrue(topology.getTraderTypes().isEmpty());
        assertTrue(topology.getCommodityTypes().isEmpty());
    }

    @Test
    public final void testAddTrader_NormalInput() {
        @NonNull String[] ids = {"id1","id2","id3"};
        @NonNull String[] names = {"name1", "name2", "name2"};
        @NonNull String[] tTypes = {"type1", "type2", "type2"};
        @NonNull String[] cTypes = {"a","b","c","d","e","f"};
        @NonNull TraderState[] states = {TraderState.ACTIVE,TraderState.INACTIVE,TraderState.ACTIVE};
        @NonNull String[][] cTypeGroups = {{"a","b","c"},{"d","e"},{"b","d","f"}};
        int[] tTypeSizes = {1,2,2};
        int[] cTypeSizes = {3,5,6};
        @NonNull Trader[] traders = new Trader[ids.length];

        @NonNull LegacyTopology topology = new LegacyTopology();
        for (int i = 0 ; i < ids.length ; ++i) {
            traders[i] = topology.addTrader(ids[i], names[i], tTypes[i], states[i], Arrays.asList(cTypeGroups[i]));

            assertEquals(i+1, topology.getEconomy().getTraders().size());
            assertEquals(0, topology.getEconomy().getMarkets().size());
            assertEquals(i+1, topology.getUuids().size());
            assertEquals(i+1, topology.getNames().size());
            assertEquals(tTypeSizes[i], topology.getTraderTypes().size());
            assertEquals(cTypeSizes[i], topology.getCommodityTypes().size());

            for (int j = 0 ; j <= i ; ++j) {
                assertTrue(topology.getEconomy().getTraders().contains(traders[j]));
                assertEquals(ids[j], topology.getUuids().get(traders[j]));
                assertSame(traders[j], topology.getUuids().inverse().get(ids[j]));
                assertEquals(names[j], topology.getNames().get(traders[j]));
                assertEquals(tTypeSizes[j]-1, traders[j].getType());
                assertSame(states[j], traders[j].getState());
            }

            for (int j = 0 ; j < tTypeSizes[i] ; ++j) {
                assertEquals(tTypes[j], topology.getTraderTypes().getName(j));
                assertEquals(j, topology.getTraderTypes().getId(tTypes[j]));
            }

            for (int j = 0 ; j < cTypeSizes[i] ; ++j) {
                assertEquals(cTypes[j], topology.getCommodityTypes().getName(j));
                assertEquals(j, topology.getCommodityTypes().getId(cTypes[j]));
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testAddTrader_InvalidInput() {
        @NonNull LegacyTopology topology = new LegacyTopology();
        topology.addTrader("id", "name1", "type1", TraderState.ACTIVE,Arrays.asList("a","b"));
        topology.addTrader("id", "name2", "type2", TraderState.ACTIVE,Arrays.asList("c","d"));
    }

    @Test
    public final void testAddBasketBought() {
        @NonNull String[] cTypes = {"a","b","c","d","e","f","g"};
        @NonNull String[][] cTypeGroups = {{},{"a","c"},{"a","c"},{"d","e","f"},{"a","b"},{"e","f","g"}};
        int[] nMarkets = {1,2,2,3,4,5};
        int[] cTypeSizes = {2,3,3,6,6,7};
        @NonNull LegacyTopology topology = new LegacyTopology();
        @NonNull Trader trader = topology.addTrader("id", "name", "type", TraderState.ACTIVE, Arrays.asList("a","b"));

        for (int i = 0 ; i < cTypeGroups.length ; ++i) {
            topology.addBasketBought(trader, Arrays.asList(cTypeGroups[i]));

            assertEquals(1, topology.getEconomy().getTraders().size());
            assertEquals(nMarkets[i], topology.getEconomy().getMarkets().size());
            assertEquals(nMarkets[i], topology.getEconomy().getMarketsAsBuyer(trader).values().stream().distinct().count());
            assertEquals(i+1, topology.getEconomy().getMarketsAsBuyer(trader).size());
            assertEquals(1, topology.getTraderTypes().size());
            assertEquals(cTypeSizes[i], topology.getCommodityTypes().size());

            for (int j = 0 ; j < cTypeSizes[i] ; ++j) {
                assertEquals(cTypes[j], topology.getCommodityTypes().getName(j));
                assertEquals(j, topology.getCommodityTypes().getId(cTypes[j]));
            }
        }
    }

    @Test
    public final void testGetEconomy() {
        assertNotNull(new LegacyTopology().getEconomy()); // It's essentially tested together with
            // the add* operations but, may think of something useful to test for the individual
            // method later.
    }

    @Test
    public final void testGetUuids() {
        @NonNull @ReadOnly BiMap<@NonNull Trader, @NonNull String> uuids = new LegacyTopology().getUuids();
        @NonNull Trader trader = new Economy().addTrader(0, TraderState.ACTIVE, new Basket());
        @NonNull String uuid = "uuid";

        MapTests.verifyUnmodifiableValidOperations(uuids, trader, uuid); // TODO: test bimap operations instead
        MapTests.verifyUnmodifiableInvalidOperations(uuids, trader, uuid);
        @NonNull BiMap<@NonNull String, @NonNull Trader> invUuids = new LegacyTopology().getUuids().inverse();

        MapTests.verifyUnmodifiableValidOperations(invUuids, uuid, trader); // TODO: test bimap operations instead
        MapTests.verifyUnmodifiableInvalidOperations(invUuids, uuid, trader);
    }

    @Test
    public final void testGetNames() {
        @NonNull @PolyRead Map<@NonNull Trader, @NonNull String> names = new LegacyTopology().getNames();
        @NonNull Trader trader = new Economy().addTrader(0, TraderState.ACTIVE, new Basket());
        @NonNull String name = "name";

        MapTests.verifyUnmodifiableValidOperations(names, trader, name);
        MapTests.verifyUnmodifiableInvalidOperations(names, trader, name);
    }

    @Test
    public final void testGetTraderTypes() {
        assertNotNull(new LegacyTopology().getTraderTypes()); // It's essentially tested together with
            // the add* operations but, may think of something useful to test for the individual
            // method later.
    }

    @Test
    public final void testGetCommodityTypes() {
        assertNotNull(new LegacyTopology().getCommodityTypes()); // It's essentially tested together with
            // the add* operations but, may think of something useful to test for the individual
            // method later.
    }

} // end LegacyTopologyTest class
