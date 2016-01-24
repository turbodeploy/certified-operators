package com.vmturbo.platform.analysis.topology;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utility.CollectionTests;

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
            assertEquals(nMarkets[i], topology.getEconomy().getMarketsAsBuyer(trader).keySet().size());
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
        @NonNull Set<Entry<@NonNull Trader, @NonNull String>> entries = new LegacyTopology().getUuids().entrySet();
        Entry<@NonNull Trader, @NonNull String> entry = new Entry<Trader, String>() {
            @Override public Trader getKey() {return null;}
            @Override public String getValue() {return null;}
            @Override public String setValue(String value) {return null;}};

            CollectionTests.verifyUnmodifiableValidOperations(entries, entry); // TODO: test bimap operations instead
            CollectionTests.verifyUnmodifiableInvalidOperations(entries, entry);
        @NonNull Set<Entry<@NonNull String, @NonNull Trader>> invEntries = new LegacyTopology().getUuids().inverse().entrySet();
        Entry<@NonNull String, @NonNull Trader> invEntry = new Entry<String, Trader>() {
            @Override public String getKey() {return null;}
            @Override public Trader getValue() {return null;}
            @Override public Trader setValue(Trader value) {return null;}};

            CollectionTests.verifyUnmodifiableValidOperations(invEntries, invEntry); // TODO: test bimap operations instead
            CollectionTests.verifyUnmodifiableInvalidOperations(invEntries, invEntry);
    }

    @Test
    public final void testGetNames() {
        @NonNull Set<Entry<@NonNull Trader, @NonNull String>> entries = new LegacyTopology().getNames().entrySet();
        Entry<@NonNull Trader, @NonNull String> entry = new Entry<Trader, String>() {
            @Override public Trader getKey() {return null;}
            @Override public String getValue() {return null;}
            @Override public String setValue(String value) {return null;}};

        CollectionTests.verifyUnmodifiableValidOperations(entries, entry); // TODO: test map operations instead
        CollectionTests.verifyUnmodifiableInvalidOperations(entries, entry);
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
