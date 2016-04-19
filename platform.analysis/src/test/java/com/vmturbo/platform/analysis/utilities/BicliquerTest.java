package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.collect.Sets;

public class BicliquerTest {

    @Test
    public void testDefaultPrefixes() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        assertEquals(Sets.newHashSet("BC-T1-0"), bicliquer.getBcKeys("A"));
        assertEquals(Sets.newHashSet("BC-T2-0"), bicliquer.getBcKeys("B"));
        assertEquals("BC-T1-0", bicliquer.getBcKey("A", "B"));
        assertEquals("BC-T2-0", bicliquer.getBcKey("B", "A"));
    }

    @Test
    public void testStar() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.edge("A1", "B1");
        bicliquer.edge("A1", "B2");
        bicliquer.edge("A1", "B3");
        bicliquer.compute();
        assertEquals(1, bicliquer.size());
        assertEquals(Sets.newHashSet("A-0"), bicliquer.getBcKeys("A1"));
        assertEquals(Sets.newHashSet(0), bicliquer.getBcIDs("A1"));
        assertEquals("A-0", bicliquer.getBcKey("A1", "B1"));
        assertEquals(Sets.newHashSet("B-0"), bicliquer.getBcKeys("B1"));
        assertEquals(Sets.newHashSet(0), bicliquer.getBcIDs("B1"));
        assertEquals("B-0", bicliquer.getBcKey("B1", "A1"));
    }

    @Test
    public void testCompleteBipartite() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.edge("A1", "B1");
        bicliquer.edge("A1", "B2");
        bicliquer.edge("A1", "B3");
        bicliquer.edge("A2", "B1");
        bicliquer.edge("A2", "B2");
        bicliquer.edge("A2", "B3");
        bicliquer.compute();
        assertEquals(1, bicliquer.size());
        assertEquals(Sets.newHashSet("A-0"), bicliquer.getBcKeys("A1"));
        assertEquals(Sets.newHashSet(0), bicliquer.getBcIDs("A1"));
        assertEquals("A-0", bicliquer.getBcKey("A1", "B1"));
        assertEquals(Sets.newHashSet("B-0"), bicliquer.getBcKeys("B1"));
        assertEquals(Sets.newHashSet(0), bicliquer.getBcIDs("B1"));
        assertEquals("B-0", bicliquer.getBcKey("B1", "A1"));
    }

    @Test
    public void testTwoBicliques() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.edge("A1", "B1");
        bicliquer.edge("A1", "B2");
        bicliquer.edge("A2", "B1");
        bicliquer.edge("A2", "B2");
        bicliquer.edge("A3", "B1");
        bicliquer.compute();
        assertEquals(2, bicliquer.size());
        assertEquals(Sets.newHashSet("A-0"), bicliquer.getBcKeys("A1"));
        assertEquals(Sets.newHashSet(0), bicliquer.getBcIDs("A1"));
        // B1 belongs to two bicliques
        assertEquals(Sets.newHashSet("B-0", "B-1"), bicliquer.getBcKeys("B1"));
        assertEquals(Sets.newHashSet(0, 1), bicliquer.getBcIDs("B1"));
        // All edges belong to bicliques
        assertEquals(0, bicliquer.getBcID("A1", "B1"));
        assertEquals(0, bicliquer.getBcID("A1", "B2"));
        assertEquals(0, bicliquer.getBcID("A2", "B1"));
        assertEquals(0, bicliquer.getBcID("A2", "B2"));
        assertEquals(1, bicliquer.getBcID("A3", "B1"));
        assertEquals("A-0", bicliquer.getBcKey("A1", "B1"));
        assertEquals("A-1", bicliquer.getBcKey("A3", "B1"));
        // TYPE2, TYPE1
        assertEquals(0, bicliquer.getBcID("B1", "A1"));
        assertEquals("B-0", bicliquer.getBcKey("B1", "A1"));
        assertEquals(1, bicliquer.getBcID("B1", "A3"));
        assertEquals("B-1", bicliquer.getBcKey("B1", "A3"));
        // Non existing edge
        assertEquals(-1, bicliquer.getBcID("A3", "B2"));
        assertNull(bicliquer.getBcKey("A3", "B2"));
    }

    @Test(expected=IllegalStateException.class)
    public void testNotComputedYet1() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.size();
    }

    @Test(expected=IllegalStateException.class)
    public void testNotComputedYet2() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.getBicliques();
    }

    @Test(expected=IllegalStateException.class)
    public void testNotComputedYet3() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.getBcKeys("A1");
    }

    @Test(expected=IllegalStateException.class)
    public void testNotComputedYet4() {
        BiCliquer bicliquer = new BiCliquer("A-", "B-");
        bicliquer.getBcIDs("A1");
    }

    @Test(expected=IllegalStateException.class)
    public void testNotComputedYet5() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.getBcID("A", "B");
    }

    @Test(expected=IllegalStateException.class)
    public void testAlreadyComputed1() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.compute();
        bicliquer.compute();
    }

    @Test(expected=IllegalStateException.class)
    public void testAlreadyComputed2() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.compute();
        bicliquer.edge("A", "B");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testUnmodifiableBicliques() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        bicliquer.getBicliques().put(Sets.newHashSet("X"), Sets.newHashSet("Y"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testUnmodifiableBicliqueNode1() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        bicliquer.getBicliques().keySet().iterator().next().add("X");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testUnmodifiableNode2() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        bicliquer.getBicliques().values().iterator().next().add("X");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testUnmodifiableBcKeys() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        bicliquer.getBcKeys("A").add("X");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testUnmodifiableBcIDs() {
        BiCliquer bicliquer = new BiCliquer();
        bicliquer.edge("A", "B");
        bicliquer.compute();
        bicliquer.getBcIDs("A").add(5);
    }
}
