package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.platform.analysis.utilities.M2Utils.TopologyMapping;

public class TestTopologyMapping {

    private static final String TRADER_NAME_1 = "T-1";
    private static final String TRADER_NAME_2 = "T-2";

    @Test
    public void testTopologyMap() {
        TopologyMapping topo = new TopologyMapping(null);
        topo.addTraderMapping(1, TRADER_NAME_1);
        topo.addTraderMapping(2, TRADER_NAME_2);
        assertEquals(topo.getTraderIndex(TRADER_NAME_1), 1);
        assertEquals(topo.getTraderIndex(TRADER_NAME_2), 2);
        assertEquals(topo.getTraderName(1), TRADER_NAME_1);
        assertEquals(topo.getTraderName(2), TRADER_NAME_2);
    }

    // Adding the same name twice throws IllegalArgumentException
    @Test(expected=IllegalArgumentException.class)
    public void testDuplicateNames() {
        TopologyMapping topo = new TopologyMapping(null);
        topo.addTraderMapping(1, TRADER_NAME_1);
        topo.addTraderMapping(2, TRADER_NAME_1);
    }
}
