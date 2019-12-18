package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyIDTest {

    private final TopologyIDFactory topologyIDFactory = new TopologyIDFactory("turbonomic-");

    @Test
    public void testDatabaseName() {
        final TopologyID id = topologyIDFactory.createTopologyID(1, 2, TopologyType.SOURCE);
        assertEquals("turbonomic-topology-1-SOURCE-2", id.toDatabaseName());
        assertEquals(id, TopologyID.fromDatabaseName(id.toDatabaseName()).get());
    }

    @Test
    public void testParseInvalidName() {
        assertFalse(TopologyID.fromDatabaseName("blah").isPresent());
        assertFalse(TopologyID.fromDatabaseName("topology-x-SOURCE-1").isPresent());
        assertFalse(TopologyID.fromDatabaseName("topology-1-BLAH-1").isPresent());
        assertFalse(TopologyID.fromDatabaseName("topology-1-SOURCE-x").isPresent());
    }

}
