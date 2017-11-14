package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyIDTest {

    @Test
    public void testDatabaseName() {
        final TopologyID id = new TopologyID(1, 2, TopologyType.SOURCE);
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
