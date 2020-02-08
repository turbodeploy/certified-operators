package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyIDTest {

    @Test
    public void testDatabaseName() {
        final TopologyID id = new TopologyID(1, 2, TopologyType.SOURCE);
        assertEquals("-1-S-2", id.toCollectionNameSuffix());
        assertEquals(id, TopologyID.fromCollectionName("topology" + id.toCollectionNameSuffix()).get());
    }

    @Test
    public void testParseInvalidName() {
        assertFalse(TopologyID.fromCollectionName("blah").isPresent());
        assertFalse(TopologyID.fromCollectionName("topology-x-S-1").isPresent());
        assertFalse(TopologyID.fromCollectionName("topology-1-BLAH-1").isPresent());
        assertFalse(TopologyID.fromCollectionName("topology-1-S-x").isPresent());
    }

}
