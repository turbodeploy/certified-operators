package com.vmturbo.repository.graph.result;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class SupplyChainQueryResultTest {

    @Test
    public void testPrune() {
        SupplyChainQueryResult res = new SupplyChainQueryResult("type",
                Sets.newHashSet("1", "2", "3"),
                ImmutableMap.of(
                    "neighbour1", Collections.singletonList(new SupplyChainNeighbour("4", 0)),
                    "neighbour2", Collections.singletonList(new SupplyChainNeighbour("5", 0))));

        SupplyChainQueryResult pruned = res.prune(Sets.newHashSet("1", "5", "1000"));
        assertEquals(Sets.newHashSet("1"), pruned.getInstances());

        // Both types should still be represented.
        assertEquals(Sets.newHashSet("neighbour1", "neighbour2"), pruned.getNeighbourTypes());

        Map<String, List<SupplyChainNeighbour>> newNeighbourInstances = pruned.getNeighbourInstances();
        assertEquals(2, newNeighbourInstances.size());
        // The neighbour with ID 4 should have gotten pruned.
        assertEquals(Collections.emptyList(), newNeighbourInstances.get("neighbour1"));
        // The neighbour with ID 5 should still be there.
        assertEquals("5", newNeighbourInstances.get("neighbour2").get(0).getId());
    }

}
