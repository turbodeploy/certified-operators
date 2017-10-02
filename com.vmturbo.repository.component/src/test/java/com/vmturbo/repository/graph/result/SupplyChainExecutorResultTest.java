package com.vmturbo.repository.graph.result;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SupplyChainExecutorResultTest {

    @Test
    public void testPrune() {
        // Create a result for a traversal starting with a VM:
        // VM "0" -> (PM "1", Storage "2") - The VM is buying from a PM and a Storage.
        // Storage "2" -> (PM "1", PM "4") - Two PMs are buying from the storage.
        // Storage "3" has no neighbour info.
        SupplyChainQueryResult vmResult = new SupplyChainQueryResult("virtualMachine",
            Collections.singleton("0"),
            ImmutableMap.of("physicalMachine",
                Collections.singletonList(new SupplyChainNeighbour("1", 2)),
                "storage", Collections.singletonList(new SupplyChainNeighbour("2", 2))));
        SupplyChainQueryResult storageResult = new SupplyChainQueryResult( "storageResult",
            Sets.newHashSet("2", "3"),
            ImmutableMap.of(
                "physicalMachine", Lists.newArrayList(
                        new SupplyChainNeighbour("1", 3),
                        new SupplyChainNeighbour("4", 3))));

        Set<String> retainIds =
                SupplyChainExecutorResult.getRetainOids(Stream.of(vmResult, storageResult));

        // The original VM ("0"), and the "unused" storage ("3") should not be retained, since
        // they do not appear in any neighbour list.

        // The the physical machine and storage the original VM relates to ("1" and "2") must
        // be retained.
        //
        // The physical machine "4" has depth 3 - which is less than the depth 2 of physical
        // machine "1", so it should be pruned.
        assertEquals(Sets.newHashSet("1", "2"), retainIds);
    }
}
