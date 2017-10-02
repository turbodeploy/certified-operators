package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class TopologyRelationshipRecorderTest {

    @Test
    public void testDiags() {
        TopologyRelationshipRecorder relationshipRecorder = new TopologyRelationshipRecorder();

        Multimap<String, String> rels = HashMultimap.create();
        rels.put("VM", "PM");
        rels.put("VM", "ST");
        rels.put("PM", "ST");
        rels.put("PM", "Cluster");
        relationshipRecorder.setGlobalSupplyChainProviderRels(rels);

        assertEquals(rels, relationshipRecorder.getGlobalSupplyChainProviderStructures());

        List<String> diags = relationshipRecorder.collectDiags();

        TopologyRelationshipRecorder restored = new TopologyRelationshipRecorder();
        restored.restoreDiags(diags);

        assertEquals(rels, restored.getGlobalSupplyChainProviderStructures());
    }
}
