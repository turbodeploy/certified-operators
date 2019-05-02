package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;

public class TopologyRelationshipRecorderTest {

    private final GraphDBExecutor graphDBExecutor = Mockito.mock(GraphDBExecutor.class);

    private final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);

    @Test
    public void testDiags() throws DiagnosticsException, TopologyEntitiesException {
        final TopologyDatabase topologyDatabase = Mockito.mock(TopologyDatabase.class);
        when(result.getRealtimeDatabase())
            .thenReturn(Optional.of(topologyDatabase));
        final TopologyID topologyId = new TopologyID(1, 2, TopologyID.TopologyType.SOURCE);
        when(result.getRealtimeTopologyId()).thenReturn(Optional.of(topologyId));

        TopologyRelationshipRecorder relationshipRecorder =
            new TopologyRelationshipRecorder(graphDBExecutor, result);

        Multimap<String, String> rels = HashMultimap.create();
        rels.put("VM", "PM");
        rels.put("VM", "ST");
        rels.put("PM", "ST");
        rels.put("PM", "Cluster");
        relationshipRecorder.setGlobalSupplyChainProviderRels(rels);

        assertEquals(rels, relationshipRecorder.getGlobalSupplyChainProviderStructures());

        List<String> diags = relationshipRecorder.collectDiags();

        TopologyRelationshipRecorder restored = new TopologyRelationshipRecorder(graphDBExecutor, result);
        restored.restoreDiags(diags);

        assertEquals(rels, restored.getGlobalSupplyChainProviderStructures());
    }
}
