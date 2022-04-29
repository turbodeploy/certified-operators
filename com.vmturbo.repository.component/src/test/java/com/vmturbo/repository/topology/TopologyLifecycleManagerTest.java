package com.vmturbo.repository.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.plan.db.SQLPlanEntityStore;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyLifecycleManagerTest {

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private final long realtimeContextId = 7;

    private ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    private TopologyLifecycleManager topologyLifecycleManager;

    private SQLPlanEntityStore sqlPlanEntityStore = mock(SQLPlanEntityStore.class);

    private static final String DATABASE_NAME = "Tturbonomic";

    @Before
    public void setup() {
        topologyLifecycleManager = new TopologyLifecycleManager(realtimeContextId, scheduler,
            liveTopologyStore, 0, 2, 2, 1, sqlPlanEntityStore, false);
    }

    @Test
    public void testNoTopology() {
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());
        assertFalse(topologyLifecycleManager.getTopologyId(1,
                TopologyType.PROJECTED).isPresent());
    }

    @Test
    public void testRegisterTopology() {
        final TopologyID source =
            new TopologyID(1L, 1L, TopologyType.SOURCE);
        final TopologyID projected =
            new TopologyID(1L, 2L, TopologyType.PROJECTED);
        topologyLifecycleManager.registerTopology(source);
        topologyLifecycleManager.registerTopology(projected);

        assertEquals(source,
                topologyLifecycleManager.getTopologyId(1L, TopologyType.SOURCE).get());
        assertEquals(projected,
                topologyLifecycleManager.getTopologyId(1L, TopologyType.PROJECTED).get());
    }

    @Test
    public void testRealtimeTopologyID() {
        // verify that "isPresent" is false when there is no topology yet.
        assertFalse(topologyLifecycleManager.getRealtimeTopologyId().isPresent());

        // register a topology
        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 1L, TopologyType.SOURCE));
        Optional<TopologyID> optionalDynamicTopologyID = topologyLifecycleManager.getRealtimeTopologyId();
        assertTrue(optionalDynamicTopologyID.isPresent());
        TopologyID dynamicTopologyID = optionalDynamicTopologyID.get();

        topologyLifecycleManager.registerTopology(new TopologyID(realtimeContextId, 2L, TopologyType.SOURCE));
        assertEquals(2L, dynamicTopologyID.getTopologyId());
    }

    // get topology set with both real time and plan topologies.
    private Set<String> getTopologyIDSet(int n, long scale) {
        List<TopologyID> topologyIDS = Lists.newArrayList();
        // reverse the order to make sure sorting are involved.
        for (int i =n; i >= 1; i--) {
            topologyIDS.add(new TopologyID(realtimeContextId, i, TopologyType.SOURCE));
            topologyIDS.add(new TopologyID(realtimeContextId, i +  scale, TopologyType.PROJECTED));
        }
        // Always added not real time topology (e.g. for plans), and they should be ignored
        topologyIDS.add(new TopologyID(realtimeContextId + 1, n + 1, TopologyType.SOURCE));
        topologyIDS.add(new TopologyID(realtimeContextId + 1, n + 1 + scale, TopologyType.PROJECTED));
        return topologyIDS.stream().map(tid -> "topology" + tid.toCollectionNameSuffix()).collect(Collectors.toSet());
    }

}
