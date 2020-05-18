package com.vmturbo.cost.component.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

public class TopologyInfoTrackerTest {


    @Test
    public void onTopologySummarySkipSummary() {

        final TopologyInfoTracker topologyInfoTracker = new TopologyInfoTracker(
                TopologyInfoTracker.SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR);

        final TopologySummary topologySummary = TopologySummary.newBuilder()
                .setSuccess(TopologyBroadcastSuccess.newBuilder())
                .setTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyType(TopologyType.PLAN))
                .build();

        // broadcast new topology
        topologyInfoTracker.onTopologySummary(topologySummary);

        // Check that the topology is not tracked
        assertThat(topologyInfoTracker.getLatestTopologyInfo(), equalTo(Optional.empty()));
    }

    @Test
    public void onTopologySummaryFirstBroadcast() {

        final TopologyInfoTracker topologyInfoTracker = new TopologyInfoTracker(
                TopologyInfoTracker.SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyContextId(123L)
                .setTopologyId(456)
                .build();
        final TopologySummary topologySummary = TopologySummary.newBuilder()
                .setSuccess(TopologyBroadcastSuccess.newBuilder())
                .setTopologyInfo(topologyInfo)
                .build();

        // broadcast new topology
        topologyInfoTracker.onTopologySummary(topologySummary);

        // Check that the topology is not tracked
        assertThat(topologyInfoTracker.getLatestTopologyInfo(), equalTo(Optional.of(topologyInfo)));
    }



    @Test
    public void isLatestTopology() {

        final TopologyInfoTracker topologyInfoTracker = new TopologyInfoTracker(
                TopologyInfoTracker.SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR);



        final TopologyInfo latestTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyContextId(123L)
                .setTopologyId(456L)
                .setCreationTime(789L)
                .build();
        final TopologySummary topologySummary = TopologySummary.newBuilder()
                .setSuccess(TopologyBroadcastSuccess.newBuilder())
                .setTopologyInfo(latestTopologyInfo)
                .build();

        // broadcast new topology
        topologyInfoTracker.onTopologySummary(topologySummary);

        final TopologyInfo staleTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyContextId(123L)
                .setTopologyId(455L)
                .setCreationTime(234L)
                .build();


        assertTrue(topologyInfoTracker.isLatestTopology(latestTopologyInfo));
        assertFalse(topologyInfoTracker.isLatestTopology(staleTopologyInfo));
    }
}
