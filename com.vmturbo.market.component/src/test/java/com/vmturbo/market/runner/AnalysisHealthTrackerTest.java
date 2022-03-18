package com.vmturbo.market.runner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Tests for Analysis health tracker.
 */
public class AnalysisHealthTrackerTest {

    private static final TopologyInfo rtTopoInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1234L).setTopologyId(1234L).build();
    private static final TopologyInfo planTopoInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1234L).setTopologyId(1234L).setPlanInfo(TopologyDTO.PlanTopologyInfo.getDefaultInstance()).build();

    /**
     * Test that we return healthy when not yet initialized.
     */
    @Test
    public void testHealthyWhenNotInitialized() {
        AnalysisHealthTracker healthTracker = new AnalysisHealthTracker();
        Assert.assertTrue(healthTracker.isAnalysisHealthy());
    }

    /**
     * Test that we return healthy when initialized and the previous cycle did not time out.
     */
    @Test
    public void testHealthyAnalysis() {
        Analysis analysis = mock(Analysis.class);
        when(analysis.getTopologyInfo()).thenReturn(rtTopoInfo);
        when(analysis.isStopAnalysis()).thenReturn(false);
        AnalysisHealthTracker healthTracker = new AnalysisHealthTracker();
        healthTracker.update(analysis);
        Assert.assertTrue(healthTracker.isAnalysisHealthy());
    }

    /**
     * Test that we return unhealthy when initialized and the previous cycle timed out.
     */
    @Test
    public void testUnhealthyAnalysis() {
        Analysis analysis = mock(Analysis.class);
        when(analysis.getTopologyInfo()).thenReturn(rtTopoInfo);
        when(analysis.isStopAnalysis()).thenReturn(true);
        AnalysisHealthTracker healthTracker = new AnalysisHealthTracker();
        healthTracker.update(analysis);
        Assert.assertFalse(healthTracker.isAnalysisHealthy());
    }

    /**
     * Test that plan analysis does not affect health status.
     */
    @Test
    public void testPlanAnalysisDoesNotAffectHealthStatus() {
        Analysis analysis = mock(Analysis.class);
        when(analysis.getTopologyInfo()).thenReturn(planTopoInfo);
        when(analysis.isStopAnalysis()).thenReturn(true);
        AnalysisHealthTracker healthTracker = new AnalysisHealthTracker();
        healthTracker.update(analysis);
        Assert.assertTrue(healthTracker.isAnalysisHealthy());
    }
}
