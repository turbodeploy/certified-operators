package com.vmturbo.market.runner;

import static org.junit.Assert.*;

import java.time.LocalDateTime;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommodityBoughtList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.Analysis.AnalysisState;

/**
 * Unit tests for {@link Analysis}.
 */
public class AnalysisTest {

    private long topologyContextId = 1111;
    private long topologyId = 2222;
    private Set<TopologyEntityDTO> EMPTY = ImmutableSet.of();

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
        topologyId += 100;
    }

    /**
     * Test the {@link Analysis} constructor.
     */
    @Test
    public void testConstructor() {
        Analysis analysis =
            new Analysis(topologyContextId, topologyId, EMPTY, TopologyType.PLAN, true);
        assertEquals(topologyContextId, analysis.getContextId());
        assertEquals(topologyId, analysis.getTopologyId());
        assertEquals(EMPTY, analysis.getTopology());
        assertEquals(Analysis.EPOCH, analysis.getStartTime());
        assertEquals(Analysis.EPOCH, analysis.getCompletionTime());
    }

    /**
     * Test the {@link Analysis#execute} method.
     */
    @Test
    public void testExecute() {
        Analysis analysis =
            new Analysis(topologyContextId, topologyId, EMPTY, TopologyType.PLAN, true);
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(analysis.getState(), AnalysisState.SUCCEEDED);
        assertFalse(analysis.getCompletionTime().isBefore(analysis.getStartTime()));
        assertFalse(LocalDateTime.now().isBefore(analysis.getCompletionTime()));

        assertTrue(analysis.getActionPlan().isPresent());
        assertTrue(analysis.getProjectedTopology().isPresent());
        assertTrue(analysis.getPriceIndexMessage().isPresent());
    }

    /**
     * Test the {@link Analysis#execute} method for a failed run.
     */
    @Test
    public void testFailedAnalysis() {
        Set<TopologyEntityDTO> set = Sets.newHashSet(buyer()); // seller is missing
        Analysis analysis =
            new Analysis(topologyContextId, topologyId, set, TopologyType.PLAN, true);
        analysis.execute();
        assertTrue(analysis.isDone());
        assertSame(AnalysisState.FAILED, analysis.getState());
        assertNotNull(analysis.getErrorMsg());

        assertFalse(analysis.getActionPlan().isPresent());
        assertFalse(analysis.getProjectedTopology().isPresent());
        assertFalse(analysis.getPriceIndexMessage().isPresent());
    }

    /**
     * A buyer that buys a negative amount (and therefore causes a failure).
     * @return a buyer
     */
    private TopologyEntityDTO buyer() {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(1000)
            .setOid(7)
            .putCommodityBoughtMap(10, CommodityBoughtList.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(1).build())
                            .setUsed(-1)
                            .build()) // commodity bought
                    .build()) // commodity bought list
            .build(); // buyer
    }

    /**
     * Test that invoking {@link Analysis#execute} multiple times throws an exception.
     */
    @Test
    public void testTwoExecutes() {
        Analysis analysis =
            new Analysis(topologyContextId, topologyId, Sets.newHashSet(), TopologyType.PLAN, true);
        boolean first = analysis.execute();
        boolean second = analysis.execute();
        assertTrue(first);
        assertFalse(second);
    }
}
