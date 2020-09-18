package com.vmturbo.action.orchestrator.store.pipeline;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * Tests for {@link ActionPipelineContext}.
 */
public class ActionPipelineContextTest {

    private static final long PLAN_ID = 1234L;
    private static final ActionPipelineContext liveContext = new ActionPipelineContext(PLAN_ID,
        TopologyType.REALTIME,
        ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyContextId(777777L)
                    .setTopologyId(5678L)
                    .setTopologyType(TopologyType.REALTIME)
                    .addAllAnalysisType(Arrays.asList(
                        AnalysisType.MARKET_ANALYSIS, AnalysisType.WASTED_FILES)))).build());
    private static final ActionPipelineContext planContext = new ActionPipelineContext(PLAN_ID,
        TopologyType.PLAN,
        ActionPlanInfo.newBuilder()
            .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                .setTopologyContextId(999999L)).build());

    /**
     * testMarketPlanName.
     */
    @Test
    public void testMarketPlanName() {
        assertEquals("REALTIME Action Pipeline (actionPlan: 1234, context: 777777, topology: 5678, [MARKET_ANALYSIS, WASTED_FILES])",
            liveContext.getPipelineName());
    }

    /**
     * testBuyRIPlanName.
     */
    @Test
    public void testBuyRIPlanName() {
        assertEquals("PLAN Action Pipeline (actionPlan: 1234, context: 999999 [BUY_RI])",
            planContext.getPipelineName());
    }

    /**
     * testGetActionPlanContextType.
     */
    @Test
    public void testGetActionPlanContextType() {
        assertEquals(TopologyType.REALTIME, liveContext.getActionPlanTopologyType());
        assertEquals(TopologyType.PLAN, planContext.getActionPlanTopologyType());
    }

    /**
     * testGetTopologyContextId.
     */
    @Test
    public void testGetTopologyContextId() {
        assertEquals(777777L, liveContext.getTopologyContextId());
        assertEquals(999999L, planContext.getTopologyContextId());
    }

    /**
     * testGetActionPlanInfo.
     */
    @Test
    public void testGetActionPlanInfo() {
        assertEquals(TypeInfoCase.MARKET, liveContext.getActionPlanInfo().getTypeInfoCase());
        assertEquals(TypeInfoCase.BUY_RI, planContext.getActionPlanInfo().getTypeInfoCase());
    }
}