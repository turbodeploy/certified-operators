package com.vmturbo.action.orchestrator.store.pipeline;

import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Tests for action pipeline factories.
 */
public class ActionPipelineFactoryTest {

    private final ActionStorehouse storehouse = mock(ActionStorehouse.class);
    private final ActionAutomationManager automationManager = mock(ActionAutomationManager.class);

    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(123)
                    .setTopologyContextId(1234L))))
        .build();

    /**
     * Ensure that live actions pipeline dependencies are set up properly.
     */
    @Test
    public void testLiveActionsPipeline() {
        final LiveActionPipelineFactory factory = new LiveActionPipelineFactory(storehouse,
            automationManager);

        // If there is an issue with pipeline dependencies, attempting to create a pipeline
        // will result in an exception and fail the test.
        factory.actionPipeline(actionPlan);
    }

    /**
     * Ensure that plan actions pipeline dependencies are set up properly.
     */
    @Test
    public void testPlanActionsPipeline() {
        final PlanActionPipelineFactory factory = new PlanActionPipelineFactory(storehouse);

        // If there is an issue with pipeline dependencies, attempting to create a pipeline
        // will result in an exception and fail the test.
        factory.actionPipeline(actionPlan);
    }
}
