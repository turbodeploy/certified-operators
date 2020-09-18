package com.vmturbo.action.orchestrator.store.pipeline;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStoreFactory;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PopulateActionStoreStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateAutomationStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateSeverityCacheStage;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;

/**
 * Tests for various {@link ActionPipelineStages} stages.
 */
public class ActionPipelineStagesTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 1234;

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStoreFactory actionStoreFactory = mock(ActionStoreFactory.class);
    private final ActionStore actionStore = mock(ActionStore.class);

    private final Action moveAction = Action.newBuilder()
        .setId(9999L)
        .setDeprecatedImportance(0)
        .setExplanation(Explanation.getDefaultInstance())
        .setInfo(ActionInfo.getDefaultInstance())
        .build();
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1234L)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(5678L)
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID))))
        .addAction(moveAction)
        .build();

    /**
     * setup.
     */
    @Before
    public void setup() {
        when(actionStorehouse.getActionStoreFactory()).thenReturn(actionStoreFactory);
        when(actionStoreFactory.newStore(eq(REALTIME_TOPOLOGY_CONTEXT_ID))).thenReturn(actionStore);
    }

    /**
     * testPopulateActionStoreStage.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPopulateActionStoreStage() throws Exception {
        final PopulateActionStoreStage stage = new PopulateActionStoreStage(actionStorehouse);
        when(actionStorehouse.storeActions(actionPlan)).thenReturn(actionStore);
        final StageResult<ActionStore> result = stage.execute(actionPlan);
        assertEquals(actionStore, result.getResult());
        verify(actionStorehouse).storeActions(eq(actionPlan));
    }

    /**
     * testUpdateAutomationStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testUpdateAutomationStage() throws PipelineStageException, InterruptedException {
        final ActionAutomationManager automationManager = mock(ActionAutomationManager.class);
        final UpdateAutomationStage stage = new UpdateAutomationStage(automationManager);
        stage.execute(actionStore);

        verify(automationManager).updateAutomation(actionStore);
    }

    /**
     * testUpdateSeverityCacheStage.
     *
     * @throws PipelineStageException on exception.
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testUpdateSeverityCacheStage() throws PipelineStageException, InterruptedException {
        final EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
        when(actionStore.getEntitySeverityCache()).thenReturn(Optional.of(severityCache));

        final UpdateSeverityCacheStage stage = new UpdateSeverityCacheStage();
        stage.execute(actionStore);

        verify(severityCache).refresh(eq(actionStore));
    }
}