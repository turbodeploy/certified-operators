package com.vmturbo.action.orchestrator.store.pipeline;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionProcessingInfoStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PopulateActionStoreStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateAutomationStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateSeverityCacheStage;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;

/**
 * A factory class for properly configured {@link ActionPipeline} objects for live topologies.
 *
 * <p>Users should not instantiate live {@link ActionPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link LiveActionPipelineFactory#actionPipeline(ActionPlan)}.
 */
public class LiveActionPipelineFactory {

    private static final Logger logger = LogManager.getLogger();

    private final ActionStorehouse actionStorehouse;
    private final ActionAutomationManager automationManager;

    private long actionPlanCount = 0;

    /**
     * Create a new {@link LiveActionPipelineFactory}.
     *
     * @param actionStorehouse The {@link ActionStorehouse}.
     * @param automationManager The {@link ActionAutomationManager}.
     */
    public LiveActionPipelineFactory(@Nonnull final ActionStorehouse actionStorehouse,
                                     @Nonnull final ActionAutomationManager automationManager) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.automationManager = Objects.requireNonNull(automationManager);
    }

    /**
     * Create a pipeline that capable of processing a live market {@link ActionPlan}.
     *
     * @param actionPlan The action plan to process.
     * @return The {@link ActionPipeline}. This pipeline will accept an {@link ActionPlan}
     *         and return the {@link ActionProcessingInfo} for the processing done by the pipeline.
     */
    public ActionPipeline<ActionPlan, ActionProcessingInfo> actionPipeline(@Nonnull final ActionPlan actionPlan) {
        final ActionPipeline<ActionPlan, ActionProcessingInfo> processingPipeline = buildLivePipeline(actionPlan);
        if (actionPlanCount == 1) {
            logger.info("\n" + processingPipeline.tabularDescription("Live Action Pipeline"));
        }
        return processingPipeline;
    }

    private ActionPipeline<ActionPlan, ActionProcessingInfo> buildLivePipeline(@Nonnull final ActionPlan actionPlan) {
        // Increment the number of action plans observed.
        actionPlanCount++;

        final ActionPipelineContext pipelineContext = new ActionPipelineContext(
            actionPlan.getId(),
            TopologyType.REALTIME,
            actionPlan.getInfo());

        return new ActionPipeline<>(PipelineDefinition.<ActionPlan, ActionProcessingInfo, ActionPipelineContext>newBuilder(pipelineContext)
            .addStage(new PopulateActionStoreStage(actionStorehouse))
            .addStage(new UpdateAutomationStage(automationManager))
            .addStage(new UpdateSeverityCacheStage())
            .finalStage(new ActionProcessingInfoStage())
        );
    }
}
