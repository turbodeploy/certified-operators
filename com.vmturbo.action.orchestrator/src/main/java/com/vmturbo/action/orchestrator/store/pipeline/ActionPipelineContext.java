package com.vmturbo.action.orchestrator.store.pipeline;

import static com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline.ACTION_PLAN_TYPE_LABEL;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The {@link ActionPipelineContext} is information that's shared by all stages
 * in an action pipeline.
 * <p/>
 * Data are attached to the context by stages based on their requirements (see
 * {@link PipelineContext#addMember(PipelineContextMemberDefinition, Object)} and then
 * automatically dropped when no downstream stages need (see
 * {@code PipelineContext#dropMember(PipelineContextMemberDefinition)}) that data so
 * that data structures are retained in memory only as long as they are needed.
 */
public class ActionPipelineContext extends PipelineContext {

    private static final String PIPELINE_STAGE_LABEL = "stage";

    private static final DataMetricSummary PIPELINE_STAGE_SUMMARY = DataMetricSummary.builder()
        .withName("ao_pipeline_duration_seconds")
        .withHelp("Duration of the individual stages in action plan processing.")
        .withLabelNames(ACTION_PLAN_TYPE_LABEL, PIPELINE_STAGE_LABEL)
        .build()
        .register();

    private final long actionPlanId;
    private final TopologyType topologyType; // ie "REALTIME" or "PLAN"
    private final ActionPlanInfo actionPlanInfo;

    /**
     * Create a new {@link ActionPipelineContext}.
     *
     * @param actionPlanId The ID of the action plan.
     * @param topologyType The topology type (ie REALTIME/PLAN) of the topology associated with the action plan.
     * @param actionPlanInfo Information identifying action plan details.
     */
    public ActionPipelineContext(final long actionPlanId,
                                 final TopologyType topologyType,
                                 @Nonnull final ActionPlanInfo actionPlanInfo) {
        this.actionPlanId = actionPlanId;
        this.topologyType = topologyType;
        this.actionPlanInfo = Objects.requireNonNull(actionPlanInfo);
    }

    /**
     * Get the action plan topology type (ie "REALTIME" or "PLAN").
     *
     * @return the action plan topology type (ie "REALTIME" or "PLAN").
     */
    public TopologyType getActionPlanTopologyType() {
        return topologyType;
    }

    /**
     * Get the topology context ID associated with the action plan.
     *
     * @return the topology context ID associated with the action plan.
     */
    public long getTopologyContextId() {
        return ActionDTOUtil.getActionPlanContextId(actionPlanInfo);
    }

    /**
     * Get the topology ID associated with the action plan. Only available on market
     * action plans.
     *
     * @return The topology ID associated with the action plan, or {@link Optional#empty()}
     *         if none is available.
     */
    public Optional<Long> getTopologyId() {
        return actionPlanInfo.hasMarket()
            ? Optional.of(actionPlanInfo.getMarket().getSourceTopologyInfo().getTopologyId())
            : Optional.empty();
    }

    /**
     * Get the {@link ActionPlanInfo} associated with the action plan processing pipeline.
     *
     * @return the {@link ActionPlanInfo} associated with the action plan processing pipeline.
     */
    public ActionPlanInfo getActionPlanInfo() {
        return actionPlanInfo;
    }

    /**
     * Get the ID of the action plan for this pipeline context.
     *
     * @return the ID of the action plan for this pipeline context.
     */
    public long getActionPlanId() {
        return actionPlanId;
    }

    @Nonnull
    @Override
    public String getPipelineName() {
        return FormattedString.format("{} Action Pipeline (actionPlan: {}, {})",
            topologyType.name(), actionPlanId, getPlanInfoDescription());
    }

    @Override
    public DataMetricTimer startStageTimer(String stageName) {
        return PIPELINE_STAGE_SUMMARY.labels(getActionPlanTopologyType().name(),
            stageName).startTimer();
    }

    @Override
    public TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }

    @Override
    public boolean includeContextMemberSummary() {
        // Action pipeline has a lot of context members. Including them in summary
        // by default is too noisy.
        return false;
    }

    private String getPlanInfoDescription() {
        switch (actionPlanInfo.getTypeInfoCase()) {
            case MARKET:
                final MarketActionPlanInfo marketPlanInfo = actionPlanInfo.getMarket();
                return FormattedString.format("context: {}, topology: {}, {}",
                    marketPlanInfo.getSourceTopologyInfo().getTopologyContextId(),
                    marketPlanInfo.getSourceTopologyInfo().getTopologyId(),
                    marketPlanInfo.getSourceTopologyInfo().getAnalysisTypeList());
            case BUY_RI:
                final BuyRIActionPlanInfo buyRIActionPlanInfo = actionPlanInfo.getBuyRi();
                return FormattedString.format("context: {} [BUY_RI]",
                    buyRIActionPlanInfo.getTopologyContextId());
            default:
                return "";
        }
    }
}
