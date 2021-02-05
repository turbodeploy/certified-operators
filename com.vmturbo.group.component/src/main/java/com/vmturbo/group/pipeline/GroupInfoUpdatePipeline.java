package com.vmturbo.group.pipeline;

import java.time.Clock;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Pipeline that captures the various stages of processing for groups that need to run after a new
 * source topology is available in the repository.
 */
public class GroupInfoUpdatePipeline extends
        Pipeline<GroupInfoUpdatePipelineInput, LongOpenHashSet, GroupInfoUpdatePipelineContext, GroupInfoUpdatePipelineSummary> {

    private static final String PIPELINE_STAGE_LABEL = "stage";

    private static final DataMetricSummary GROUP_INFO_UPDATE_SUMMARY = DataMetricSummary.builder()
            .withName("group_info_update_duration_seconds")
            .withHelp("Duration of group information update (membership cache + database storage.")
            .build()
            .register();

    private static final DataMetricSummary GROUP_INFO_UPDATE_STAGE_SUMMARY =
            DataMetricSummary.builder()
                    .withName("group_info_update_pipeline_duration_seconds")
                    .withHelp("Duration of the individual stages that update information for"
                            + " groups.")
                    .withLabelNames(PIPELINE_STAGE_LABEL)
                    .build()
                    .register();

    protected GroupInfoUpdatePipeline(
            @Nonnull PipelineDefinition<GroupInfoUpdatePipelineInput, LongOpenHashSet, GroupInfoUpdatePipelineContext> stages) {
        super(stages, new GroupInfoUpdatePipelineSummary(Clock.systemUTC(), stages.getContext(),
                stages.getStages()));
    }

    @Override
    protected DataMetricTimer startPipelineTimer() {
        return GROUP_INFO_UPDATE_SUMMARY.startTimer();
    }

    @Override
    protected DataMetricTimer startStageTimer(String stageName) {
        return GROUP_INFO_UPDATE_STAGE_SUMMARY.labels(PIPELINE_STAGE_LABEL).startTimer();
    }

    @Override
    protected TracingScope startPipelineTrace() {
        return Tracing.trace("Group info update")
                .tag("topology_id", getContext().getTopologyId())
                .baggageItem(Tracing.DISABLE_DB_TRACES_BAGGAGE_KEY, "");
    }

    @Override
    protected TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }
}
