package com.vmturbo.group.pipeline;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link PipelineContext} implementation for {@link GroupInfoUpdatePipeline}, containing
 * information that's shared by all stages in a pipeline. Currently these include only the id of the
 * topology that this pipeline processes.
 */
public class GroupInfoUpdatePipelineContext extends PipelineContext {

    private static final String PIPELINE_STAGE_LABEL = "stage";

    private static final DataMetricSummary GROUP_INFO_UPDATE_STAGE_SUMMARY =
        DataMetricSummary.builder()
            .withName("group_info_update_pipeline_duration_seconds")
            .withHelp("Duration of the individual stages that update information for"
                + " groups.")
            .withLabelNames(PIPELINE_STAGE_LABEL)
            .build()
            .register();

    private final long topologyId;

    /**
     * Constructor for pipeline context.
     *
     * @param topologyId the id of the topology that the pipeline is handling.
     */
    public GroupInfoUpdatePipelineContext(long topologyId) {
        this.topologyId = topologyId;
    }

    /**
     * Accessor for the id of the topology that corresponds to this pipeline.
     *
     * @return the id of the topology.
     */
    public long getTopologyId() {
        return topologyId;
    }

    /**
     * Returns a string that describes the pipeline.
     *
     * @return the pipeline's name.
     */
    @Nonnull
    @Override
    public String getPipelineName() {
        return "Group Information Update Pipeline";
    }

    @Override
    public DataMetricTimer startStageTimer(String stageName) {
        return GROUP_INFO_UPDATE_STAGE_SUMMARY.labels(PIPELINE_STAGE_LABEL).startTimer();
    }

    @Override
    public TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }
}
