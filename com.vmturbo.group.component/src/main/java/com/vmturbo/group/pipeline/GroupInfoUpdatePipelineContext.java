package com.vmturbo.group.pipeline;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.pipeline.PipelineContext;

/**
 * {@link PipelineContext} implementation for {@link GroupInfoUpdatePipeline}, containing
 * information that's shared by all stages in a pipeline. Currently these include only the id of the
 * topology that this pipeline processes.
 */
public class GroupInfoUpdatePipelineContext extends PipelineContext {

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
}
