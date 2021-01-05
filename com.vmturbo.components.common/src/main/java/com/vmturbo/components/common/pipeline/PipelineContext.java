package com.vmturbo.components.common.pipeline;

import javax.annotation.Nonnull;

/**
 * The {@link PipelineContext} is information that's shared by all stages
 * in a pipeline.
 *
 * <p/>This is the place to put generic info that applies to many stages, as well as utility objects
 * that have some state which are shared across the stages. The context shouldn't have too much
 * stuff, because those kinds of dependencies defeat the point of having a pipeline with
 * individual "independent" stages.
 */
public interface PipelineContext {

    /**
     * Get the name of the pipeline instance. Used mainly for logging/debugging.
     *
     * @return The pipeline name.
     */
    @Nonnull
    String getPipelineName();
}
