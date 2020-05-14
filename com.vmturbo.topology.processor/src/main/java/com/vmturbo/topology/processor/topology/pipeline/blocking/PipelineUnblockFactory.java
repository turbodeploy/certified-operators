package com.vmturbo.topology.processor.topology.pipeline.blocking;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Factory class for {@link PipelineUnblock} operations.
 */
public interface PipelineUnblockFactory {

    /**
     * Create a new {@link PipelineUnblock} operation.
     *
     * @param pipelineExecutorService The {@link TopologyPipelineExecutorService} that the
     *                                operation will unblock when all targets are discovered.
     * @return The {@link DiscoveryBasedUnblock}.
     */
    PipelineUnblock newUnblockOperation(@Nonnull TopologyPipelineExecutorService pipelineExecutorService);

    /**
     * Runnable that does some processing/waiting, and unblocks a
     * {@link TopologyPipelineExecutorService} afterwards.
     */
    interface PipelineUnblock extends Runnable {
    }
}
