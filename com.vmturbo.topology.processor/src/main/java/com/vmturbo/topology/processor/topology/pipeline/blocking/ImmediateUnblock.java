package com.vmturbo.topology.processor.topology.pipeline.blocking;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblockFactory.PipelineUnblock;

/**
 * A {@link PipelineUnblock} operation that unblocks immediately.
 */
public class ImmediateUnblock implements PipelineUnblock {

    private TopologyPipelineExecutorService pipelineExecutorService;

    private ImmediateUnblock(TopologyPipelineExecutorService pipelineExecutorService) {
        this.pipelineExecutorService = pipelineExecutorService;
    }

    @Override
    public void run() {
        pipelineExecutorService.unblockBroadcasts();
    }

    /**
     * Factory class for {@link ImmediateUnblock} operations.
     */
    public static class ImmediateUnblockFactory implements PipelineUnblockFactory {

        @Override
        public PipelineUnblock newUnblockOperation(
                @Nonnull TopologyPipelineExecutorService pipelineExecutorService) {
            return new ImmediateUnblock(pipelineExecutorService);
        }
    }
}
