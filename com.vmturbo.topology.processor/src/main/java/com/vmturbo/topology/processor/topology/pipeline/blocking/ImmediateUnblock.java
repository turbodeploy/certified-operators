package com.vmturbo.topology.processor.topology.pipeline.blocking;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblockFactory.PipelineUnblock;

/**
 * A {@link PipelineUnblock} operation that unblocks immediately.
 */
public class ImmediateUnblock implements PipelineUnblock {

    private static final Logger logger = LogManager.getLogger();

    private TopologyPipelineExecutorService pipelineExecutorService;

    private ImmediateUnblock(TopologyPipelineExecutorService pipelineExecutorService) {
        this.pipelineExecutorService = pipelineExecutorService;
    }

    @Override
    public void run() {
        logger.info("Immediately unblocking broadcasts.");
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
