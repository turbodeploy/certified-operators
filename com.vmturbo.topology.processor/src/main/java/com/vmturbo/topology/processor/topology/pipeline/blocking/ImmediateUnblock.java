package com.vmturbo.topology.processor.topology.pipeline.blocking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * A {@link PipelineUnblock} operation that unblocks immediately.
 */
public class ImmediateUnblock implements PipelineUnblock {

    private static final Logger logger = LogManager.getLogger();

    private TopologyPipelineExecutorService pipelineExecutorService;

    ImmediateUnblock(TopologyPipelineExecutorService pipelineExecutorService) {
        this.pipelineExecutorService = pipelineExecutorService;
    }

    @Override
    public void run() {
        logger.info("Immediately unblocking broadcasts.");
        pipelineExecutorService.unblockBroadcasts();
    }
}
