package com.vmturbo.topology.processor.topology.pipeline.blocking;

import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Runnable that does some processing/waiting, and unblocks a
 * {@link TopologyPipelineExecutorService} afterwards.
 */
public interface PipelineUnblock extends Runnable {
}
