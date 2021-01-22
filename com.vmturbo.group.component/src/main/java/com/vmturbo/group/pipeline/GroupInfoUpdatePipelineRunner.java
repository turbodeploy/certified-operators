package com.vmturbo.group.pipeline;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Utility class that runs {@link GroupInfoUpdatePipeline}s.
 * Currently pipelines are running every time the repository announces that a new topology has
 * arrived.
 */
public class GroupInfoUpdatePipelineRunner implements RepositoryListener {

    private final long realtimeContextId;

    private static final Logger logger = LogManager.getLogger();

    private final GroupInfoUpdatePipelineFactory factory;

    private volatile boolean pipelineIsRunning = false;

    private final ExecutorService executorService;

    /**
     * Constructor.
     *
     * @param factory used to create new pipelines to run.
     * @param executorService used to run pipelines.
     * @param realtimeContextId the context id of real time topology.
     */
    public GroupInfoUpdatePipelineRunner(@Nonnull final GroupInfoUpdatePipelineFactory factory,
            @Nonnull final ExecutorService executorService,
            final long realtimeContextId) {
        this.factory = factory;
        this.executorService = executorService;
        this.realtimeContextId = realtimeContextId;
    }

    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        // If this notification corresponds to a plan topology, skip it
        if (topologyContextId != realtimeContextId) {
            return;
        }
        // If another pipeline is already running, then skip current notification (usually when group
        // component restarts, there are a lot of notifications in the kafka topic. We don't want to
        // run a pipeline for all of them.
        if (pipelineIsRunning) {
            logger.info("Skipping group info update pipeline for topology id {}; another "
                    + "pipeline is already running", topologyId);
            return;
        }
        pipelineIsRunning = true;
        // Start a new pipeline in a new thread. That thread will set the 'pipelineIsRunning flag to
        // false once it's done.
        executorService.execute(() -> {
            // Note: when the component restarts, since there will be a lot of older notifications
            // in the kafka topic, the topology id might not be the one that corresponds to the live
            // topology that exists in repository; The pipeline will always process the (live)
            // topology, even if an older id is provided.
            final GroupInfoUpdatePipeline pipeline = factory.newPipeline(topologyId);
            try {
                pipeline.run(new GroupInfoUpdatePipelineInput());
            } catch (PipelineException | InterruptedException e) {
                logger.error("Failed to complete group information update pipeline (topology id: {})"
                                + " due to error: {}",
                        pipeline.getContext().getTopologyId(), e.getMessage());
            } finally {
                pipelineIsRunning = false;
            }
        });
    }
}
