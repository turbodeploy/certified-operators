package com.vmturbo.group.pipeline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.EntitySeverityClientCacheUpdateListener;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Utility class that runs {@link GroupInfoUpdatePipeline}s and group severity updates.
 * Currently pipelines are running every time the repository announces that a new topology has
 * arrived, and severity updates every time the
 * {@link com.vmturbo.action.orchestrator.api.EntitySeverityClientCache} gets fully refreshed (after
 * market announces a new action plan).
 */
public class GroupInfoUpdater implements RepositoryListener,
        EntitySeverityClientCacheUpdateListener {

    private final long realtimeContextId;

    private static final Logger logger = LogManager.getLogger();

    private final GroupInfoUpdatePipelineFactory factory;

    private final GroupSeverityUpdater groupSeverityUpdater;

    private volatile boolean fullPipelineIsQueued = false;

    private volatile boolean severityUpdateIsQueued = false;

    /**
     * {@link GroupInfoUpdatePipeline}s and {@link GroupSeverityUpdater#refreshGroupSeverities()}s
     * are being queued asynchronously. We want to avoid concurrent execution of them, since it
     * might lead to race conditions during data ingestion. To avoid that, we are using a single
     * threaded executor so that if one of them is queued while the other is running, it won't run
     * until the other has finished.
     */
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * Constructor to be used ONLY in unit tests.
     * Allows passing a different executor service to be able to mock easier the thread's behavior.
     * *NOTE*: do NOT use this constructor for non unit test cases (see {@link #executorService}'s
     * comments).
     *
     * @param factory used to create new pipelines to run.
     * @param groupSeverityUpdater used to run severity refreshes.
     * @param realtimeContextId the context id of real time topology.
     * @param executorService used to run pipelines and severity updates.
     */
    GroupInfoUpdater(@Nonnull final GroupInfoUpdatePipelineFactory factory,
            @Nonnull final GroupSeverityUpdater groupSeverityUpdater,
            final long realtimeContextId,
            @Nonnull final ExecutorService executorService) {
        this.factory = factory;
        this.groupSeverityUpdater = groupSeverityUpdater;
        this.realtimeContextId = realtimeContextId;
        this.executorService = executorService;
    }

    /**
     * Constructor.
     *
     * @param factory used to create new pipelines to run.
     * @param groupSeverityUpdater used to run severity refreshes.
     * @param realtimeContextId the context id of real time topology.
     */
    public GroupInfoUpdater(@Nonnull final GroupInfoUpdatePipelineFactory factory,
            @Nonnull final GroupSeverityUpdater groupSeverityUpdater,
            final long realtimeContextId) {
        this.factory = factory;
        this.groupSeverityUpdater = groupSeverityUpdater;
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
        if (fullPipelineIsQueued) {
            logger.info("Skipping group info update pipeline for topology id {}; another "
                    + "pipeline is already running", topologyId);
            return;
        }
        fullPipelineIsQueued = true;
        // Start a new pipeline in a new thread. That thread will set the 'fullPipelineIsQueued'
        // flag to false once it's done.
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
                        pipeline.getContext().getTopologyId(), e);
            } finally {
                fullPipelineIsQueued = false;
            }
        });
    }

    @Override
    public void onEntitySeverityClientCacheRefresh() {
        if (severityUpdateIsQueued) {
            logger.info("Skipping group severity update; another severity update is already queued.");
            return;
        }
        severityUpdateIsQueued = true;
        // Start a new severity update in a new thread. That thread will set the
        // 'severityUpdateIsQueued' flag to false once it's done.
        executorService.execute(() -> {
            try {
                groupSeverityUpdater.refreshGroupSeverities();
            } finally {
                severityUpdateIsQueued = false;
            }
        });
    }
}
