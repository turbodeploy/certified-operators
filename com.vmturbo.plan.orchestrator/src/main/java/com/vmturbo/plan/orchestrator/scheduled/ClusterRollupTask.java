package com.vmturbo.plan.orchestrator.scheduled;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * The ClusterRollupTask runs once per day to calculate stats values for each cluster.
 **/
public class ClusterRollupTask {

    private final Logger logger = LogManager.getLogger(getClass());

    private final StatsHistoryServiceBlockingStub statsServiceRpc;

    private final CronTrigger cronTrigger;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private final ClusterServiceGrpc.ClusterServiceBlockingStub clusterServiceRpc;

    private AtomicInteger rollupCount = new AtomicInteger(0);


    ClusterRollupTask(@Nonnull StatsHistoryServiceBlockingStub statsServiceRpc,
                      @Nonnull ClusterServiceGrpc.ClusterServiceBlockingStub clusterServiceRpc,
                      @Nonnull ThreadPoolTaskScheduler threadPoolTaskScheduler,
                      @Nonnull CronTrigger cronTrigger) {
        this.clusterServiceRpc = clusterServiceRpc;
        this.statsServiceRpc = statsServiceRpc;
        this.cronTrigger = cronTrigger;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
    }

    /**
     * Start the cluster rollup schedule based on the given {@link CronTrigger}.
     *
     * Also, schedule an initial roll-up. It is up to HistoryComponent to reject unnecessary roll-ups.
     */
    public void initializeSchedule() {
        // kick off an initial roll-up
        requestClusterRollup();

        // establish a schedule for future roll-ups based on the given "cronTrigger"
        logger.info("initializing the schedule:  trigger {}", cronTrigger.getExpression());
        threadPoolTaskScheduler.schedule(this::requestClusterRollup, cronTrigger);
    }

    /**
     * Request that periodic ClusterRollup be calculated.
     *
     * This consists of (A) fetching all known clusters from the Cluster Service, and
     * (B) sending the list of all clusters to the History Component.
     *
     * Each Cluster will include both the ID and the list of all members (PMs).
     *
     * It is up to the HistoryComponent to track the ClusterRollup results.
     */
    @VisibleForTesting
    void requestClusterRollup() {
        // future commit - create the protobuf to send to History and the corresponding gRPC entry
        rollupCount.incrementAndGet();
        logger.info("Request Cluster Roll-Up #{}", getRollupCount());
        try {
            Iterator<GroupDTO.Cluster> allClusters = clusterServiceRpc.getClusters(GroupDTO.GetClustersRequest.newBuilder()
                .build());
            statsServiceRpc.computeClusterRollup(Stats.ClusterRollupRequest.newBuilder()
                .addAllClustersToRollup(() -> allClusters)
                .build());
        } catch (RuntimeException e) {
            logger.error("Exception during cluster rollup request:", e);
        }
    }

    @VisibleForTesting
    int getRollupCount() {
        return rollupCount.get();
    }

}
