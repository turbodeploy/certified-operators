package com.vmturbo.plan.orchestrator.scheduled;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.SimpleTriggerContext;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * The ClusterRollupTask runs once per day to calculate stats values for each cluster.
 **/
public class ClusterRollupTask {

    /**
     * This is the key we will use to store the last cluster rollup time in the KV store.
     * We need it so that we know whether or not we need to kick a cluster rollup off at
     * plan orchestrator restart.
     */
    @VisibleForTesting
    static final String LAST_ROLLUP_TIME_KEY = "lastClusterRollupTime";

    private static final Logger logger = LogManager.getLogger();

    private final StatsHistoryServiceBlockingStub statsServiceRpc;

    private final CronTrigger cronTrigger;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private final GroupServiceBlockingStub groupRpcService;

    private AtomicInteger rollupCount = new AtomicInteger(0);

    private final Clock clock;

    private final KeyValueStore keyValueStore;

    ClusterRollupTask(@Nonnull StatsHistoryServiceBlockingStub statsServiceRpc,
                      @Nonnull GroupServiceBlockingStub groupRpcService,
                      @Nonnull ThreadPoolTaskScheduler threadPoolTaskScheduler,
                      @Nonnull CronTrigger cronTrigger,
                      @Nonnull KeyValueStore keyValueStore,
                      @Nonnull Clock clock) {
        this.groupRpcService = groupRpcService;
        this.statsServiceRpc = statsServiceRpc;
        this.cronTrigger = cronTrigger;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.keyValueStore = keyValueStore;
        this.clock = clock;
    }

    /**
     * Start the cluster rollup schedule based on the given {@link CronTrigger}.
     *
     * Also, schedule an initial roll-up if necessary.
     */
    public void initializeSchedule() {
        final Date nextExecutionTime = keyValueStore.get(LAST_ROLLUP_TIME_KEY)
            .map(Instant::parse)
            .map(Date::from)
            .map(lastRollup -> new SimpleTriggerContext(lastRollup, lastRollup, lastRollup))
            .map(cronTrigger::nextExecutionTime)
            // A default that will guarantee we kick off an initial rollup.
            .orElse(Date.from(clock.instant().minus(1, ChronoUnit.DAYS)));

        if (nextExecutionTime.before(Date.from(clock.instant()))) {
            // Kick off an initial roll-up. We will do it asynchronously, so the plan
            // orchestrator startup is not blocked waiting for the rollup to complete.
            logger.info("Scheduling an initial rollup, since the last execution time was long ago.");
            threadPoolTaskScheduler.execute(this::requestClusterRollup);
        } else {
            logger.info("Not scheduling an initial rollup, because the next execution " +
                "time is {}, which is in the future.", nextExecutionTime);
        }

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
            final List<Grouping> allClusters = new ArrayList<>();

            GroupProtoUtil.CLUSTER_GROUP_TYPES
                .stream()
                .map(type ->
                    groupRpcService.getGroups(
                                    GetGroupsRequest.newBuilder()
                                    .setGroupFilter(GroupFilter.newBuilder()
                                                    .setGroupType(type))
                                    .build())
                ).forEach(it -> {
                    it.forEachRemaining(allClusters::add);
                });

            statsServiceRpc.computeClusterRollup(Stats.ClusterRollupRequest.newBuilder()
                .addAllClustersToRollup(allClusters)
                .build());
            keyValueStore.put(LAST_ROLLUP_TIME_KEY, clock.instant().toString());
        } catch (RuntimeException e) {
            logger.error("Exception during cluster rollup request:", e);
        }
    }

    @VisibleForTesting
    int getRollupCount() {
        return rollupCount.get();
    }

}
