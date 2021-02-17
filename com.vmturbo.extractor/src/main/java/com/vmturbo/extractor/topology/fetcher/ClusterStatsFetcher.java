package com.vmturbo.extractor.topology.fetcher;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters.Builder;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.utils.MultiStageTimer;

/**
 * Fetch all necessary cluster stats from the history component.
 */
public class ClusterStatsFetcher extends DataFetcher<List<EntityStats>> {

    /**
     * Cluster stats are sent by the history component in a paginated form. So potentially we
     * might have to do multiple calls to get all of them. This limit is there to protect against
     * a potential endless iteration.
     */
    private static final int MAX_N_ITERATIONS = 100;
    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);

    private final Duration headroomCheckInterval;
    private final StatsHistoryServiceBlockingStub historyService;
    private Instant latestFetchedPropsTime;
    private final long topologyCreationTime;
    Consumer<Instant> onComplete;

    /**
     * Create a new instance.
     * @param historyService history service endpoint
     * @param timer a {@link MultiStageTimer} to collect timing information for this fetcher
     * @param consumer     fn to handle fetched cluster data
     * @param latestFetchedPropsTime the last time cluster props have been fetched
     * @param topologyCreationTime the creation time of the current topology
     * @param onComplete consumer that will be called once the run method is done
     * @param headroomCheckInterval interval at which we will check for cluster headroom props
     */
    public ClusterStatsFetcher(@Nonnull StatsHistoryServiceBlockingStub historyService,
                               @Nonnull MultiStageTimer timer,
                               @Nonnull Consumer<List<EntityStats>> consumer,
                               @Nonnull final Instant latestFetchedPropsTime,
                               @Nonnull long topologyCreationTime,
                               @Nonnull Consumer<Instant> onComplete,
                               final int headroomCheckInterval) {
        super(timer, consumer);
        this.historyService = historyService;
        this.latestFetchedPropsTime = latestFetchedPropsTime;
        this.topologyCreationTime = topologyCreationTime;
        this.onComplete = onComplete;
        this.headroomCheckInterval = Duration.ofHours(headroomCheckInterval);
    }


    @Override
    protected String getName() {
        return "Load cluster stats";
    }

    /**
     * Fetch cluster stats from the history component.
     *
     * @return list of cluster {@link EntityStats}
     */
    @Override
    protected List<EntityStats> fetch() {
        Instant topologyCreationInstant = Instant.ofEpochMilli(topologyCreationTime);
        if (topologyCreationInstant.isAfter(latestFetchedPropsTime.plus(headroomCheckInterval))) {
            // We want to look for props starting the day after the last results we had
            final long startingDay =
                truncateToDay(latestFetchedPropsTime.plus(Duration.ofDays(1)).toEpochMilli());
            if (startingDay <= truncateToDay(topologyCreationInstant.toEpochMilli())) {
                List<EntityStats> result =  getResults(startingDay,
                    topologyCreationInstant.toEpochMilli());
                getMostRecentTimeStamp(result).ifPresent(time -> onComplete.accept(Instant.ofEpochMilli(time)));
                return result;
            }
        }
        return Collections.emptyList();
    }

    private List<EntityStats> getResults(final long startingDay, final long endTime) {
        final List<EntityStats> entityStatsList = new ArrayList<>();
        Optional<String> cursor = Optional.empty();
        logger.info("Fetching cluster stats. Starting time: {}. End time {}. Next fetch will "
                + "happen at {}", startingDay, endTime,
            endTime);
        long totalIterations = 0;

        // Keep iterating until we consume all the paginated data or we reach the max number of
        // allowed iterations
        do {
            final ClusterStatsRequest clusterStatsRequest =
                createClusterRequest(cursor, startingDay, endTime);
            Iterator<ClusterStatsResponse> response =
                historyService.getClusterStats(clusterStatsRequest);
            while (response.hasNext()) {
                totalIterations += 1;
                ClusterStatsResponse chunk = response.next();
                if (chunk.hasSnapshotsChunk()) {
                    entityStatsList.addAll(chunk.getSnapshotsChunk().getSnapshotsList());
                } else {
                    cursor =
                        chunk.getPaginationResponse().hasNextCursor()
                            ? Optional.of(chunk.getPaginationResponse().getNextCursor()) : Optional.empty();
                }
            }
        } while (cursor.isPresent() && totalIterations < MAX_N_ITERATIONS);
        if (!entityStatsList.isEmpty()) {
            logger.info("Fetched {} cluster stats", entityStatsList.size());
        }
        return entityStatsList;
    }

    private static long truncateToDay(long time) {
        return (time / ONE_DAY_IN_MILLIS) * ONE_DAY_IN_MILLIS;
    }

    private static Optional<Long> getMostRecentTimeStamp(List<EntityStats> stats) {
        return stats.stream().map(EntityStats::getStatSnapshotsList).flatMap(Collection::stream).map(StatSnapshot::getSnapshotDate).max(Long::compare);
    }

    private ClusterStatsRequest createClusterRequest(Optional<String> cursor, long latestFetchedPropsTime, long currentTopoTime) {
        Builder params = PaginationParameters.newBuilder();
        cursor.ifPresent(params::setCursor);
        return ClusterStatsRequest.newBuilder().setPaginationParams(params.build()).setStats(StatsFilter.newBuilder()
            .setStartDate(latestFetchedPropsTime)
            .setEndDate(currentTopoTime)
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.CPU_HEADROOM))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.MEM_HEADROOM))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.STORAGE_HEADROOM))
            .addCommodityRequests(CommodityRequest.newBuilder()
                .setCommodityName(StringConstants.TOTAL_HEADROOM))
        ).build();
    }

}
