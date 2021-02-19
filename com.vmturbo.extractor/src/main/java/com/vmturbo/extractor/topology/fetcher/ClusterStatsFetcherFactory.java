package com.vmturbo.extractor.topology.fetcher;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record1;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.tables.Entity;
import com.vmturbo.extractor.schema.tables.Metric;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Factory to generate the {@link ClusterStatsFetcher}.
 */
public class ClusterStatsFetcherFactory {

    private final int headroomMaxBackfillingDays;

    protected final Logger logger = LogManager.getLogger(getClass());

    private final StatsHistoryServiceBlockingStub historyService;
    private Instant latestFetchedPropsTime;
    private final Future initializationTask;
    final int headroomCheckInterval;


    /**
     * Create a new instance.
     * @param historyService history service endpoint
     * @param dbEndpoint db endpoint
     * @param headroomMaxBackfillingDays max amount of days for which the fetcher will backfill data
     * @param headroomCheckInterval interval at which we will check for cluster headroom props
     */
    public ClusterStatsFetcherFactory(@Nonnull StatsHistoryServiceBlockingStub historyService,
                                      @Nonnull final DbEndpoint dbEndpoint,
                                      final int headroomMaxBackfillingDays,
                                      final int headroomCheckInterval) {

        this.historyService = historyService;
        this.headroomCheckInterval = headroomCheckInterval;
        this.headroomMaxBackfillingDays = headroomMaxBackfillingDays;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        initializationTask = executor.submit(new RecentClusterStatsLoader(dbEndpoint,
            this::setLatestFetchedPropsTime, headroomMaxBackfillingDays));
        executor.shutdown();
    }

    /**
     * Get a new {@link ClusterStatsFetcher} instance.
     * @param consumer     fn to handle fetched cluster data
     * @param topologyCreationTime the creation time of the current topology
     * @return an instance of a {@link ClusterStatsFetcher}
     */
    public ClusterStatsFetcher getClusterStatsFetcher(@Nonnull Consumer<List<EntityStats>> consumer,  @Nonnull long topologyCreationTime) {
        try {
            final int clusterStatsInitializationTimeoutSeconds = 300;
            initializationTask.get(clusterStatsInitializationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            latestFetchedPropsTime = Instant.now();
            logger.error("Could not get most recent cluster properties, this might result in some"
                + " missing data", e);
        }
        // We do not want to fetch results that are older than headroomMaxBackfillingDays
        if (Instant.now().minus(Duration.ofDays(headroomMaxBackfillingDays)).isAfter(latestFetchedPropsTime)) {
            latestFetchedPropsTime = Instant.now().minus(Duration.ofDays(headroomMaxBackfillingDays));
        }
        return new ClusterStatsFetcher(historyService, new MultiStageTimer(logger), consumer,
            latestFetchedPropsTime,
            topologyCreationTime, this::setLatestFetchedPropsTime, headroomCheckInterval);
    }

    private void setLatestFetchedPropsTime(Instant time) {
        this.latestFetchedPropsTime = time;
    }

    /**
     * Runnable that will perform the initialization of the cluster fetcher. This is used to get
     * the most recent (if any) cluster properties in the database. Based on that we know how to
     * backfill the data.
     */
    private static class RecentClusterStatsLoader implements Runnable {
        private final DbEndpoint dbEndpoint;
        protected final Logger logger = LogManager.getLogger(getClass());
        private final Consumer<Instant> onComplete;
        private int headroomMaxBackfillingDays;


        /**
         * Return an instance of a {@link RecentClusterStatsLoader}.
         *
         * @param dbEndpoint database endpoint
         * @param onComplete consumer that will be called once the run method is done
         * @param headroomMaxBackfillingDays max amount of days for which the fetcher will backfill data
         */
        RecentClusterStatsLoader(@Nonnull final DbEndpoint dbEndpoint,
                                 @Nonnull final Consumer<Instant> onComplete,
                                 final int headroomMaxBackfillingDays) {
            this.dbEndpoint = dbEndpoint;
            this.onComplete = onComplete;
            this.headroomMaxBackfillingDays = headroomMaxBackfillingDays;
        }

        @Override
        public void run() {
            Instant time = getLatestFetchedPropsTime();
            onComplete.accept(time);
        }

        /**
         * Return the instant  of the latest cluster properties. If there are no cluster props in
         * the db or an error occurs, return a timestamp from yesterday. This is needed because
         * we are guaranteed that we have read all the results until yesterday, but there's still
         * a chance the properties for today haven't been written yet.
         *
         * @return the timestamp for the latest properties in the db. If none are present return
         * the current timestamp
         */
        private Instant getLatestFetchedPropsTime() {
            final OffsetDateTime now = OffsetDateTime.now();
            Instant latestWrittenPropsTime = now.toInstant().minus(Duration.ofDays(1));
            try {
                Record1<OffsetDateTime> latestTime;
                latestTime = dbEndpoint.dslContext().select(Metric.METRIC.TIME)
                    .from(Metric.METRIC)
                    .join(Entity.ENTITY)
                    .on(Metric.METRIC.ENTITY_OID.eq(Entity.ENTITY.OID))
                    .where(Metric.METRIC.TYPE.eq(MetricType.CPU_HEADROOM)
                        .and(Metric.METRIC.TIME.between(OffsetDateTime.now().minus(Duration.ofDays(headroomMaxBackfillingDays)),
                            OffsetDateTime.now()))
                        .and(Entity.ENTITY.TYPE.eq(EntityType.COMPUTE_CLUSTER)))
                    .orderBy(Metric.METRIC.TIME.desc())
                    .limit(1).fetchOne();
                if (latestTime != null) {
                    Instant result =
                        latestTime.getValue(Metric.METRIC.TIME, Timestamp.class).toInstant();
                    logger.info("Fetched most recent cluster properties with timestamp {}", result);
                    return result;
                }
            } catch (UnsupportedDialectException | SQLException | InterruptedException e) {
                logger.error("Error in querying the most recent timestamp for cluster properties");
            }
            return latestWrittenPropsTime;
        }
    }

}
