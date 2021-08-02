package com.vmturbo.topology.processor.history.movingstats;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.GetMovingStatisticsRequest;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.AbstractBlobsPersistenceTask;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;

/**
 * Persist the moving statistics data to/from historydb.
 */
public class MovingStatisticsPersistenceTask extends AbstractBlobsPersistenceTask<MovingStatistics,
        MovingStatisticsRecord, MovingStatisticsChunk, SetMovingStatisticsResponse,
        MovingStatisticsHistoricalEditorConfig> {

    private static final Logger logger = LogManager.getLogger();

    private final MovingStatisticsHistoricalEditorConfig config;

    /**
     * Construct the task to load the latest moving statistics data from the persistent store.
     *
     * @param statsHistoryClient persistent store grpc interface
     * @param clock The clock used for timing.
     * @param range range from start timestamp till end timestamp for which we need
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     * @param config Configuration data for the moving statistics editor
     */
    public MovingStatisticsPersistenceTask(@Nonnull StatsHistoryServiceStub statsHistoryClient,
                                           @Nonnull Clock clock,
                                           @Nonnull Pair<Long, Long> range, final boolean enableExpiredOidFiltering,
                                           @Nonnull MovingStatisticsHistoricalEditorConfig config) {
        super(statsHistoryClient, clock, range, enableExpiredOidFiltering);
        this.config = config;
    }

    @Override
    protected void makeGetRequest(@Nonnull StatsHistoryServiceStub statsHistoryClient,
            @Nonnull MovingStatisticsHistoricalEditorConfig config, long startTimestamp,
            @Nonnull ReaderObserver observer) {
        Objects.requireNonNull(statsHistoryClient);
        Objects.requireNonNull(config);
        Objects.requireNonNull(observer);
        statsHistoryClient.getMovingStatistics(GetMovingStatisticsRequest.newBuilder()
                .setChunkSize((int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE))
                .build(), observer);
    }

    @Override
    protected StreamObserver<MovingStatisticsChunk> makeSetRequest(
            @Nonnull StatsHistoryServiceStub statsHistoryClient, @Nonnull WriterObserver observer) {
        Objects.requireNonNull(observer);
        return Objects.requireNonNull(statsHistoryClient).setMovingStatistics(observer);
    }

    @Override
    protected byte[] toByteArray(@Nonnull MovingStatistics stats) {
        return Objects.requireNonNull(stats).toByteArray();
    }

    @Override
    protected int getCount(@Nonnull MovingStatistics stats) {
        return Objects.requireNonNull(stats).getStatisticRecordsCount();
    }

    @Override
    protected MovingStatisticsChunk newChunk(long startTimestamp, long periodMs,
            @Nonnull ByteString payload) {
        Objects.requireNonNull(payload);
        return MovingStatisticsChunk.newBuilder().setStartTimestamp(startTimestamp)
                .setContent(payload).build();
    }

    @Override
    protected ByteString getChunkContent(@Nonnull MovingStatisticsChunk chunk) {
        return Objects.requireNonNull(chunk).getContent();
    }

    @Override
    protected long getChunkPeriod(@Nonnull MovingStatisticsChunk chunk) {
        // moving statistics has no period
        return 0L;
    }

    @Nonnull
    @Override
    protected Map<EntityCommodityFieldReference, MovingStatisticsRecord> parse(long startTimestamp,
                                                                               @Nonnull InputStream source,
                                                                               @Nonnull LongSet oidsToUse,
                                                                               boolean enableExpiredOidFiltering)
            throws IOException {
        return parse(source, MovingStatistics::parseFrom, config, oidsToUse, enableExpiredOidFiltering);
    }

    /**
     * Parses raw bytes to create mapping from field reference to {@link MovingStatisticsRecord}
     * instance.
     *
     * @param source Loaded data that should be parsed to create the MovingStatistics data.
     * @param parser method that need to be used to create {@link MovingStatistics}
     *               instance from {@link InputStream}.
     * @param config The configuration containing the mapping for various moving stats record types.
     * @param oidsToUse non expired oids that should be used
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     * @return mapping from field reference to appropriate percentile record instance.
     * @throws IOException in case of error while parsing percentile records.
     */
    @Nonnull
    protected static Map<EntityCommodityFieldReference, MovingStatisticsRecord>
    parse(@Nonnull InputStream source, @Nonnull ThrowingFunction<InputStream, MovingStatistics, IOException> parser,
          @Nonnull MovingStatisticsHistoricalEditorConfig config,
          @Nullable LongSet oidsToUse, boolean enableExpiredOidFiltering)
        throws IOException {
        final ThrowingFunction<InputStream, Iterable<MovingStatisticsRecord>, IOException> statsToRecords = inputStream -> {
            final MovingStatistics stats = parser.apply(source);
            if (stats == null) {
                throw new InvalidProtocolBufferException(String.format("Cannot parse '%s'",
                    MovingStatistics.class.getSimpleName()));
            }
            return stats.getStatisticRecordsList();
        };
        final Function<MovingStatisticsRecord, EntityCommodityFieldReference> recordToRef = record -> {
            final Integer commType = config.getCommodityTypeForSamplerCase(record.getMovingStatisticsSamplerDataCase());
            if (commType == null) {
                logger.error("Unknown statistics sampler case for {}", record);
                return null;
            }

            return new EntityCommodityFieldReference(record.getEntityOid(),
                CommodityType.newBuilder().setType(commType).build(), null, CommodityField.USED);
        };

        return parseDbRecords(0, source, statsToRecords, oidsToUse,
            MovingStatisticsRecord::getEntityOid, recordToRef, enableExpiredOidFiltering);
    }
}
