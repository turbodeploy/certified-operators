/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmturbo.common.protobuf.stats.Stats.GetMovingStatisticsRequest;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk.Builder;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.MovingStatisticsBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.MovingStatisticsBlobsRecord;

/**
 * {@link MovingStatisticsReader} reads moving stats information from database.
 */
public class MovingStatisticsReader extends AbstractBlobsReader<GetMovingStatisticsRequest,
        MovingStatisticsChunk, MovingStatisticsBlobsRecord> {
    private static final MovingStatisticsBlobs MOVING_STATISTICS_BLOBS_TABLE
            = MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS;

    /**
     * Creates {@link MovingStatisticsReader} instance.
     *
     * @param timeToWaitNetworkReadinessMs time to wait network buffer readiness to
     *                 send accept next chunk for sending over the network.
     * @param grpcTimeoutMs GRPC interaction timeout in milliseconds
     * @param clock provides information about current time.
     * @param historydbIO provides connection to database.
     */
    public MovingStatisticsReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
                    @Nonnull Clock clock, @Nonnull HistorydbIO historydbIO) {
        super(timeToWaitNetworkReadinessMs, grpcTimeoutMs, clock, historydbIO,
                SharedMetrics.MOVING_STATISTICS_READING, MOVING_STATISTICS_BLOBS_TABLE,
                MovingStatisticsBlobsRecord.class.getSimpleName());
    }

    @Override
    protected String getRequestInfo(@Nonnull GetMovingStatisticsRequest request) {
        return request.getClass().getSimpleName();
    }

    @Override
    protected int getRequestChunkSize(@Nonnull GetMovingStatisticsRequest request) {
        return request.getChunkSize();
    }

    @Override
    protected long getRecordStartTimestamp(@Nonnull MovingStatisticsBlobsRecord record) {
        return record.getStartTimestamp();
    }

    @Override
    protected long getRecordAggregationWindowLength(@Nonnull MovingStatisticsBlobsRecord record) {
        return 0;
    }

    @Override
    protected int getRecordChunkIndex(@Nonnull MovingStatisticsBlobsRecord record) {
        return record.getChunkIndex();
    }

    @Override
    protected byte[] getRecordData(@Nonnull MovingStatisticsBlobsRecord record) {
        return record.getData();
    }

    @Override
    protected MovingStatisticsChunk buildChunk(long period, long startTimestamp, ByteString data) {
        final Builder chunkBuilder = MovingStatisticsChunk.newBuilder();
        chunkBuilder.setStartTimestamp(startTimestamp);
        chunkBuilder.setContent(data);
        return chunkBuilder.build();
    }

    @Override
    protected Result<MovingStatisticsBlobsRecord> queryData(@Nonnull DSLContext context,
            @Nonnull GetMovingStatisticsRequest request) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(request);
        final Result<MovingStatisticsBlobsRecord> lastRecord =
                context.selectFrom(MOVING_STATISTICS_BLOBS_TABLE)
                        .orderBy(MOVING_STATISTICS_BLOBS_TABLE.START_TIMESTAMP.desc())
                        .limit(1).fetch();
        if (lastRecord.isEmpty()) {
            return lastRecord;
        }
        final Long latestTimestamp = lastRecord.get(0).getValue(MOVING_STATISTICS_BLOBS_TABLE.START_TIMESTAMP);
        return context.selectFrom(MOVING_STATISTICS_BLOBS_TABLE)
                .where(MOVING_STATISTICS_BLOBS_TABLE.START_TIMESTAMP.eq(latestTimestamp))
                .orderBy(MOVING_STATISTICS_BLOBS_TABLE.CHUNK_INDEX).fetch();
    }
}
