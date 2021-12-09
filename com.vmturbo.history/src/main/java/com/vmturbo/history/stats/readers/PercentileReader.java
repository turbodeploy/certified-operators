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

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk.Builder;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;

/**
 * {@link PercentileReader} reads percentile information from database. For more information,
 * please, look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 */
public class PercentileReader extends AbstractBlobsReader<GetPercentileCountsRequest, PercentileChunk,
        PercentileBlobsRecord> {
    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;

    /**
     * Creates {@link PercentileReader} instance.
     *
     * @param timeToWaitNetworkReadinessMs time to wait network buffer readiness to
     *                 send accept next chunk for sending over the network.
     * @param grpcTimeoutMs GRPC interaction timeout in milliseconds
     * @param clock provides information about current time.
     * @param dsl provides connection to database.
     */
    public PercentileReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
                    @Nonnull Clock clock, @Nonnull DSLContext dsl) {
        super(timeToWaitNetworkReadinessMs, grpcTimeoutMs, clock, dsl,
                SharedMetrics.PERCENTILE_READING, PERCENTILE_BLOBS_TABLE,
                PercentileBlobsRecord.class.getSimpleName());
    }

    @Override
    protected String getRequestInfo(@Nonnull GetPercentileCountsRequest request) {
        return String.format("timestamp %s", Objects.requireNonNull(request).getStartTimestamp());
    }

    @Override
    protected int getRequestChunkSize(@Nonnull GetPercentileCountsRequest request) {
        return Objects.requireNonNull(request).getChunkSize();
    }

    @Override
    protected long getRecordStartTimestamp(@Nonnull PercentileBlobsRecord record) {
        return Objects.requireNonNull(record).getStartTimestamp();
    }

    @Override
    protected long getRecordAggregationWindowLength(@Nonnull PercentileBlobsRecord record) {
        return Objects.requireNonNull(record).getAggregationWindowLength();
    }

    @Override
    protected int getRecordChunkIndex(@Nonnull PercentileBlobsRecord record) {
        return Objects.requireNonNull(record).getChunkIndex();
    }

    @Override
    protected byte[] getRecordData(@Nonnull PercentileBlobsRecord record) {
        return Objects.requireNonNull(record).getData();
    }

    @Override
    protected PercentileChunk buildChunk(long period, long startTimestamp, ByteString data) {
        final Builder percentileChunkBuilder = PercentileChunk.newBuilder();
        percentileChunkBuilder.setPeriod(period);
        percentileChunkBuilder.setStartTimestamp(startTimestamp);
        percentileChunkBuilder.setContent(data);
        return percentileChunkBuilder.build();
    }

    @Override
    protected Result<PercentileBlobsRecord> queryData(@Nonnull DSLContext context,
            @Nonnull GetPercentileCountsRequest request) {
        Objects.requireNonNull(request);
        return Objects.requireNonNull(context).selectFrom(PERCENTILE_BLOBS_TABLE)
                .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP.eq(request.getStartTimestamp()))
                .orderBy(PERCENTILE_BLOBS_TABLE.CHUNK_INDEX).fetch();
    }
}
