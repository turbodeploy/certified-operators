package com.vmturbo.history.stats.writers;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk.Builder;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link PercentileWriter} implementation is working as expected.
 */
public class PercentileWriterTest extends AbstractBlobsWriterTest<SetPercentileCountsResponse,
        PercentileChunk> {
    private static final long PERIOD = 4L;

    @Override
    public PercentileChunk newChunk(@Nullable Long period, long startTimestamp,
            @Nonnull ByteString data) {
        Objects.requireNonNull(data);
        final Builder percentileChunkBuilder = PercentileChunk.newBuilder();
        percentileChunkBuilder.setPeriod(period == null ? 0 : period);
        percentileChunkBuilder.setStartTimestamp(startTimestamp);
        percentileChunkBuilder.setContent(data);
        return percentileChunkBuilder.build();
    }

    @Override
    protected AbstractBlobsWriter newWriter(
            @Nonnull StreamObserver<SetPercentileCountsResponse> responseObserver,
            @Nonnull HistorydbIO historydbIO) {
        return new PercentileWriter(responseObserver, historydbIO);
    }

    @Override
    protected String deleteStatement() {
        return String.format("delete from `%s`.`%s`", TEST_DB_SCHEMA_NAME,
                PercentileBlobs.PERCENTILE_BLOBS.getName());
    }

    @Override
    protected String addInsertStatement() {
        final String insertStatement = String.format("insert into `%s`.`%s", TEST_DB_SCHEMA_NAME,
                PercentileBlobs.PERCENTILE_BLOBS.getName());
        sqlRequestToResponse.add(Pair.create(Pair.create(insertStatement,
                Arrays.asList(START_TIMESTAMP, getPeriod(), null, 0)), null));
        return insertStatement;
    }

    @Override
    protected Long getPeriod() {
        return PERIOD;
    }
}
