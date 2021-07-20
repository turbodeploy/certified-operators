package com.vmturbo.history.stats.writers;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk.Builder;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.MovingStatisticsBlobs;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link MovingStatisticsWriter} implementation is working as expected.
 */
public class MovingStatisticsWriterTest extends AbstractBlobsWriterTest<SetMovingStatisticsResponse,
        MovingStatisticsChunk> {
    @Override
    public MovingStatisticsChunk newChunk(@Nullable Long period, long startTimestamp,
            @Nonnull ByteString data) {
        Objects.requireNonNull(data);
        final Builder chunkBuilder = MovingStatisticsChunk.newBuilder();
        chunkBuilder.setStartTimestamp(startTimestamp);
        chunkBuilder.setContent(data);
        return chunkBuilder.build();
    }

    @Override
    protected AbstractBlobsWriter newWriter(
            @Nonnull StreamObserver<SetMovingStatisticsResponse> responseObserver,
            @Nonnull HistorydbIO historydbIO) {
        return new MovingStatisticsWriter(responseObserver, historydbIO);
    }

    @Override
    protected String deleteStatement() {
        return String.format("delete from `%s`.`%s`", TEST_DB_SCHEMA_NAME,
                MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS.getName());
    }

    @Override
    protected String addInsertStatement() {
        final String insertStatement = String.format("insert into `%s`.`%s", TEST_DB_SCHEMA_NAME,
                MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS.getName());
        sqlRequestToResponse.add(Pair.create(Pair.create(insertStatement,
                Arrays.asList(START_TIMESTAMP, null, 0)), null));
        return insertStatement;
    }

    @Override
    protected Long getPeriod() {
        return null;
    }
}
