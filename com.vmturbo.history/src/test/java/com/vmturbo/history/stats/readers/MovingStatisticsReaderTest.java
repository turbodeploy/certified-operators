package com.vmturbo.history.stats.readers;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.GetMovingStatisticsRequest;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.MovingStatisticsBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.MovingStatisticsBlobsRecord;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link PercentileReader} is working as expected.
 */
public class MovingStatisticsReaderTest extends AbstractBlobsReaderTest<GetMovingStatisticsRequest,
        MovingStatisticsChunk, MovingStatisticsBlobsRecord> {
    private static final String SQL_SELECT_STATEMENT = String.format(
            "select `%s`.`%s`.`start_timestamp`", TEST_DB_SCHEMA_NAME,
            MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS.getName());

    @Override
    protected Table<MovingStatisticsBlobsRecord> getBlobsTable() {
        return MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS;
    }

    @Override
    protected long getPeriod() {
        return 0;
    }

    @Override
    protected String getRecordClassName() {
        return MovingStatisticsBlobsRecord.class.getSimpleName();
    }

    @Override
    protected Class<MovingStatisticsChunk> getChunkClass() {
        return MovingStatisticsChunk.class;
    }

    @Override
    protected ByteString getChunkContent(@Nonnull MovingStatisticsChunk chunk) {
        return Objects.requireNonNull(chunk).getContent();
    }

    @Override
    protected AbstractBlobsReader newReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
            @Nonnull Clock clock, @Nonnull HistorydbIO historydbIO) {
        return new MovingStatisticsReader(timeToWaitNetworkReadinessMs, grpcTimeoutMs, clock, historydbIO);
    }

    @Override
    protected MovingStatisticsBlobsRecord newRecord(@Nonnull DSLContext context,
            @Nonnull String chunk, int chunkIndex, long timestamp) {
        Objects.requireNonNull(chunk);
        return Objects.requireNonNull(context).newRecord(getBlobsTable())
                .values(timestamp, chunk.getBytes(StandardCharsets.UTF_8), chunkIndex);
    }

    @Override
    protected GetMovingStatisticsRequest newRequest(int chunkSize, long timestamp) {
        return GetMovingStatisticsRequest.newBuilder().setChunkSize(chunkSize).build();
    }

    @Override
    protected void addSqlResult(
            @Nonnull LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse,
            long timestamp, @Nullable Result<MovingStatisticsBlobsRecord> result) {
        Objects.requireNonNull(sqlRequestToResponse)
                .add(Pair.create(Pair.create(SQL_SELECT_STATEMENT, Arrays.asList(1L)), result));   // first query for the latest timestamp
        sqlRequestToResponse.add(Pair.create(Pair.create(SQL_SELECT_STATEMENT,
                Collections.singletonList(timestamp)), result));
    }
}
