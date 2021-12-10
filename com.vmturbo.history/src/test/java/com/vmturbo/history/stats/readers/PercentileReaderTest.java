package com.vmturbo.history.stats.readers;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Queue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;
import com.vmturbo.history.stats.TestDataProvider.SqlWithResponse;

/**
 * Checks that {@link PercentileReader} is working as expected.
 */
public class PercentileReaderTest extends AbstractBlobsReaderTest<GetPercentileCountsRequest,
        PercentileChunk, PercentileBlobsRecord> {
    private static final String SQL_SELECT_STATEMENT = String.format(
            "select `%s`.`%s`.`start_timestamp`", TEST_DB_SCHEMA_NAME,
            PercentileBlobs.PERCENTILE_BLOBS.getName());
    private static final long PERIOD = 3L;

    @Override
    protected Table<PercentileBlobsRecord> getBlobsTable() {
        return PercentileBlobs.PERCENTILE_BLOBS;
    }

    @Override
    protected long getPeriod() {
        return PERIOD;
    }

    @Override
    protected String getRecordClassName() {
        return PercentileBlobsRecord.class.getSimpleName();
    }

    @Override
    protected Class<PercentileChunk> getChunkClass() {
        return PercentileChunk.class;
    }

    @Override
    protected ByteString getChunkContent(@Nonnull PercentileChunk chunk) {
        return Objects.requireNonNull(chunk).getContent();
    }

    @Override
    protected AbstractBlobsReader newReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
            @Nonnull Clock clock, @Nonnull DataSource dataSource) {
        Settings settings = new Settings().withRenderSchema(true)
                .withRenderMapping(new RenderMapping()
                        .withSchemata(new MappedSchema().withInput(Vmtdb.VMTDB.getName())
                                .withOutput(TEST_DB_SCHEMA_NAME)));
        return new PercentileReader(timeToWaitNetworkReadinessMs, grpcTimeoutMs, clock,
                DSL.using(dataSource, SQLDialect.MARIADB, settings));
    }

    @Override
    protected PercentileBlobsRecord newRecord(@Nonnull DSLContext context, @Nonnull String chunk,
            int chunkIndex, long timestamp) {
        Objects.requireNonNull(chunk);
        return Objects.requireNonNull(context).newRecord(getBlobsTable())
                .values(timestamp, PERIOD, chunk.getBytes(StandardCharsets.UTF_8), chunkIndex);
    }

    @Override
    protected GetPercentileCountsRequest newRequest(int chunkSize, long timestamp) {
        return GetPercentileCountsRequest.newBuilder().setChunkSize(chunkSize)
                .setStartTimestamp(timestamp).build();
    }

    @Override
    protected void addSqlResult(
            @Nonnull Queue<SqlWithResponse> sqlRequestToResponse,
            long timestamp, @Nullable Result<PercentileBlobsRecord> result) {
        Objects.requireNonNull(sqlRequestToResponse).add(new SqlWithResponse(
                SQL_SELECT_STATEMENT, Collections.singletonList(timestamp), result));
    }
}
