/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.TableField;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;

/**
 * {@link PercentileWriter} writes information about percentile into database.
 */
public class PercentileWriter extends AbstractBlobsWriter<SetPercentileCountsResponse, PercentileChunk,
        PercentileBlobsRecord> {
    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;
    private final Logger logger = LogManager.getLogger();

    /**
     * Creates {@link PercentileWriter} instance.
     *
     * @param responseObserver provides information about errors to the client side that called
     *                         percentile writer instance.
     * @param dataSource provides connection to database.
     */
    public PercentileWriter(@Nonnull StreamObserver<SetPercentileCountsResponse> responseObserver,
                    @Nonnull DataSource dataSource) {
        super(responseObserver, dataSource, SharedMetrics.PERCENTILE_WRITING, PERCENTILE_BLOBS_TABLE);
    }

    @Override
    protected long getStartTimestamp(@Nonnull PercentileChunk chunk) {
        return chunk.getStartTimestamp();
    }

    @Override
    protected ByteString getContent(@Nonnull PercentileChunk chunk) {
        return chunk.getContent();
    }

    @Nonnull
    @Override
    protected TableField<PercentileBlobsRecord, Long> getStartTimestampField() {
        return PERCENTILE_BLOBS_TABLE.START_TIMESTAMP;
    }

    @Override
    protected SetPercentileCountsResponse newResponse() {
        return SetPercentileCountsResponse.newBuilder().build();
    }

    @Override
    protected String getRecordSimpleName() {
        return PercentileBlobsRecord.class.getSimpleName();
    }

    @Override
    protected byte[] writeChunk(@Nonnull Connection connection, @Nonnull DSLContext context,
            int processedChunks, @Nonnull final PercentileChunk chunk) throws SQLException,
            IOException {
        final ByteString content = Objects.requireNonNull(chunk).getContent();
        final byte[] bytes = Objects.requireNonNull(content).toByteArray();
        try (PreparedStatement insertNewValues = Objects.requireNonNull(connection)
                .prepareStatement(context.insertInto(PERCENTILE_BLOBS_TABLE).columns(
                        PERCENTILE_BLOBS_TABLE.START_TIMESTAMP,
                        PERCENTILE_BLOBS_TABLE.AGGREGATION_WINDOW_LENGTH,
                        PERCENTILE_BLOBS_TABLE.DATA,
                        PERCENTILE_BLOBS_TABLE.CHUNK_INDEX
                ).values(chunk.getStartTimestamp(), chunk.getPeriod(), bytes, processedChunks).getSQL())) {
            try (ByteArrayInputStream data = new ByteArrayInputStream(bytes)) {
                insertNewValues.setLong(1, chunk.getStartTimestamp());
                insertNewValues.setLong(2, chunk.getPeriod());
                insertNewValues.setBinaryStream(3, data);
                insertNewValues.setInt(4, processedChunks);
                insertNewValues.execute();
            }
            logger.trace("Chunk#{} of {} data '{}' for '{}' timestamp and '{}' period has been processed.",
                    () -> processedChunks, this::getRecordSimpleName, content::size,
                    chunk::getStartTimestamp, chunk::getPeriod);
        }
        return bytes;
    }
}
