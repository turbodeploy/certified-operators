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

import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.schema.abstraction.tables.MovingStatisticsBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.MovingStatisticsBlobsRecord;

/**
 * {@link MovingStatisticsWriter} writes information about moving statistics into database.
 */
public class MovingStatisticsWriter extends AbstractBlobsWriter<SetMovingStatisticsResponse,
        MovingStatisticsChunk, MovingStatisticsBlobsRecord> {
    private static final MovingStatisticsBlobs MOVING_STATISTICS_BLOBS_TABLE = MovingStatisticsBlobs.MOVING_STATISTICS_BLOBS;
    private final Logger logger = LogManager.getLogger();

    /**
     * Creates {@link MovingStatisticsWriter} instance.
     *
     * @param responseObserver provides information about errors to the client side that called
     *                         the moving statistics writer instance.
     * @param dataSource provides connection to database.
     */
    public MovingStatisticsWriter(@Nonnull StreamObserver<SetMovingStatisticsResponse> responseObserver,
                    @Nonnull DataSource dataSource) {
        super(responseObserver, dataSource, SharedMetrics.PERCENTILE_WRITING, MOVING_STATISTICS_BLOBS_TABLE);
    }

    @Override
    protected long getStartTimestamp(@Nonnull MovingStatisticsChunk chunk) {
        return chunk.getStartTimestamp();
    }

    @Override
    protected ByteString getContent(@Nonnull MovingStatisticsChunk chunk) {
        return chunk.getContent();
    }

    @Nonnull
    @Override
    protected TableField<MovingStatisticsBlobsRecord, Long> getStartTimestampField() {
        return MOVING_STATISTICS_BLOBS_TABLE.START_TIMESTAMP;
    }

    @Override
    protected SetMovingStatisticsResponse newResponse() {
        return SetMovingStatisticsResponse.newBuilder().build();
    }

    @Override
    protected String getRecordSimpleName() {
        return MovingStatisticsBlobsRecord.class.getSimpleName();
    }

    @Override
    protected byte[] writeChunk(@Nonnull Connection connection, @Nonnull DSLContext context,
            int processedChunks, @Nonnull MovingStatisticsChunk chunk) throws SQLException,
            IOException {
        final ByteString content = Objects.requireNonNull(chunk).getContent();
        final byte[] bytes = Objects.requireNonNull(content).toByteArray();
        try (PreparedStatement insertNewValues = Objects.requireNonNull(connection)
                .prepareStatement(context.insertInto(MOVING_STATISTICS_BLOBS_TABLE).columns(
                        MOVING_STATISTICS_BLOBS_TABLE.START_TIMESTAMP,
                        MOVING_STATISTICS_BLOBS_TABLE.DATA,
                        MOVING_STATISTICS_BLOBS_TABLE.CHUNK_INDEX
                ).values(chunk.getStartTimestamp(), bytes, processedChunks).getSQL())) {
            try (ByteArrayInputStream data = new ByteArrayInputStream(bytes)) {
                insertNewValues.setLong(1, chunk.getStartTimestamp());
                insertNewValues.setBinaryStream(2, data);
                insertNewValues.setInt(3, processedChunks);
                insertNewValues.execute();
            }
            logger.trace("Chunk#{} of {} data '{}' for '{}' timestamp has been processed.",
                    () -> processedChunks, this::getRecordSimpleName, content::size,
                    chunk::getStartTimestamp);
        }
        return bytes;
    }
}
