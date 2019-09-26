/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;
import com.vmturbo.history.stats.RequestBasedReader;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link PercentileReader} reads percentile information from database. For more information,
 * please, look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 */
public class PercentileReader
                implements RequestBasedReader<GetPercentileCountsRequest, PercentileChunk> {
    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;
    private static final Logger LOGGER = LogManager.getLogger();
    private final HistorydbIO historydbIO;

    /**
     * Creates {@link PercentileReader} instance.
     *
     * @param historydbIO provides connection to database.
     */
    public PercentileReader(@Nonnull HistorydbIO historydbIO) {
        this.historydbIO = Objects.requireNonNull(historydbIO, "HistorydbIO cannot be null");
    }

    private static void convert(PercentileBlobsRecord record, int chunkSize,
                    StreamObserver<PercentileChunk> responseObserver) {
        int totalProcessed = 0;
        try {
            final byte[] sourceData = record.getData();
            while (totalProcessed < sourceData.length) {
                final PercentileChunk.Builder percentileChunkBuilder = PercentileChunk.newBuilder();
                percentileChunkBuilder.setPeriod(record.getAggregationWindowLength());
                final long startTimestamp = record.getStartTimestamp().getTime();
                percentileChunkBuilder.setStartTimestamp(startTimestamp);
                final ByteString data = ByteString.readFrom(
                                new ByteArrayInputStream(sourceData, totalProcessed, chunkSize));
                percentileChunkBuilder.setContent(data);
                totalProcessed += data.size();
                responseObserver.onNext(percentileChunkBuilder.build());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Percentile chunk#{} for {} processed({}/{})",
                                    sourceData.length / chunkSize, startTimestamp, totalProcessed,
                                    sourceData.length);
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Cannot split the data from table '{}' into chunks for start timestamp '{}' and period '{}'",
                            PERCENTILE_BLOBS_TABLE, record.getStartTimestamp(),
                            record.getAggregationWindowLength(), ex);
            responseObserver.onError(
                            Status.INTERNAL.withDescription(ex.getMessage()).asException());
        }
    }

    @Override
    public void processRequest(@Nonnull GetPercentileCountsRequest request,
                    @Nonnull StreamObserver<PercentileChunk> responseObserver) {
        try (DataMetricTimer dataMetricTimer = SharedMetrics.PERCENTILE_READING.startTimer()) {
            final long startTimestamp = request.getStartTimestamp();
            try (Connection connection = historydbIO.transConnection();
                            DSLContext context = historydbIO.using(connection)) {
                final Result<PercentileBlobsRecord> percentileBlobsRecords =
                                context.selectFrom(PERCENTILE_BLOBS_TABLE)
                                                .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP
                                                                .eq(new Timestamp(startTimestamp)))
                                                .fetch();
                final int amountOfRecords = percentileBlobsRecords.size();
                if (amountOfRecords < 1) {
                    final String message =
                                    String.format("There is no percentile information for '%s' timestamp",
                                                    startTimestamp);
                    LOGGER.warn(message);
                    responseObserver.onCompleted();
                    return;
                }
                if (amountOfRecords > 1) {
                    LOGGER.warn("There are '{}' percentile records for '{}' timestamp, first random will be chosen",
                                    amountOfRecords, startTimestamp);
                }
                convert(percentileBlobsRecords.iterator().next(), request.getChunkSize(),
                                responseObserver);
                LOGGER.trace("Percentile data read from database for '{}' timestamp in '{}'",
                                () -> startTimestamp, dataMetricTimer::getTimeElapsedSecs);
                responseObserver.onCompleted();
            } catch (VmtDbException | SQLException ex) {
                LOGGER.error("Cannot extract data from the database for '{}'", startTimestamp, ex);
                responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        }
    }
}
