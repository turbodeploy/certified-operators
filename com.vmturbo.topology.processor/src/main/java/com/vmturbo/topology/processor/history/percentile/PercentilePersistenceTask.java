package com.vmturbo.topology.processor.history.percentile;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.AbstractBlobsPersistenceTask;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Persist the percentile data to/from historydb.
 */
public class PercentilePersistenceTask extends AbstractBlobsPersistenceTask<PercentileCounts,
        PercentileRecord, PercentileChunk, SetPercentileCountsResponse, PercentileHistoricalEditorConfig> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Construct the task to load percentile data for the 'full window' from the persistent store.
     * @param statsHistoryClient persistent store grpc interface
     * @param clock The clock used for timing.
     * @param range range from start timestamp till end timestamp for which we need
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     */
    public PercentilePersistenceTask(@Nonnull StatsHistoryServiceStub statsHistoryClient,
                                     @Nonnull Clock clock,
                                     @Nonnull Pair<Long, Long> range, final boolean enableExpiredOidFiltering) {
        super(statsHistoryClient, clock, range, enableExpiredOidFiltering);
    }

    @Override
    protected void makeGetRequest(@Nonnull StatsHistoryServiceStub statsHistoryClient,
            @Nonnull PercentileHistoricalEditorConfig config, long startTimestamp,
            @Nonnull ReaderObserver observer) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(observer);
        Objects.requireNonNull(statsHistoryClient).getPercentileCounts(
                GetPercentileCountsRequest.newBuilder().setStartTimestamp(startTimestamp)
                        .setChunkSize((int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE))
                        .build(), observer);
    }

    @Override
    protected StreamObserver<PercentileChunk> makeSetRequest(
            @Nonnull StatsHistoryServiceStub statsHistoryClient, @Nonnull WriterObserver observer) {
        Objects.requireNonNull(observer);
        return Objects.requireNonNull(statsHistoryClient).setPercentileCounts(observer);
    }

    @Override
    protected byte[] toByteArray(@Nonnull PercentileCounts stats) {
        return Objects.requireNonNull(stats).toByteArray();
    }

    @Override
    protected int getCount(@Nonnull PercentileCounts stats) {
        return Objects.requireNonNull(stats).getPercentileRecordsCount();
    }

    @Override
    protected PercentileChunk newChunk(long startTimestamp, long periodMs, @Nonnull ByteString payload) {
        return PercentileChunk.newBuilder().setStartTimestamp(startTimestamp)
                .setPeriod(periodMs).setContent(Objects.requireNonNull(payload)).build();
    }

    @Override
    protected ByteString getChunkContent(@Nonnull PercentileChunk chunk) {
        return Objects.requireNonNull(chunk).getContent();
    }

    @Override
    protected long getChunkPeriod(@Nonnull PercentileChunk chunk) {
        return Objects.requireNonNull(chunk).getPeriod();
    }

    @Nonnull
    @Override
    protected Map<EntityCommodityFieldReference, PercentileRecord> parse(long startTimestamp,
                                                                         @Nonnull InputStream source,
                                                                         @Nonnull LongSet oidsToUse,
                                                                         boolean enableExpiredOidFiltering)
            throws IOException {
        return parse(startTimestamp, source, PercentileCounts::parseFrom, oidsToUse, enableExpiredOidFiltering);
    }

    /**
     * Parses raw bytes to create mapping from field reference to {@link PercentileRecord}
     * instance.
     *
     * @param startTimestamp timestamp for which source loaded
     * @param source data that have been loaded
     * @param parser method that need to be used to create {@link PercentileCounts}
     *                 instance from {@link InputStream}.
     * @param oidsToUse non expired oids that should be used
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     * @return mapping from field reference to appropriate percentile record instance.
     * @throws IOException in case of error while parsing percentile records.
     */
    @Nonnull
    protected static Map<EntityCommodityFieldReference, PercentileRecord> parse(long startTimestamp,
                                                                                @Nonnull InputStream source,
                                                                                @Nonnull ThrowingFunction<InputStream, PercentileCounts, IOException> parser,
                                                                                LongSet oidsToUse, boolean enableExpiredOidFiltering)
        throws IOException {

        final ThrowingFunction<InputStream, Iterable<PercentileRecord>, IOException> countsToRecords = inputStream -> {
            final PercentileCounts counts = parser.apply(source);
            if (counts == null) {
                throw new InvalidProtocolBufferException(String.format("Cannot parse '%s' for '%s' timestamp",
                    PercentileCounts.class.getSimpleName(), startTimestamp));
            }
            return counts.getPercentileRecordsList();
        };
        final Function<PercentileRecord, EntityCommodityFieldReference> recordToRef = record -> {
            final CommodityType.Builder commTypeBuilder =
                CommodityType.newBuilder().setType(record.getCommodityType());
            if (record.hasKey()) {
                commTypeBuilder.setKey(record.getKey());
            }
            final Long provider = record.hasProviderOid() ? record.getProviderOid() : null;
            return new EntityCommodityFieldReference(record.getEntityOid(),
                commTypeBuilder.build(), provider, CommodityField.USED);
        };
        return parseDbRecords(startTimestamp, source, countsToRecords, oidsToUse,
            PercentileRecord::getEntityOid, recordToRef, enableExpiredOidFiltering);
    }
}
