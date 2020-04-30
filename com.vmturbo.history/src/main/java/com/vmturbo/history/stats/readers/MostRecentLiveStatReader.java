package com.vmturbo.history.stats.readers;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static org.jooq.impl.DSL.field;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.stub.ServerCallStreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatHistoricalEpoch;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Reads the statistics tables and returns the most recent entries for the requested commodity
 * attributes. The hour, day and month tables are read.
 */
public class MostRecentLiveStatReader {

    private final HistorydbIO historydbIO;

    private final Logger logger = LogManager.getLogger();
    private static final DataMetricSummary GET_MOST_RECENT_STAT_READER = DataMetricSummary.builder()
            .withName("most_recent_live_stats_reader")
            .withHelp("Duration in seconds to read stats tables for most recent live statistic")
            .build()
            .register();

    /**
     * Creates an instance of MostRecentLiveStatReader.
     *
     * @param historydbIO instance to query the database.
     */
    public MostRecentLiveStatReader(@Nonnull final HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Returns the most recent statistic for the given commodityType and commodityKey from the
     * statistics tables determined by entityClassName. For example, entityClassName
     * VirtualMachine will look up statistics from the vm_stats_* set of tables.
     *
     * @param entityClassName whose tables the statistic should be retrieved from.
     * @param commodityType for which the most recent statistic is sought.
     * @param commodityKey of the commodity for which the most recent statistic is sought.
     * @param streamObserver to check if client has cancelled the request.
     * @return Optional of GetMostRecentStatResponse.Builder or empty if no statistics are found.
     */
    @Nonnull
    public Optional<GetMostRecentStatResponse.Builder> getMostRecentStat(
        @Nonnull final String entityClassName,
        @Nonnull final String commodityType,
        @Nonnull final String commodityKey,
        @Nullable final ServerCallStreamObserver streamObserver) {

        final EntityType entityType = EntityType.get(entityClassName);
        final List<Supplier<Optional<GetMostRecentStatResponse.Builder>>> orderedQueries = Arrays
                .asList(
                // 1. daily table query
                () -> retrieveMostRecentStat(createQuery(entityType.getDayTable().get(),
                    commodityType, commodityKey), StatHistoricalEpoch.DAY),
                // 2. monthly table query
                () -> retrieveMostRecentStat(createQuery(entityType.getMonthTable().get(),
                        commodityType, commodityKey), StatHistoricalEpoch.MONTH)
        );
        for (final Supplier<Optional<GetMostRecentStatResponse.Builder>> query : orderedQueries) {
            if (streamObserver != null && streamObserver.isCancelled()) {
                logger.debug("Client cancelled GetMostRecentStat request for volume id: {}",
                    commodityKey);
                return Optional.empty();
            }
            final Optional<GetMostRecentStatResponse.Builder> result = query.get();
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    private Optional<GetMostRecentStatResponse.Builder> retrieveMostRecentStat(
            final Select<?> query, final StatHistoricalEpoch epoch) {
        final DataMetricTimer timer = GET_MOST_RECENT_STAT_READER.startTimer();
        final Result<? extends Record> result = executeQuery(query);
        final double timeElapsedSecs = timer.observe();
        logger.trace("Most recent stat retrieval, query: {}, result: {}", () -> query,
                () -> result);
        logger.debug("Retrieval time for most recent stat query for epoch {} is {} seconds",
                epoch, timeElapsedSecs);
        return Optional.ofNullable(result)
                .filter(Result::isNotEmpty)
                .map(records -> createResponseBuilder(records.iterator().next(), epoch));
    }

    @Nullable
    private Result<? extends Record> executeQuery(final Select<?> query) {
        try {
            return historydbIO.execute(Style.IMMEDIATE, query);
        } catch (VmtDbException e) {
            logger.error(String.format("Error executing most recent stat retrieval query for %s ",
                    query), e);
            return null;
        }
    }

    private GetMostRecentStatResponse.Builder createResponseBuilder(
            final Record record, final StatHistoricalEpoch epoch) {
        return GetMostRecentStatResponse.newBuilder()
                .setEpoch(epoch)
                .setEntityUuid(Long.parseLong(String.valueOf(record.get(UUID))))
                .setSnapshotDate(((Timestamp)record.get(SNAPSHOT_TIME)).getTime());
    }

    private Select<?> createQuery(final Table<?> table,
                                  final String commodityType,
                                  final String commodityKey) {
        final List<Condition> conditions = Arrays.asList(
                JooqUtils.getStringField(table, PROPERTY_TYPE)
                        .eq(commodityType),
                JooqUtils.getStringField(table, COMMODITY_KEY)
                        .eq(commodityKey)
        );
        return HistorydbIO.getJooqBuilder()
                .select(field(UUID), field(SNAPSHOT_TIME))
                .from(table)
                .where(conditions)
                .orderBy(JooqUtils.getTimestampField(table, SNAPSHOT_TIME).desc());
    }
}
