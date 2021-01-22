package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_STATS_HOURLY;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsStatsHourlyRecord;

/**
 * Implementation of store that accesses savings hourly/daily/monthly DB tables.
 */
public class SqlEntitySavingsStore implements EntitySavingsStore {
    /**
     * Minimal info logging.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Used for timestamp conversions before storing/reading DB values.
     */
    private final Clock clock;

    /**
     * Used for inserts, to enable batch insert. Default: 1000 ?
     */
    private final int chunkSize;

    /**
     * Need some dummy init time.
     */
    private static final LocalDateTime INIT_TIME = LocalDateTime.now();

    /**
     * New one creation.
     *
     * @param dsl JOOQ access.
     * @param clock Used for timestamp conversions before storing/reading DB values.
     * @param chunkSize Used for inserts, to enable batch insert.
     */
    public SqlEntitySavingsStore(@Nonnull final DSLContext dsl, @Nonnull final Clock clock,
            final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
        logger.info("Created new Entity Savings Store with chunk size {} and clock {}.",
                this.chunkSize, this.clock);
    }

    @Override
    public void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStats)
            throws EntitySavingsException {
        try {
            Iterators.partition(hourlyStats.iterator(), chunkSize)
                    .forEachRemaining(chunk -> dsl.transaction(transaction -> {
                        final DSLContext transactionContext = DSL.using(transaction);
                        InsertReturningStep<EntitySavingsStatsHourlyRecord> insert = dsl
                                .insertInto(ENTITY_SAVINGS_STATS_HOURLY)
                                .set(ENTITY_SAVINGS_STATS_HOURLY.ENTITY_ID, 0L)
                                .set(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME, INIT_TIME)
                                .set(ENTITY_SAVINGS_STATS_HOURLY.STATS_TYPE, 1)
                                .set(ENTITY_SAVINGS_STATS_HOURLY.STATS_VALUE, BigDecimal.ZERO)
                                .onDuplicateKeyIgnore();
                        final BatchBindStep batch = transactionContext.batch(insert);
                        chunk.forEach(stats -> {
                            batch.bind(stats.getEntityId(),
                                    getLocalDateTime(stats.getTimestamp()),
                                    stats.getType().getNumber(),
                                    stats.getValue());
                        });
                        if (batch.size() > 0) {
                            batch.execute();
                        }
                    }));
        } catch (Exception e) {
            throw new EntitySavingsException("Could not add " + hourlyStats.size()
                    + " hourly entity savings stats to DB.", e);
        }
    }

    @Nonnull
    @Override
    public Set<EntitySavingsStats> getHourlyStats(@Nonnull final Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime, @Nonnull final Set<Long> entityIds)
            throws EntitySavingsException {
        if (statsTypes.isEmpty() || entityIds.isEmpty()) {
            throw new EntitySavingsException("Cannot get hourly entity savings stats: Type count: "
                    + statsTypes.size() + ", Entity count: " + entityIds.size());
        }
        if (startTime > endTime) {
            throw new EntitySavingsException("Cannot get hourly entity savings stats: Start time: "
                    + getLocalDateTime(startTime) + ", End time: " + getLocalDateTime(endTime));
        }
        try {
            final Result<EntitySavingsStatsHourlyRecord> records = dsl
                    .selectFrom(ENTITY_SAVINGS_STATS_HOURLY)
                    .where(ENTITY_SAVINGS_STATS_HOURLY.ENTITY_ID.in(entityIds)
                            .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TYPE.in(statsTypes
                                    .stream()
                                    .map(EntitySavingsStatsType::getNumber)
                                    .collect(Collectors.toSet())))
                            .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME
                                    .ge(getLocalDateTime(startTime))
                                    .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME
                                            .lt(getLocalDateTime(endTime))))).fetch();
            return new HashSet<>(records.map(this::convertStatsDbRecord));
        } catch (Exception e) {
            throw new EntitySavingsException("Could not get hourly entity savings stats for "
                    + entityIds.size() + " entities from DB between " + getLocalDateTime(startTime)
                    + " and " + getLocalDateTime(endTime), e);
        }
    }

    /**
     * Gets stats DB record into Stats instance.
     *
     * @param rec DB record.
     * @return Stats instance will filled in values read from DB.
     */
    @Nonnull
    private EntitySavingsStats convertStatsDbRecord(@Nonnull final EntitySavingsStatsHourlyRecord rec) {
        return new EntitySavingsStats(rec.getEntityId(),
                TimeUtil.localDateTimeToMilli(rec.getStatsTime(), clock),
                EntitySavingsStatsType.forNumber(rec.getStatsType()),
                rec.getStatsValue().doubleValue());
    }

    /**
     * Util to convert epoch millis to LocalDateTime before storing into DB.
     *
     * @param timeMillis epoch millis.
     * @return LocalDateTime created from millis.
     */
    @Nonnull
    private static LocalDateTime getLocalDateTime(long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atZone(ZoneOffset.UTC).toLocalDateTime();
    }
}
