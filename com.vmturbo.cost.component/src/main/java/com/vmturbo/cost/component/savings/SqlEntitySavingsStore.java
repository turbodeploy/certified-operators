package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_STATS_HOURLY;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.trueCondition;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsStatsHourlyRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
     * Map of EntityType to DB column in scope table, so that we can query for right scope.
     */
    private static final Map<EntityType, TableField<EntityCloudScopeRecord, Long>> TYPE_TO_DB_MAP =
            ImmutableMap.of(
                    EntityType.BUSINESS_ACCOUNT, ENTITY_CLOUD_SCOPE.ACCOUNT_OID,
                    EntityType.REGION, ENTITY_CLOUD_SCOPE.REGION_OID,
                    EntityType.AVAILABILITY_ZONE, ENTITY_CLOUD_SCOPE.AVAILABILITY_ZONE_OID,
                    EntityType.SERVICE_PROVIDER, ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID
            );

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
    public Set<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException {
        if (statsTypes.isEmpty() || entitiesByType.isEmpty()) {
            throw new EntitySavingsException("Cannot get hourly entity savings stats: Type count: "
                    + statsTypes.size() + ", Entity type count: " + entitiesByType.size());
        }
        if (startTime > endTime) {
            throw new EntitySavingsException("Cannot get hourly entity savings stats: Start time: "
                    + getLocalDateTime(startTime) + ", End time: " + getLocalDateTime(endTime));
        }
        try {
            final Set<Integer> statsTypeCodes = statsTypes.stream()
                    .map(EntitySavingsStatsType::getNumber)
                    .collect(Collectors.toSet());
            final Result<Record3<LocalDateTime, Integer, BigDecimal>> records = dsl
                    .select(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME,
                            ENTITY_SAVINGS_STATS_HOURLY.STATS_TYPE,
                            sum(ENTITY_SAVINGS_STATS_HOURLY.STATS_VALUE)
                                    .as(ENTITY_SAVINGS_STATS_HOURLY.STATS_VALUE))
                    .from(ENTITY_SAVINGS_STATS_HOURLY)
                    .join(ENTITY_CLOUD_SCOPE)
                    .on(ENTITY_SAVINGS_STATS_HOURLY.ENTITY_ID.eq(ENTITY_CLOUD_SCOPE.ENTITY_OID))
                    .where(getCondition(entitiesByType))
                            .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TYPE.in(statsTypeCodes))
                            .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME
                                    .ge(getLocalDateTime(startTime))
                                    .and(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME
                                            .lt(getLocalDateTime(endTime))))
                    .groupBy(ENTITY_SAVINGS_STATS_HOURLY.STATS_TIME,
                            ENTITY_SAVINGS_STATS_HOURLY.STATS_TYPE)
                    .fetch();
            return new HashSet<>(records.map(this::convertStatsDbRecord));
        } catch (Exception e) {
            throw new EntitySavingsException("Could not get hourly entity savings stats for "
                    + entitiesByType.size() + " entity types from DB between "
                    + getLocalDateTime(startTime)
                    + " and " + getLocalDateTime(endTime), e);
        }
    }

    /**
     * Makes up Condition based on entityType to OID map.
     *
     * @param entitiesByType EntityType to OIDs.
     * @return Set of conditions.
     */
    private List<Condition> getCondition(@Nonnull MultiValuedMap<EntityType, Long> entitiesByType) {
        final List<Condition> conditions = new ArrayList<>();
        final Set<Long> rawEntityOids = new HashSet<>();
        entitiesByType.keySet().forEach(entityType -> {
            TableField<EntityCloudScopeRecord, Long> dbField = TYPE_TO_DB_MAP.get(entityType);
            if (dbField == null) {
                // If no higher scope type (like account/region) provided, then assume entity.
                rawEntityOids.addAll(entitiesByType.get(entityType));
            } else {
                conditions.add(getCondition(entitiesByType.get(entityType), dbField));
            }
        });
        conditions.add(getCondition(rawEntityOids, ENTITY_CLOUD_SCOPE.ENTITY_OID));
        return conditions;
    }

    /**
     * Makes up one condition based on OID inputs.
     *
     * @param oidValues Values of entity OIDs.
     * @param dbField DB column for condition.
     * @return Condition created.
     */
    @Nonnull
    private Condition getCondition(@Nonnull final Collection<Long> oidValues,
            @Nonnull final TableField<EntityCloudScopeRecord, Long> dbField) {
        if (oidValues.isEmpty()) {
            return trueCondition();
        }
        if (oidValues.size() == 1) {
            return dbField.eq(oidValues.iterator().next());
        }
        return dbField.in(oidValues);
    }

    /**
     * Gets stats DB record into Stats instance.
     *
     * @param rec DB record.
     * @return Stats instance will filled in values read from DB.
     */
    @Nonnull
    private AggregatedSavingsStats convertStatsDbRecord(
            @Nonnull final Record3<LocalDateTime, Integer, BigDecimal> rec) {
        return new AggregatedSavingsStats(TimeUtil.localDateTimeToMilli(rec.value1(), clock),
                EntitySavingsStatsType.forNumber(rec.value2()),
                rec.value3().doubleValue());
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

    /**
     * Only for testing.
     *
     * @return DSL context.
     */
    @Nonnull
    @VisibleForTesting
    DSLContext getDsl() {
        return dsl;
    }
}
