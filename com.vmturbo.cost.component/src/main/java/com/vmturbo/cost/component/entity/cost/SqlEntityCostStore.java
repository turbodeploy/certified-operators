package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.PLAN_ENTITY_COST;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.trueCondition;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.math.DoubleMath;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record7;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.Builder;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCostByDayRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostByHourRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostRecord;
import com.vmturbo.cost.component.persistence.DataIngestionBouncer;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.CostGroupBy;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;
import com.vmturbo.trax.TraxNumber;

public class SqlEntityCostStore implements EntityCostStore, MultiStoreDiagnosable {

    private static final Logger logger = LogManager.getLogger();

    // Tables that contain non-projected entity costs.
    private static final Set<Table<?>> nonProjectedEntityCostTables =
            ImmutableSet.of(ENTITY_COST, PLAN_ENTITY_COST);

    private final DSLContext dsl;

    private final Clock clock;

    private final ExecutorService batchExecutorService;

    private final int chunkSize;

    private static final String entityCostDumpFile = "entityCost_dump";

    private final LatestEntityCostsDiagsHelper latestEntityCostsDiagsHelper;

    private final EntityCostsByMonthDiagsHelper entityCostsByMonthDiagsHelper;

    private final EntityCostsByDayDiagsHelper entityCostsByDayDiagsHelper;

    private final EntityCostsByHourDiagsHelper entityCostsByHourDiagsHelper;

    private final InMemoryEntityCostStore latestEntityCostStore;

    private final DataIngestionBouncer ingestionBouncer;

    public SqlEntityCostStore(@Nonnull final DSLContext dsl,
                              @Nonnull final Clock clock,
                              @Nonnull ExecutorService batchExecutorService,
                              final int chunkSize,
                              @Nonnull InMemoryEntityCostStore inMemoryEntityCostStore,
                              @Nonnull DataIngestionBouncer ingestionBouncer) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.batchExecutorService = Objects.requireNonNull(batchExecutorService);
        this.chunkSize = chunkSize;
        this.latestEntityCostsDiagsHelper = new LatestEntityCostsDiagsHelper(dsl);
        this.entityCostsByMonthDiagsHelper = new EntityCostsByMonthDiagsHelper(dsl);
        this.entityCostsByDayDiagsHelper = new EntityCostsByDayDiagsHelper(dsl);
        this.entityCostsByHourDiagsHelper = new EntityCostsByHourDiagsHelper(dsl);
        this.latestEntityCostStore = inMemoryEntityCostStore;
        this.ingestionBouncer = Objects.requireNonNull(ingestionBouncer);
    }

    @Override
    public void persistEntityCost(
            @Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> costJournals,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
            final long topologyCreationTimeOrContextId, final boolean isPlan) throws DbException {

        logger.info("Persisting {} entity cost journals", costJournals::size);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final LocalDateTime createdTime =
                Instant.ofEpochMilli(isPlan
                        ? System.currentTimeMillis()
                        : topologyCreationTimeOrContextId).atZone(ZoneOffset.UTC).toLocalDateTime();
        if (!isPlan) {
            final List<EntityCost> entityCosts = Collections2.transform(costJournals.values(),
                     CostJournal::toEntityCostProto).stream().collect(Collectors.toList());
            latestEntityCostStore.updateEntityCosts(entityCosts);
        }

        if (isPlan || ingestionBouncer.isTableIngestible(ENTITY_COST)) {
            final Optional<Long> planId =
                    isPlan ? Optional.of(topologyCreationTimeOrContextId) : Optional.empty();
            final CompletionService<Void> completionService = new ExecutorCompletionService(batchExecutorService);
            final List<Future<Void>> insertTaskFutures = Streams.stream(Iterables.partition(costJournals.values(), chunkSize))
                    .map(chunk -> (Runnable)() -> batchInsertTask(cloudTopology, chunk, planId, createdTime))
                    .map(taskRunnable -> completionService.submit(taskRunnable, null))
                    .collect(ImmutableList.toImmutableList());

            // wait for all tasks to complete
            for (int i = 0; i < insertTaskFutures.size(); i++) {
                try {
                    completionService.take().get();
                } catch (ExecutionException | InterruptedException e) {
                    logger.error(
                            "Error during bulk entity cost insert. Canceling all remaining tasks", e);
                }
            }

            logger.info("Persisted {} entity cost journals in {}", costJournals::size, stopwatch::elapsed);

        } else {
            logger.warn("Skipping entity cost persistence for topology due to long running delete");
        }
    }

    private boolean isInMemoryCostRequest(@Nonnull final CostFilter entityCostFilter) {
        return (entityCostFilter.isLatest() || entityCostFilter.isLatestTimeStampRequested())
                && entityCostFilter.getTimeFrame() == TimeFrame.LATEST
                && !entityCostFilter.hasPlanTopologyContextId();
    }

    @Override
    public Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final CostFilter entityCostFilter) throws DbException {
        try {
            if (isInMemoryCostRequest(entityCostFilter)) {
                logger.debug("Current entity costs requetsed. All Account, Region, Zone filters will be transformed" +
                        " to entity Id filters and duration filters ignored.");
                EntityCostFilter entityFilter = transformFiltersToEntityIds((EntityCostFilter) entityCostFilter);
                return ImmutableMap.of(System.currentTimeMillis(),
                        latestEntityCostStore.getEntityCosts(entityFilter));
            }
            final Table<?> table = entityCostFilter.getTable();
            final Field<Long> entityId = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_ID);
            final Field<LocalDateTime> createdTime = getField(table, ENTITY_COST.CREATED_TIME);
            final Field<Integer> entityType = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_TYPE);
            final Field<Integer> costType = getField(table, ENTITY_COST.COST_TYPE);
            final Field<Integer> currency = getField(table, ENTITY_COST.CURRENCY);
            final Field<BigDecimal> amount = getField(table, ENTITY_COST.AMOUNT);
            List<Field<?>> list = Arrays.asList(entityId, createdTime, entityType, costType, currency, amount);
            List<Field<?>> modifiableList = new ArrayList<>(list);
            if (nonProjectedEntityCostTables.contains(table)) {
                final Field<Integer> costSource = getField(table, ENTITY_COST.COST_SOURCE);
                modifiableList.add(costSource);
            }

            SelectConditionStep<Record> selectCondition = dsl
                    .select(modifiableList)
                    .from(table)
                    .where(entityCostFilter.getConditions());

            // If latest timestamp is requested only return the info related to latest timestamp (created_time)
            // we want to get the latest time stamp for every entity. Since entities can have different
            // max(created_time), we create a tmp table with (entityId, max_created_time) and join with
            // cost table, so that each entity will have a record with its own entity time.
            if (entityCostFilter.isLatestTimeStampRequested()) {
                final String maxCreatedTime = "max_created_time";
                SelectHavingStep tmpTable = dsl.select(entityId, createdTime.max().as(maxCreatedTime))
                        .from(table).where(entityCostFilter.getConditions()).groupBy(entityId);
                final Field<Long> entityIdTmp = tmpTable.field(ENTITY_COST.ASSOCIATED_ENTITY_ID);
                final Field<LocalDateTime> createdTimeMax = tmpTable.field(maxCreatedTime);

                selectCondition = dsl
                    .select(modifiableList)
                    .from(table)
                    .join(tmpTable)
                    .on(entityId.eq(entityIdTmp).and(createdTime.eq(createdTimeMax)))
                    .where(entityCostFilter.getConditions());
            }

            final Result<? extends Record> records = selectCondition.fetch();
            return constructEntityCostMap(records);
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB", e);
        }
    }

    /**
     * Construct StatRecord indexed by timeStamp. Values are List of StatRecordDTO.
     *
     * @param startDate start date
     * @throws DbException if anything goes wrong while deleting entries from COST DB.
     */
    public void cleanEntityCosts(@Nonnull final LocalDateTime startDate) throws DbException {
        try {
            dsl.deleteFrom(ENTITY_COST)
                    .where(ENTITY_COST.CREATED_TIME.le(startDate))
                    .execute();
        } catch (DataAccessException e) {
            throw new DbException("Failed to clean entity costs to DB" + e.getMessage());
        }
    }

    @Override
    public Map<Long, Collection<StatRecord>> getEntityCostStats(
            @Nonnull final EntityCostFilter entityCostFilter) throws DbException {
        try {
            if (isInMemoryCostRequest(entityCostFilter)) {
                logger.debug("Current entity costs requetsed. All Account, Region, Zone filters will be transformed" +
                        " to entity Id filters and duration filters ignored.");
                EntityCostFilter entityFilter = transformFiltersToEntityIds(entityCostFilter);
                if (entityCostFilter.getCostGroupBy() != null && entityFilter.getRequestedGroupBy() != null) {
                    return ImmutableMap.of(System.currentTimeMillis(),
                            latestEntityCostStore.getEntityCostStatRecordsByGroup(entityFilter.getRequestedGroupBy(),
                                    entityFilter));
                } else {
                    return ImmutableMap.of(System.currentTimeMillis(),
                            latestEntityCostStore.getEntityCostStatRecords(entityFilter));

                }
            }
            if (entityCostFilter.getCostGroupBy() != null) {
                // Queries based on filter and groupBy. Returns only fields used for grouping.
                return fetchStatRecordsByGroup(entityCostFilter);
            } else {
                // Queries based on filter only. Returns all fields.
                final TimeFrame timeFrame =
                        entityCostFilter.isTotalValuesRequested() ? entityCostFilter.getTimeFrame()
                                : TimeFrame.HOUR;
                return constructStatRecordsMap(fetchRecords(entityCostFilter), timeFrame);
            }
        } catch (final DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB", e);
        }
    }

    private EntityCostFilter transformFiltersToEntityIds(final EntityCostFilter entityCostFilter) {

        EntityCostFilter.EntityCostFilterBuilder filterBuilder =
                entityCostFilter.toNewBuilder();

        /**
         * If the entity cost filter has either the region, account or zone filters,
         * fetch entity ids for the entity costs recorded for these filters and
         * create a cost filter with only entity ids.
         */
        if (entityCostFilter.getAccountIds().isPresent() ||
                entityCostFilter.getRegionIds().isPresent() ||
                entityCostFilter.getAvailabilityZoneIds().isPresent()) {
            final Table<?> table = entityCostFilter.getTable();
            final Field<Long> entityId = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_ID);
            final Result<Record1<Long>> records = dsl
                    .select(entityId)
                    .from(table)
                    .where(Arrays.asList(entityCostFilter.getConditions()))
                    .and(getConditionForEntityCost(dsl, entityCostFilter, table))
                    .fetch();
            Set<Long> entityIds = records.stream()
                    .map(r -> new RecordWrapper((r)))
                    .map(r -> r.getAssociatedEntityId())
                    .collect(Collectors.toSet());
            filterBuilder.entityIds(entityIds);
            // clear out the account Ids, availability zone ids and regions
            filterBuilder.clearRegionIds();
            filterBuilder.clearAccountIds();
            filterBuilder.clearAvailabilityZoneIds();
        }
        return filterBuilder.build();

    }

    @Nonnull
    private Map<Long, Collection<StatRecord>> fetchStatRecordsByGroup(
            @Nonnull final EntityCostFilter entityCostFilter) {
        final CostGroupBy costGroupBy = entityCostFilter.getCostGroupBy();
        final Set<Field<?>> groupByFields = costGroupBy.getGroupByFields();
        final @Nonnull Table<?> table = costGroupBy.getTable();
        final Set<Field<?>> selectableFields = Sets.newHashSet(groupByFields);
        selectableFields.add(sum(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(max(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(min(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(avg(costGroupBy.getAmountFieldInTable()));
        final Result<Record> result = dsl.select(selectableFields)
                .from(table)
                .where(Arrays.asList(entityCostFilter.getConditions()))
                .and(getConditionForEntityCost(dsl, entityCostFilter, table))
                .groupBy(groupByFields)
                .fetch();
        final TimeFrame timeFrame =
                entityCostFilter.isTotalValuesRequested() ? entityCostFilter.getTimeFrame()
                        : TimeFrame.HOUR;
        return createGroupByStatRecords(result, table, selectableFields, timeFrame);
    }

    @Nonnull
    private Result<? extends Record> fetchRecords(@Nonnull final CostFilter entityCostFilter) {
        final Table<?> table = entityCostFilter.getTable();
        final Field<Long> entityId = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_ID);
        final Field<LocalDateTime> createdTime = getField(table, ENTITY_COST.CREATED_TIME);
        final Field<Integer> entityType = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_TYPE);
        final Field<Integer> costType = getField(table, ENTITY_COST.COST_TYPE);
        final Field<Integer> currency = getField(table, ENTITY_COST.CURRENCY);
        final Field<BigDecimal> amount = getField(table, ENTITY_COST.AMOUNT);
        final List<Field<?>> list = Arrays.asList(entityId, createdTime, entityType, costType, currency, amount);
        final List<Field<?>> modifiableList = new ArrayList<>(list);
        if (nonProjectedEntityCostTables.contains(table)) {
            final Field<Integer> costSource = getField(table, ENTITY_COST.COST_SOURCE);
            modifiableList.add(costSource);
        }
        return dsl
                .select(modifiableList)
                .from(table)
                .where(Arrays.asList(entityCostFilter.getConditions()))
                .and(getConditionForEntityCost(dsl, entityCostFilter, table))
                .fetch();
    }

    /**
     * Construct entity cost map. Key is timestamp in long, Values are List of EntityCost DTOs.
     * It will first group the records by timestamp, and combine the entity costs with same id.
     *
     * @param entityCostRecords entity cost records in db
     * @return Entity cost map, key is timestamp in long, values are List of EntityCost DTOs.
     */
    @Nonnull
    private Map<Long, Map<Long, EntityCost>> constructEntityCostMap(@Nonnull final Result<? extends Record> entityCostRecords) {
        final Map<Long, Map<Long, EntityCost>> records = new HashMap<>();
        entityCostRecords.forEach(entityRecord -> {
            Map<Long, EntityCost> costsForTimestamp = records
                    .computeIfAbsent(TimeUtil.localDateTimeToMilli((LocalDateTime)entityRecord.getValue(ENTITY_COST.CREATED_TIME.getName()), clock), k -> new HashMap<>());
            //TODO: optimize to avoid building EntityCost
            final EntityCost newCost = toEntityCostDTO(new RecordWrapper(entityRecord));
            costsForTimestamp.compute(newCost.getAssociatedEntityId(),
                    (id, existingCost) -> existingCost == null
                            ? newCost
                            : existingCost.toBuilder()
                                    .addAllComponentCost(newCost.getComponentCostList())
                                    .build());
        });
        return records;
    }

    @Nonnull
    private Map<Long, Collection<StatRecord>> createGroupByStatRecords(
            @Nonnull final Result<Record> res, final @Nonnull Table<?> table,
            @Nonnull final Set<Field<?>> selectableFields, final @Nonnull TimeFrame timeFrame) {
        final Map<Long, Collection<StatRecord>> statRecordsByTimeStamp = Maps.newHashMap();
        res.forEach(item -> {
            Collection<StatRecord> entityCosts = statRecordsByTimeStamp.computeIfAbsent(TimeUtil
                    .localDateTimeToMilli(item.getValue(ENTITY_COST.CREATED_TIME),
                    clock), k -> Sets.newHashSet());
            entityCosts.add(mapToEntityCost(item, table, selectableFields, timeFrame));
        });
        return statRecordsByTimeStamp;
    }

    @Nonnull
    private StatRecord mapToEntityCost(@Nonnull final Record item, @Nonnull final Table<?> table,
            @Nonnull final Set<Field<?>> selectableFields, final @Nonnull TimeFrame timeFrame) {
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder();
        if (selectableFields.contains(getField(table, ENTITY_COST.COST_TYPE))) {
            statRecordBuilder.setCategory(CostCategory.forNumber(item.get(ENTITY_COST.COST_TYPE)));
        }
        if (selectableFields.contains(getField(table, ENTITY_COST.COST_SOURCE))) {
            statRecordBuilder.setCostSource(
                    CostSource.forNumber(item.get(ENTITY_COST.COST_SOURCE)));
        }
        if (selectableFields.contains(getField(table, ENTITY_COST.ASSOCIATED_ENTITY_TYPE))) {
            statRecordBuilder.setAssociatedEntityType(
                    item.getValue(ENTITY_COST.ASSOCIATED_ENTITY_TYPE));
        }
        if (selectableFields.contains(getField(table, ENTITY_COST.ASSOCIATED_ENTITY_ID))) {
            statRecordBuilder.setAssociatedEntityId(
                    item.getValue(ENTITY_COST.ASSOCIATED_ENTITY_ID));
        }

        setStatRecordValues(statRecordBuilder, item.getValue("avg", Float.class),
                item.getValue("max", Float.class), item.getValue("min", Float.class),
                item.getValue("sum", Float.class), timeFrame);
        return statRecordBuilder.build();
    }

    private void setStatRecordValues(@Nonnull final StatRecord.Builder statRecordBuilder,
            final float avg, final float max, final float min, final float sum,
            final @Nonnull TimeFrame timeFrame) {
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits(timeFrame.getUnits());
        final double multiplier = timeFrame.getMultiplier();
        statRecordBuilder.setValues(StatValue.newBuilder()
                .setAvg((float)(avg * multiplier))
                .setMax((float)(max * multiplier))
                .setMin((float)(min * multiplier))
                .setTotal((float)(sum * multiplier))
                .build());
    }

    /**
     * Construct StatRecord indexed by timeStamp. Values are List of StatRecordDTO.
     *
     * @param entityCostRecords entity cost records in db
     * @return Entity cost map, key is timestamp in long, values are List of EntityCost DTOs.
     */
    @Nonnull
    private Map<Long, Collection<StatRecord>> constructStatRecordsMap(
            @Nonnull final Result<? extends Record> entityCostRecords,
            @Nonnull final TimeFrame timeFrame) {
        final Map<Long, Map<Long, EntityCost>> records = new HashMap<>();
        entityCostRecords.forEach(entityRecord -> {
            Map<Long, EntityCost> costsForTimestamp = records
                    .computeIfAbsent(TimeUtil.localDateTimeToMilli((LocalDateTime)entityRecord.getValue(1),
                            clock), k -> new HashMap<>());
            //TODO: optimize to avoid building EntityCost
            final EntityCost newCost = toEntityCostDTO(new RecordWrapper(entityRecord));
            costsForTimestamp.compute(newCost.getAssociatedEntityId(),
                    (id, existingCost) -> existingCost == null
                            ? newCost
                            : existingCost.toBuilder()
                                    .addAllComponentCost(newCost.getComponentCostList())
                                    .build());
        });
        Map<Long, Collection<StatRecord>> result = Maps.newHashMap();
        records.forEach((time, costsByEntity) -> {
            for (EntityCost entityCost : costsByEntity.values()) {
                final Collection<StatRecord> statRecords =
                        EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCost,
                                timeFrame);
                result.compute(time, (currentTime, currentValue) -> {
                    if (currentValue == null) {
                        currentValue = Lists.newArrayList();
                    }
                    currentValue.addAll(statRecords);
                    return currentValue;
                });
            }
        });
        return result;
    }

    /**
     * Convert EntityCostRecord DB record to entity cost proto DTO.
     *
     * @param recordWrapper Record from DB.
     * @return Transformed {@link EntityCost}.
     * */
    private EntityCost toEntityCostDTO(@Nonnull final RecordWrapper recordWrapper) {
        Builder componentCostBuilder = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(recordWrapper.getAmount().doubleValue())
                        .setCurrency(recordWrapper.getCurrency()))
                .setCategory(CostCategory.forNumber(recordWrapper.getCostType()));
        if (recordWrapper.getCostSource() != null) {
            componentCostBuilder.setCostSource(CostSource.forNumber(recordWrapper.getCostSource()));
        }
        return Cost.EntityCost.newBuilder()
                .setAssociatedEntityId(recordWrapper.getAssociatedEntityId())
                .addComponentCost(componentCostBuilder.build())
                .setAssociatedEntityType(recordWrapper.getAssociatedEntityType())
                .build();
    }

    /**
     * Helper method to create the extra condition based on if the query is latest EntityCost only.
     *
     * @param dsl      dslContext used.
     * @param entityCostFilter Filter for requested entity costs.
     * @param table table to be used in condition.
     * @return {@link Condition}. This is used in the query.
     */
    @Nonnull
    private Condition getConditionForEntityCost(
            @Nonnull final DSLContext dsl,
            @Nonnull final CostFilter entityCostFilter,
            @Nonnull final Table<?> table) {
        final Field<LocalDateTime> createdTime = getField(table, ENTITY_COST.CREATED_TIME);
        if (entityCostFilter.isLatest() && !entityCostFilter.hasPlanTopologyContextId()) {
            return createdTime.eq(dsl.select(max(createdTime)).from(table));
        } else {
            return trueCondition();
        }
    }

    private static <T> Field<T> getField(
            @Nonnull final Table<?> table,
            @Nonnull final TableField<EntityCostRecord, T> field) {
        return (Field<T>)table.field(field.getName());
    }

    @Override
    public Set<Diagnosable> getDiagnosables(final boolean collectHistoricalStats) {
        HashSet<Diagnosable> storesToSave = new HashSet<>();
        storesToSave.add(latestEntityCostsDiagsHelper);
        if (collectHistoricalStats) {
            storesToSave.add(entityCostsByDayDiagsHelper);
            storesToSave.add(entityCostsByHourDiagsHelper);
            storesToSave.add(entityCostsByMonthDiagsHelper);
        }
        return storesToSave;
    }

    private void batchInsertTask(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                 @Nonnull List<CostJournal<TopologyEntityDTO>> costJournals,
                                 @Nonnull Optional<Long> planId,
                                 @Nonnull LocalDateTime createdTime) {

        final Table table = planId.isPresent() ? PLAN_ENTITY_COST : ENTITY_COST;
        final CostSourceFilter costSourceFilter = planId.isPresent()
                ? CostSourceFilter.INCLUDE_ALL
                : CostSourceFilter.EXCLUDE_UPTIME;

        dsl.transaction(transaction -> {
            try (DSLContext transactionContext = DSL.using(transaction)) {
                // Initialize the batch.
                // Provide dummy values for jooq
                InsertSetMoreStep insert = dsl.insertInto(table)
                        .set(getField(table, ENTITY_COST.ASSOCIATED_ENTITY_ID), 0L)
                        .set(getField(table, ENTITY_COST.CREATED_TIME), createdTime)
                        .set(getField(table, ENTITY_COST.ASSOCIATED_ENTITY_TYPE), 0)
                        .set(getField(table, ENTITY_COST.COST_TYPE), 0)
                        .set(getField(table, ENTITY_COST.COST_SOURCE), 0)
                        .set(getField(table, ENTITY_COST.CURRENCY), 0)
                        .set(getField(table, ENTITY_COST.AMOUNT), BigDecimal.valueOf(0))
                        .set(getField(table, ENTITY_COST.ACCOUNT_ID), 0L)
                        .set(getField(table, ENTITY_COST.AVAILABILITY_ZONE_ID), 0L)
                        .set(getField(table, ENTITY_COST.REGION_ID), 0L);
                if (planId.isPresent()) {
                    insert.set(PLAN_ENTITY_COST.PLAN_ID, 0L);
                }
                final BatchBindStep batch = transactionContext.batch(insert);

                // Bind values to the batch insert statement. Each "bind" should have values for
                // all fields set during batch initialization.

                costJournals.forEach(journal -> {
                    try {
                        journal.getCategories().forEach(costType -> {
                            Map<CostSource, TraxNumber> filteredCostsBySource = journal
                                    .getFilteredCategoryCostsBySource(
                                            costType,
                                            costSourceFilter);
                            filteredCostsBySource.forEach((costSource, categoryCost) -> {

                                // We should always record the on-demand rate cost source, in order to properly
                                // average the cost for an entity over time, including when an entity is powered
                                // off and the on-demand rate is zero.
                                final boolean persistCostItem = categoryCost != null
                                        && Double.isFinite(categoryCost.getValue())
                                        && (costSource == CostSource.ON_DEMAND_RATE
                                        || !DoubleMath.fuzzyEquals(0d, categoryCost.getValue(), .0000001d));

                                if (persistCostItem) {
                                    final long entityOid = journal.getEntity().getOid();
                                    if (planId.isPresent()) {
                                        batch.bind(entityOid,
                                                createdTime,
                                                journal.getEntity().getEntityType(),
                                                costType.getNumber(),
                                                costSource.getNumber(),
                                                // TODO (roman, Sept 5 2018): Not handling currency in cost
                                                //  calculation yet.
                                                CurrencyAmount.getDefaultInstance().getCurrency(),
                                                BigDecimal.valueOf(categoryCost.getValue()),
                                                cloudTopology.getOwner(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L),
                                                cloudTopology.getConnectedAvailabilityZone(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L),
                                                cloudTopology.getConnectedRegion(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L),
                                                planId.get());
                                    } else {
                                        batch.bind(entityOid,
                                                createdTime,
                                                journal.getEntity().getEntityType(),
                                                costType.getNumber(),
                                                costSource.getNumber(),
                                                // TODO (roman, Sept 5 2018): Not handling currency in cost
                                                //  calculation yet.
                                                CurrencyAmount.getDefaultInstance().getCurrency(),
                                                BigDecimal.valueOf(categoryCost.getValue()),
                                                cloudTopology.getOwner(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L),
                                                cloudTopology.getConnectedAvailabilityZone(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L),
                                                cloudTopology.getConnectedRegion(entityOid)
                                                        .map(TopologyEntityDTO::getOid).orElse(0L)
                                        );
                                    }
                                } else {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("Skipping cost item for entity '{}' with source '{}' and cost '{}'",
                                                journal.getEntity().getOid(), costSource, categoryCost);
                                    }
                                }
                            });
                        });
                    } catch (Exception e) {
                        logger.error("Error in processing journal for entity '{}'",
                                journal.getEntity().getOid(), e);
                    }
                });

                if (batch.size() > 0) {
                    logger.debug("Persisting batch of size: {}", batch::size);
                    batch.execute();
                }
            }
        });
    }

    /**
     * Remove plan entity cost snapshot that was created by a plan.
     * @param planId the ID of the plan for which to remove cost data.
     */
    public void deleteEntityCosts(final long planId) {
        logger.info("Deleting data from plan entity costs for planId: {}", planId);
        final int rowsDeleted = dsl
                .deleteFrom(PLAN_ENTITY_COST)
                .where(Tables.PLAN_ENTITY_COST.PLAN_ID.eq(planId))
                .execute();
        logger.info("Deleted {} records from plan entity costs for planId {}", rowsDeleted, planId);
    }

    /**
     * Stats table field info by rollup type.
     */
    final static Map<RollupDurationType, StatsTypeFields> statsFieldsByRollup =
            buildStatsFieldsForRollup();

    private static Map<RollupDurationType, StatsTypeFields> buildStatsFieldsForRollup() {
        ImmutableMap.Builder<RollupDurationType, StatsTypeFields> statsFieldsByRollupBuilder = new ImmutableMap.Builder<>();
        StatsTypeFields hourFields = new StatsTypeFields();
        hourFields.table = ENTITY_COST_BY_HOUR;
        hourFields.associatedEntityIdFiled = ENTITY_COST_BY_HOUR.ASSOCIATED_ENTITY_ID;
        hourFields.createdTimeField = ENTITY_COST_BY_HOUR.CREATED_TIME;
        hourFields.associatedEntityTypeField = ENTITY_COST_BY_HOUR.ASSOCIATED_ENTITY_TYPE;
        hourFields.costTypeField = ENTITY_COST_BY_HOUR.COST_TYPE;
        hourFields.currencyField = ENTITY_COST_BY_HOUR.CURRENCY;
        hourFields.amountField = ENTITY_COST_BY_HOUR.AMOUNT;
        hourFields.accountIdFiled = ENTITY_COST_BY_HOUR.ACCOUNT_ID;
        hourFields.regionIdFiled = ENTITY_COST_BY_HOUR.REGION_ID;
        hourFields.availabilityZoneIdFiled = ENTITY_COST_BY_HOUR.AVAILABILITY_ZONE_ID;
        hourFields.samplesField = ENTITY_COST_BY_HOUR.SAMPLES;
        statsFieldsByRollupBuilder.put(RollupDurationType.HOURLY, hourFields);

        StatsTypeFields dayFields = new StatsTypeFields();
        dayFields.table = ENTITY_COST_BY_DAY;
        dayFields.associatedEntityIdFiled = ENTITY_COST_BY_DAY.ASSOCIATED_ENTITY_ID;
        dayFields.createdTimeField = ENTITY_COST_BY_DAY.CREATED_TIME;
        dayFields.associatedEntityTypeField = ENTITY_COST_BY_DAY.ASSOCIATED_ENTITY_TYPE;
        dayFields.costTypeField = ENTITY_COST_BY_DAY.COST_TYPE;
        dayFields.currencyField = ENTITY_COST_BY_DAY.CURRENCY;
        dayFields.accountIdFiled = ENTITY_COST_BY_DAY.ACCOUNT_ID;
        dayFields.regionIdFiled = ENTITY_COST_BY_DAY.REGION_ID;
        dayFields.availabilityZoneIdFiled = ENTITY_COST_BY_DAY.AVAILABILITY_ZONE_ID;
        dayFields.amountField = ENTITY_COST_BY_DAY.AMOUNT;
        dayFields.samplesField = ENTITY_COST_BY_DAY.SAMPLES;
        statsFieldsByRollupBuilder.put(RollupDurationType.DAILY, dayFields);

        StatsTypeFields monthFields = new StatsTypeFields();
        monthFields.table = ENTITY_COST_BY_MONTH;
        monthFields.associatedEntityIdFiled = ENTITY_COST_BY_MONTH.ASSOCIATED_ENTITY_ID;
        monthFields.createdTimeField = ENTITY_COST_BY_MONTH.CREATED_TIME;
        monthFields.associatedEntityTypeField = ENTITY_COST_BY_MONTH.ASSOCIATED_ENTITY_TYPE;
        monthFields.costTypeField = ENTITY_COST_BY_MONTH.COST_TYPE;
        monthFields.currencyField = ENTITY_COST_BY_MONTH.CURRENCY;
        monthFields.accountIdFiled = ENTITY_COST_BY_MONTH.ACCOUNT_ID;
        monthFields.regionIdFiled = ENTITY_COST_BY_MONTH.REGION_ID;
        monthFields.availabilityZoneIdFiled = ENTITY_COST_BY_MONTH.AVAILABILITY_ZONE_ID;
        monthFields.amountField = ENTITY_COST_BY_MONTH.AMOUNT;
        monthFields.samplesField = ENTITY_COST_BY_MONTH.SAMPLES;
        statsFieldsByRollupBuilder.put(RollupDurationType.MONTHLY, monthFields);
        return statsFieldsByRollupBuilder.build();
    }

    @Override
    public void performRollup(@Nonnull RollupDurationType durationType,
            @Nonnull List<LocalDateTime> fromTimes, @Nonnull Clock clock) {
        fromTimes.forEach(fromTime -> getRollupUpsert(durationType, fromTime, clock));
    }

    private void getRollupUpsert(@Nonnull final RollupDurationType rollupDuration,
            @Nonnull final LocalDateTime fromDateTimes, @Nonnull final Clock clock) {
        final Table<?> source;
        final Table<?> target;
        final Field<Integer> fSourceSamples;
        final LocalDateTime toDateTime;
        final LocalDateTime sourceConditionTime;
        final StatsTypeFields sourceFields = new StatsTypeFields();
        if (rollupDuration == RollupDurationType.HOURLY) {
            source = ENTITY_COST;
            target = ENTITY_COST_BY_HOUR;
            fSourceSamples = DSL.inline(1);
            sourceFields.createdTimeField = ENTITY_COST.CREATED_TIME;
            toDateTime = fromDateTimes.truncatedTo(ChronoUnit.HOURS);
            sourceConditionTime = fromDateTimes;
        } else if (rollupDuration == RollupDurationType.DAILY) {
            source = ENTITY_COST;
            target = ENTITY_COST_BY_DAY;
            fSourceSamples = DSL.inline(1);
            sourceFields.createdTimeField = ENTITY_COST.CREATED_TIME;
            toDateTime = fromDateTimes.truncatedTo(ChronoUnit.DAYS);
            sourceConditionTime = fromDateTimes;
        } else {
            source = ENTITY_COST;
            target = ENTITY_COST_BY_MONTH;
            fSourceSamples = DSL.inline(1);
            sourceFields.createdTimeField = ENTITY_COST.CREATED_TIME;
            YearMonth month = YearMonth.from(fromDateTimes);
            toDateTime = month.atEndOfMonth().atStartOfDay();
            sourceConditionTime = fromDateTimes;
        }

        final StatsTypeFields rollupFields = statsFieldsByRollup.get(rollupDuration);

        final UpsertBuilder upsert = new UpsertBuilder().withTargetTable(target)
                .withSourceTable(source)
                .withInsertFields(rollupFields.associatedEntityIdFiled,
                        rollupFields.createdTimeField, rollupFields.associatedEntityTypeField,
                        rollupFields.costTypeField, rollupFields.currencyField,
                        rollupFields.amountField, rollupFields.samplesField,
                        rollupFields.accountIdFiled, rollupFields.regionIdFiled,
                        rollupFields.availabilityZoneIdFiled)
                .withInsertValue(rollupFields.createdTimeField, DSL.val(toDateTime))
                .withInsertValue(rollupFields.associatedEntityIdFiled,
                        DSL.max(ENTITY_COST.ASSOCIATED_ENTITY_ID).as("associated_entity_id"))
                .withInsertValue(rollupFields.associatedEntityTypeField,
                        DSL.max(ENTITY_COST.ASSOCIATED_ENTITY_TYPE).as("associated_entity_type"))
                .withInsertValue(rollupFields.costTypeField,
                        DSL.max(ENTITY_COST.COST_TYPE).as("cost_type"))
                .withInsertValue(rollupFields.currencyField,
                        DSL.max(ENTITY_COST.CURRENCY).as("currency"))
                .withInsertValue(rollupFields.accountIdFiled,
                        DSL.max(ENTITY_COST.ACCOUNT_ID).as("account_id"))
                .withInsertValue(rollupFields.regionIdFiled,
                        DSL.max(ENTITY_COST.REGION_ID).as("region_id"))
                .withInsertValue(rollupFields.availabilityZoneIdFiled,
                        DSL.max(ENTITY_COST.AVAILABILITY_ZONE_ID).as("availability_zone_id"))
                .withInsertValue(rollupFields.amountField, DSL.sum(ENTITY_COST.AMOUNT).as("amount"))
                .withInsertValue(rollupFields.samplesField, fSourceSamples)
                .withUpdateValue(rollupFields.samplesField, UpsertBuilder::sum)
                .withUpdateValue(rollupFields.amountField,
                        UpsertBuilder.avg(rollupFields.samplesField))
                .withSourceCondition(sourceFields.createdTimeField.eq(sourceConditionTime))
                .withSourceGroupBy(ENTITY_COST.ASSOCIATED_ENTITY_ID, ENTITY_COST.CREATED_TIME,
                        ENTITY_COST.ASSOCIATED_ENTITY_TYPE, ENTITY_COST.COST_TYPE,
                        ENTITY_COST.CURRENCY, ENTITY_COST.ACCOUNT_ID, ENTITY_COST.REGION_ID,
                        ENTITY_COST.AVAILABILITY_ZONE_ID)
                .withConflictColumns(rollupFields.associatedEntityIdFiled,
                        rollupFields.createdTimeField, rollupFields.associatedEntityTypeField,
                        rollupFields.costTypeField, rollupFields.currencyField,
                        rollupFields.accountIdFiled, rollupFields.regionIdFiled,
                        rollupFields.availabilityZoneIdFiled);
        upsert.getUpsert(dsl).execute();
    }

    /**
     * Used to store info about stats tables (hourly/daily/monthly fields).
     */
    private static final class StatsTypeFields {
        Table<?> table;
        TableField<?, Long> associatedEntityIdFiled;
        TableField<?, LocalDateTime> createdTimeField;
        TableField<?, Integer> associatedEntityTypeField;
        TableField<?, Integer> costTypeField;
        TableField<?, Integer> currencyField;
        TableField<?, BigDecimal> amountField;
        TableField<?, Long> accountIdFiled;
        TableField<?, Long> regionIdFiled;
        TableField<?, Long> availabilityZoneIdFiled;
        TableField<?, Integer> samplesField;
    }

    /**
     * A wrapper class to wrap {@link Record7} class, to make it more readable.
     */
    class RecordWrapper {
        final Record record7;

        RecordWrapper(Record record7) {
            this.record7 = record7;
        }

        long getAssociatedEntityId() {
            return record7.get(ENTITY_COST.ASSOCIATED_ENTITY_ID);
        }

        LocalDateTime getCreatedTime() {
            return record7.get(ENTITY_COST.CREATED_TIME);
        }

        int getAssociatedEntityType() {
            return record7.get(ENTITY_COST.ASSOCIATED_ENTITY_TYPE);
        }

        int getCostType() {
            return record7.get(ENTITY_COST.COST_TYPE);
        }

        Integer getCostSource() {
            try {
                return record7.get(ENTITY_COST.COST_SOURCE);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        int getCurrency() {
            return record7.get(ENTITY_COST.CURRENCY);
        }

        BigDecimal getAmount() {
            return record7.get(ENTITY_COST.AMOUNT);
        }
    }

    /**
     * Helper class for dumping latest entity cost db records to exported topology.
     */
    private static final class LatestEntityCostsDiagsHelper implements
            TableDiagsRestorable<Object, EntityCostRecord> {

        private final DSLContext dsl;

        LatestEntityCostsDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<EntityCostRecord> getTable() {
            return ENTITY_COST;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return entityCostDumpFile;
        }
    }

    /**
     * Helper class for dumping daily entity cost db records to exported topology.
     */
    private static final class EntityCostsByDayDiagsHelper implements TableDiagsRestorable<Object, EntityCostByDayRecord> {
        private static final String entityCostByDayDumpFile = "entityCostByDay_dump";

        private final DSLContext dsl;

        EntityCostsByDayDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<EntityCostByDayRecord> getTable() {
            return Tables.ENTITY_COST_BY_DAY;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return entityCostByDayDumpFile;
        }
    }

    /**
     * Helper class for dumping hourly entity cost db records to exported topology.
     */
    private static final class EntityCostsByHourDiagsHelper implements TableDiagsRestorable<Object, EntityCostByHourRecord> {
        private static final String entityCostByDayDumpFile = "entityCostByHour_dump";

        private final DSLContext dsl;

        EntityCostsByHourDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<EntityCostByHourRecord> getTable() {
            return Tables.ENTITY_COST_BY_HOUR;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return entityCostByDayDumpFile;
        }
    }

    /**
     * Helper class for dumping monthly entity cost db records to exported topology.
     */
    private static final class EntityCostsByMonthDiagsHelper implements TableDiagsRestorable<Object, EntityCostByMonthRecord> {
        private static final String entityCostByDayDumpFile = "entityCostByMonth_dump";

        private final DSLContext dsl;

        EntityCostsByMonthDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<EntityCostByMonthRecord> getTable() {
            return Tables.ENTITY_COST_BY_MONTH;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return entityCostByDayDumpFile;
        }
    }
}
