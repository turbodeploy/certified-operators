package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
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
import java.time.ZoneOffset;
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

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.Record;
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
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCostByDayRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostByHourRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCostRecord;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.CostGroupBy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
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

    public SqlEntityCostStore(@Nonnull final DSLContext dsl,
                              @Nonnull final Clock clock,
                              @Nonnull ExecutorService batchExecutorService,
                              final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.batchExecutorService = Objects.requireNonNull(batchExecutorService);
        this.chunkSize = chunkSize;
        this.latestEntityCostsDiagsHelper = new LatestEntityCostsDiagsHelper(dsl);
        this.entityCostsByMonthDiagsHelper = new EntityCostsByMonthDiagsHelper(dsl);
        this.entityCostsByDayDiagsHelper = new EntityCostsByDayDiagsHelper(dsl);
        this.entityCostsByHourDiagsHelper = new EntityCostsByHourDiagsHelper(dsl);
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
        final Optional<Long> planId = isPlan ? Optional.of(topologyCreationTimeOrContextId) : Optional.empty();
        final List<Runnable> bulkInsertTasks = Streams.stream(Iterables.partition(costJournals.values(), chunkSize))
                .map(chunk -> (Runnable)() -> batchInsertTask(cloudTopology, chunk, planId, createdTime))
                .collect(ImmutableList.toImmutableList());
        final CompletionService<Void> completionService = new ExecutorCompletionService(batchExecutorService);

        // submit all the tasks
        final List<Future<Void>> bulkInsertFutures = bulkInsertTasks.stream()
                .map(task -> completionService.submit(task, null))
                .collect(ImmutableList.toImmutableList());

        // wait for all tasks to complete
        for (int i = 0; i < bulkInsertTasks.size(); i++) {
            try {
                completionService.take().get();
            } catch (ExecutionException|InterruptedException e) {
                logger.error("Error during bulk entity cost insert. Canceling all remaining tasks", e);
                bulkInsertFutures.subList(i + 1, bulkInsertTasks.size()).forEach(f -> f.cancel(true));

                throw new RuntimeException(e);
            }
        }

        logger.info("Persisted {} entity cost journals in {}", costJournals::size, stopwatch::elapsed);
    }

    @Override
    public Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final CostFilter entityCostFilter) throws DbException {
        try {
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
            @Nonnull final CostFilter entityCostFilter) throws DbException {
        try {
            if (entityCostFilter.getCostGroupBy() != null) {
                // Queries based on filter and groupBy. Returns only fields used for grouping.
                return fetchStatRecordsByGroup(entityCostFilter);
            } else {
                // Queries based on filter only. Returns all fields.
                return constructStatRecordsMap(fetchRecords(entityCostFilter));
            }
        } catch (final DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB", e);
        }
    }

    @Nonnull
    private Map<Long, Collection<StatRecord>> fetchStatRecordsByGroup(
            @Nonnull final CostFilter entityCostFilter) {
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
        return createGroupByStatRecords(result, table, selectableFields);
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
    private Map<Long, Collection<StatRecord>> createGroupByStatRecords(@Nonnull final Result<Record> res,
            @Nonnull Table<?> table, @Nonnull final Set<Field<?>> selectableFields) {
        final Map<Long, Collection<StatRecord>> statRecordsByTimeStamp = Maps.newHashMap();
        res.forEach(item -> {
            Collection<StatRecord> entityCosts = statRecordsByTimeStamp.computeIfAbsent(TimeUtil
                    .localDateTimeToMilli(item.getValue(ENTITY_COST.CREATED_TIME),
                    clock), k -> Sets.newHashSet());
            entityCosts.add(mapToEntityCost(item, table, selectableFields));
        });
        return statRecordsByTimeStamp;
    }

    @Nonnull
    private StatRecord mapToEntityCost(@Nonnull final Record item, @Nonnull Table<?> table,
            @Nonnull final Set<Field<?>> selectableFields) {
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
                item.getValue("sum", Float.class));
        return statRecordBuilder.build();
    }

    private void setStatRecordValues(@Nonnull final StatRecord.Builder statRecordBuilder,
            final float avg, final float max, final float min, final float sum) {
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                .setAvg(avg)
                .setMax(max)
                .setMin(min)
                .setTotal(sum)
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
            @Nonnull final Result<? extends Record> entityCostRecords) {
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
                final Collection<StatRecord> statRecords = EntityCostToStatRecordConverter
                        .convertEntityToStatRecord(entityCost);
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
                costJournals.forEach(journal -> journal.getCategories().forEach(costType -> {
                    for (final CostSource costSource : CostSource.values()) {
                        final TraxNumber categoryCost =
                                journal.getHourlyCostBySourceAndCategory(costType,
                                        costSource);
                        if (categoryCost != null) {
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
                        }
                    }
                }));

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
            TableDiagsRestorable<Void, EntityCostRecord> {

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
    private static final class EntityCostsByDayDiagsHelper implements TableDiagsRestorable<Void, EntityCostByDayRecord> {
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
    private static final class EntityCostsByHourDiagsHelper implements TableDiagsRestorable<Void, EntityCostByHourRecord> {
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
    private static final class EntityCostsByMonthDiagsHelper implements TableDiagsRestorable<Void, EntityCostByMonthRecord> {
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
