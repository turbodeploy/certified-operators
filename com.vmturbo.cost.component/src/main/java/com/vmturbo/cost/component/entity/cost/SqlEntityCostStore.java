package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record7;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.db.tables.records.EntityCostRecord;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.CostGroupBy;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.trax.TraxNumber;

/**
 * {@inheritDoc}
 */
public class SqlEntityCostStore implements EntityCostStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Clock clock;

    private final int chunkSize;

    public SqlEntityCostStore(@Nonnull final DSLContext dsl,
                              @Nonnull final Clock clock,
                              final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
    }

    @Override
    public void persistEntityCost(
            @Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> costJournals,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
            final long topologyCreationTime) throws DbException {
        final LocalDateTime createdTime =
                Instant.ofEpochMilli(topologyCreationTime).atZone(ZoneOffset.UTC).toLocalDateTime();
        try {
            logger.info("Persisting {} entity cost journals", costJournals::size);

            // We chunk the transactions for speed, and to avoid overloading the DB buffers
            // on large topologies. Ideally this should be one transaction.
            // TODO (roman, Sept 6 2018): Try to handle transaction failure (e.g. by deleting all
            //  committed data).
            Iterators.partition(costJournals.values().iterator(), chunkSize)
                    .forEachRemaining(chunk -> dsl.transaction(transaction -> {
                        final DSLContext transactionContext = DSL.using(transaction);
                        // Initialize the batch.
                        final BatchBindStep batch = transactionContext.batch(
                                //have to provide dummy values for jooq
                                dsl.insertInto(ENTITY_COST)
                                        .set(ENTITY_COST.ASSOCIATED_ENTITY_ID, 0L)
                                        .set(ENTITY_COST.CREATED_TIME, createdTime)
                                        .set(ENTITY_COST.ASSOCIATED_ENTITY_TYPE, 0)
                                        .set(ENTITY_COST.COST_TYPE, 0)
                                        .set(ENTITY_COST.COST_SOURCE, 0)
                                        .set(ENTITY_COST.CURRENCY, 0)
                                        .set(ENTITY_COST.AMOUNT, BigDecimal.valueOf(0))
                                        .set(ENTITY_COST.ACCOUNT_ID, 0L)
                                        .set(ENTITY_COST.AVAILABILITY_ZONE_ID, 0L)
                                        .set(ENTITY_COST.REGION_ID, 0L));

                        // Bind values to the batch insert statement. Each "bind" should have values for
                        // all fields set during batch initialization.
                        chunk.forEach(journal -> journal.getCategories().forEach(costType -> {
                                    for (final CostSource costSource : CostSource.values()) {
                                final TraxNumber categoryCost =
                                        journal.getHourlyCostBySourceAndCategory(costType,
                                                costSource);
                                if (categoryCost != null) {
                                    final long entityOid = journal.getEntity().getOid();
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
                        }));

                        if (batch.size() > 0) {
                            logger.info("Persisting batch of size: {}", batch::size);
                            batch.execute();
                        }
                    }));
        } catch (final DataAccessException e) {
            throw new DbException("Failed to persist entity costs to DB", e);
        }
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
            if (table.equals(ENTITY_COST)) {
                final Field<Integer> costSource = getField(table, ENTITY_COST.COST_SOURCE);
                modifiableList.add(costSource);
            }

            SelectConditionStep<Record> selectCondition = dsl
                    .select(modifiableList)
                    .from(table)
                    .where(entityCostFilter.getConditions());

            // If latest timestamp is requested only return the info related to latest timestamp
            if (entityCostFilter.isLatestTimeStampRequested()) {
                selectCondition =
                        selectCondition.and(ENTITY_COST.CREATED_TIME.eq(
                                dsl.select(DSL.max(ENTITY_COST.CREATED_TIME))
                                        .from(table)
                                        .where(entityCostFilter.getConditions())));
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, Collection<StatRecord>> getEntityCostStats(@Nonnull final CostFilter entityCostFilter)
            throws DbException {
        try {
            if (entityCostFilter.getCostGroupBy() != null) {
                //queries based on groupBy + filter. Returns only fields used for grouping.
                return fetchStatRecordsByGroup(entityCostFilter);
            } else {
                //queries based on filter only. Returns all fields.
                return constructStatRecordsMap(fetchRecords(entityCostFilter));
            }
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB", e);
        }
    }

    @Nonnull
    private Map<Long, Collection<StatRecord>> fetchStatRecordsByGroup(@Nonnull final CostFilter entityCostFilter) {
        final CostGroupBy costGroupBy = entityCostFilter.getCostGroupBy();
        final Set<Field<?>> groupByFields = costGroupBy.getGroupByFields();
        final Table<?> table = costGroupBy.getTable();
        final Field<LocalDateTime> createdTimeField = getField(table, ENTITY_COST.CREATED_TIME);
        final Field<Integer> entityType = getField(table, ENTITY_COST.ASSOCIATED_ENTITY_TYPE);
        final Field<Integer> costType = getField(table, ENTITY_COST.COST_TYPE);
        final Set<Field<?>> selectableFields = Sets.newHashSet(groupByFields);
        selectableFields.add(createdTimeField);
        selectableFields.add(entityType);
        selectableFields.add(costType);
        selectableFields.add(sum(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(max(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(min(costGroupBy.getAmountFieldInTable()));
        selectableFields.add(avg(costGroupBy.getAmountFieldInTable()));
        final Result<Record> res = dsl
                .select(selectableFields)
                .from(table)
                .where(Arrays.asList(entityCostFilter.getConditions()))
                .and(getConditionForEntityCost(dsl, entityCostFilter, createdTimeField, table))
                .groupBy(groupByFields)
                .fetch();
        return createGroupByStatRecords(res, selectableFields);
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
        if (table.equals(ENTITY_COST)) {
            final Field<Integer> costSource = getField(table, ENTITY_COST.COST_SOURCE);
            modifiableList.add(costSource);
        }
        return dsl
                .select(modifiableList)
                .from(table)
                .where(Arrays.asList(entityCostFilter.getConditions()))
                .and(getConditionForEntityCost(dsl, entityCostFilter, createdTime, table))
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
                    (id, existingCost) -> existingCost == null ?
                            newCost :
                            existingCost.toBuilder()
                                    .addAllComponentCost(newCost.getComponentCostList())
                                    .build());
        });
        return records;
    }

    @Nonnull
    private Map<Long, Collection<StatRecord>> createGroupByStatRecords(@Nonnull final Result<Record> res,
                                                                       @Nonnull final Set<Field<?>> selectableFields) {
        final Map<Long, Collection<StatRecord>> statRecordsByTimeStamp = Maps.newHashMap();
        res.forEach(item -> {
            Collection<StatRecord> entityCosts = statRecordsByTimeStamp.computeIfAbsent(TimeUtil
                    .localDateTimeToMilli(item.getValue(ENTITY_COST.CREATED_TIME),
                    clock), k -> Sets.newHashSet());
            entityCosts.add(mapToEntityCost(item, selectableFields));
        });
        return statRecordsByTimeStamp;
    }

    @Nonnull
    private StatRecord mapToEntityCost(@Nonnull final Record item,
                                        @Nonnull final Set<Field<?>> selectableFields) {
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder();
        statRecordBuilder.setCategory(CostCategory.forNumber(item.get(ENTITY_COST.COST_TYPE)));
        statRecordBuilder.setAssociatedEntityType(item.getValue(ENTITY_COST.ASSOCIATED_ENTITY_TYPE));
        if (selectableFields.contains(ENTITY_COST.ASSOCIATED_ENTITY_ID)) {
            statRecordBuilder.setAssociatedEntityId(item.getValue(ENTITY_COST.ASSOCIATED_ENTITY_ID));
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
                    (id, existingCost) -> existingCost == null ?
                            newCost :
                            existingCost.toBuilder()
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
     * @param createdTime FieldName for created_Time.
     * @param table table to be used in condition.
     * @return {@link Condition}. This is used in the query.
     */
    @Nonnull
    private Condition getConditionForEntityCost(
            @Nonnull final DSLContext dsl,
            @Nonnull final CostFilter entityCostFilter,
            @Nonnull final Field<LocalDateTime> createdTime,
            @Nonnull final Table<?> table) {
        if (entityCostFilter.isLatest()) {
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
}
