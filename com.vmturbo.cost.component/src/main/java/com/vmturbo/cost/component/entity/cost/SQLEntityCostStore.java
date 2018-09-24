package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.db.tables.records.EntityCostRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

/**
 * {@inheritDoc}
 */
public class SQLEntityCostStore implements EntityCostStore {

    private final DSLContext dsl;

    private final Clock clock;

    private final int chunkSize;

    public SQLEntityCostStore(@Nonnull final DSLContext dsl,
                              @Nonnull final Clock clock,
                              final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void persistEntityCosts(@Nonnull final List<EntityCost> entityCosts)
            throws DbException, InvalidEntityCostsException {
        final LocalDateTime curTime = LocalDateTime.now(clock);
        Objects.requireNonNull(entityCosts);
        if (!isValidEntityCosts(entityCosts)) {
            throw new InvalidEntityCostsException("All entity cost must have associated entity id," +
                    " spend type, component cost list, total amount ");
        }

        try {
            // We chunk the transactions for speed, and to avoid overloading the DB buffers
            // on large topologies. Ideally this should be one transaction.
            //
            // TODO (roman, Sept 6 2018): Try to handle transaction failure (e.g. by deleting all
            // committed data).
            Lists.partition(entityCosts, chunkSize).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final DSLContext transactionContext = DSL.using(transaction);
                    // Initialize the batch.
                    final BatchBindStep batch = transactionContext.batch(
                            //have to provide dummy values for jooq
                            transactionContext.insertInto(ENTITY_COST)
                                    .set(ENTITY_COST.ASSOCIATED_ENTITY_ID, 0L)
                                    .set(ENTITY_COST.CREATED_TIME, curTime)
                                    .set(ENTITY_COST.ASSOCIATED_ENTITY_TYPE, 0)
                                    .set(ENTITY_COST.COST_TYPE, 0)
                                    .set(ENTITY_COST.CURRENCY, 0)
                                    .set(ENTITY_COST.AMOUNT, BigDecimal.valueOf(0)));

                    // Bind values to the batch insert statement. Each "bind" should have values for
                    // all fields set during batch initialization.
                    chunk.forEach(entityCost -> entityCost.getComponentCostList()
                        .forEach(componentCost ->
                            batch.bind(entityCost.getAssociatedEntityId(),
                            curTime,
                            entityCost.getAssociatedEntityType(),
                            componentCost.getCategory().getNumber(),
                            entityCost.getTotalAmount().getCurrency(),
                            BigDecimal.valueOf(componentCost.getAmount().getAmount()))));

                    // Actually execute the batch insert.
                    batch.execute();
                });
            });
        } catch (DataAccessException e) {
            throw new DbException("Failed to persist entity costs to DB" + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void persistEntityCost(@Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> costJournals) throws DbException {
        final LocalDateTime curTime = LocalDateTime.now(clock);
        Objects.requireNonNull(costJournals);
        try {
            // We chunk the transactions for speed, and to avoid overloading the DB buffers
            // on large topologies. Ideally this should be one transaction.
            //
            // TODO (roman, Sept 6 2018): Try to handle transaction failure (e.g. by deleting all
            // committed data).
            Iterators.partition(costJournals.values().iterator(), chunkSize)
                .forEachRemaining(chunk -> dsl.transaction(transaction -> {
                    final DSLContext transactionContext = DSL.using(transaction);
                    // Initialize the batch.
                    final BatchBindStep batch = transactionContext.batch(
                            //have to provide dummy values for jooq
                            dsl.insertInto(ENTITY_COST)
                                    .set(ENTITY_COST.ASSOCIATED_ENTITY_ID, 0L)
                                    .set(ENTITY_COST.CREATED_TIME, curTime)
                                    .set(ENTITY_COST.ASSOCIATED_ENTITY_TYPE, 0)
                                    .set(ENTITY_COST.COST_TYPE, 0)
                                    .set(ENTITY_COST.CURRENCY, 0)
                                    .set(ENTITY_COST.AMOUNT, BigDecimal.valueOf(0)));

                    // Bind values to the batch insert statement. Each "bind" should have values for
                    // all fields set during batch initialization.
                    chunk.forEach(journal -> journal.getCategories().forEach(costType -> {
                        final double categoryCost = journal.getHourlyCostForCategory(costType);
                        batch.bind(journal.getEntity().getOid(),
                                curTime,
                                journal.getEntity().getEntityType(),
                                costType.getNumber(),
                                // TODO (roman, Sept 5 2018): Not handling currency in cost
                                // calculation yet.
                                CurrencyAmount.getDefaultInstance().getCurrency(),
                                BigDecimal.valueOf(categoryCost));
                    }));

                    // Actually execute the batch insert.
                    batch.execute();
                }));

        } catch (DataAccessException e) {
            throw new DbException("Failed to persist entity costs to DB" + e.getMessage());
        }
    }

    // ensure all the entity cost object has id, spend type, components and their amount/rate
    private boolean isValidEntityCosts(final List<EntityCost> entityCosts) {
        return entityCosts.stream().allMatch(entityCost ->
                entityCost.hasAssociatedEntityId()
                        && entityCost.hasAssociatedEntityType()
                        && !entityCost.getComponentCostList().isEmpty()
                        && entityCost.getComponentCostList().stream().allMatch(ComponentCost::hasAmount));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final LocalDateTime startDate,
                                                           @Nonnull final LocalDateTime endDate) throws DbException {
        try {
            return constructEntityCostMap(dsl
                    .selectFrom(ENTITY_COST)
                    .where(ENTITY_COST.CREATED_TIME.between(endDate, startDate))
                    .fetch());
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB" + e.getMessage());
        }
    }

    /**
     * Construct entity cost map. Key is timestamp in long, Values are List of EntityCost DTOs.
     * It will first group the records by timestamp, and combine the entity costs with same id.
     *
     * @param entityCostRecords entity cost records in db
     * @return Entity cost map, key is timestamp in long, values are List of EntityCost DTOs.
     */
    private Map<Long, Map<Long, EntityCost>> constructEntityCostMap(final Result<EntityCostRecord> entityCostRecords) {
        final Map<Long, Map<Long, EntityCost>> records = new HashMap<>();
        entityCostRecords.forEach(entityRecord -> {
            Map<Long, EntityCost> costsForTimestamp = records.computeIfAbsent(TimeUtil.localDateTimeToMilli(entityRecord.getCreatedTime()), k -> new HashMap<>());
            //TODO: optimize to avoid building EntityCost
            final EntityCost newCost = toEntityCostDTO(entityRecord);
            costsForTimestamp.compute(newCost.getAssociatedEntityId(),
                    (id, existingCost) -> existingCost == null ?
                            newCost :
                            existingCost.toBuilder()
                                    .addAllComponentCost(newCost.getComponentCostList())
                                    .build());
        });
        return records;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final Set<Long> entityIds,
                                                           @Nonnull final LocalDateTime startDate,
                                                           @Nonnull final LocalDateTime endDate) throws DbException {
        try {
            return constructEntityCostMap(dsl
                    .selectFrom(ENTITY_COST)
                    .where(ENTITY_COST.CREATED_TIME.between(startDate, endDate))
                    .and(ENTITY_COST.ASSOCIATED_ENTITY_ID.in(entityIds))
                    .fetch());
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB" + e.getMessage());
        }
    }

    /**
     * Clean up entity costs based on start date. All the entity costs before {@param startDate} will be cleaned.
     *
     * @param startDate start date
     * @throws DbException if anything goes wrong in the database
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

    //Convert EntityCostRecord DB record to entity cost proto DTO
    private Cost.EntityCost toEntityCostDTO(@Nonnull final EntityCostRecord entityCostRecord) {
        ComponentCost componentCost = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(entityCostRecord.getAmount().doubleValue())
                        .setCurrency(entityCostRecord.getCurrency()))
                .setCategory(CostCategory.forNumber(entityCostRecord.getCostType()))
                .build();
        return Cost.EntityCost.newBuilder()
                .setAssociatedEntityId(entityCostRecord.getAssociatedEntityId())
                .addComponentCost(componentCost)
                .setAssociatedEntityType(entityCostRecord.getAssociatedEntityType())
                .build();
    }
}
