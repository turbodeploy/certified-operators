package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
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

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostType;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.cost.component.db.tables.records.EntityCostRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

/**
 * {@inheritDoc}
 */
public class SQLEntityCostStore implements EntityCostStore {

    private final DSLContext dsl;

    public SQLEntityCostStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void persistEntityCosts(@Nonnull final List<EntityCost> entityCosts)
            throws DbException, InvalidEntityCostsException {
        final LocalDateTime curTime = LocalDateTime.now();
        Objects.requireNonNull(entityCosts);
        if (isValidEntityCosts(entityCosts)) {
            try {
                BatchBindStep batch = dsl.batch(
                        //have to provide dummy values for jooq
                        dsl.insertInto(ENTITY_COST,
                                ENTITY_COST.ASSOCIATED_ENTITY_ID,
                                ENTITY_COST.CREATED_TIME,
                                ENTITY_COST.ASSOCIATED_ENTITY_TYPE,
                                ENTITY_COST.COST_TYPE,
                                ENTITY_COST.CURRENCY,
                                ENTITY_COST.AMOUNT)
                                .values(entityCosts.get(0).getAssociatedEntityId(),
                                        curTime,
                                        entityCosts.get(0).getAssociatedEntityType(),
                                        entityCosts.get(0).getComponentCostList().get(0).getCostType().getNumber(),
                                        entityCosts.get(0).getTotalAmount().getCurrency(),
                                        BigDecimal.valueOf(entityCosts.get(0).getComponentCostList().get(0).getAmount().getAmount())));

                for (EntityCost entityCost : entityCosts) {
                    entityCost.getComponentCostList().forEach(componentCost -> batch.bind(entityCost.getAssociatedEntityId(),
                            curTime,
                            entityCost.getAssociatedEntityType(),
                            componentCost.getCostType().getNumber(),
                            entityCost.getTotalAmount().getCurrency(),
                            BigDecimal.valueOf(componentCost.getAmount().getAmount())));
                }
                //TODO: implement batch size, see OM-38675.
                batch.execute();

            } catch (DataAccessException e) {
                throw new DbException("Failed to persist entity costs to DB" + e.getMessage());

            }
        } else {
            throw new InvalidEntityCostsException("All entity cost must have associated entity id," +
                    " spend type, component cost list, total amount ");
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
            Map<Long, EntityCost> costsForTimestamp = records.computeIfAbsent(localDateTimeToDate(entityRecord.getCreatedTime()), k -> new HashMap<>());
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
                .setCostType(CostType.forNumber(entityCostRecord.getCostType()))
                .build();
        return Cost.EntityCost.newBuilder()
                .setAssociatedEntityId(entityCostRecord.getAssociatedEntityId())
                .addComponentCost(componentCost)
                .setAssociatedEntityType(entityCostRecord.getAssociatedEntityType())
                .build();
    }

    /**
     * Convert local date time to long.
     *
     * @param startOfDay start of date with LocalDateTime type.
     * @return date time in long type.
     */
    private long localDateTimeToDate(LocalDateTime startOfDay) {
        return Date.from(startOfDay.atZone(ZoneId.systemDefault()).toInstant()).getTime();
    }
}
