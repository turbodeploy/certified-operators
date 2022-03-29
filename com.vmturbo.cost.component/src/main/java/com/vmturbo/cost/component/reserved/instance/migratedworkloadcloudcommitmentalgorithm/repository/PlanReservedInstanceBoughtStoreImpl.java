package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import static com.vmturbo.cost.component.db.Tables.PLAN_RESERVED_INSTANCE_BOUGHT;

import org.jooq.DSLContext;

import com.vmturbo.cost.component.db.tables.pojos.PlanReservedInstanceBought;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;

/**
 * PlanReservedInstanceBoughtStoreImpl.
 */
public class PlanReservedInstanceBoughtStoreImpl implements PlanReservedInstanceBoughtStore {
    /**
     * JOOQ DSL Context.
     */
    private final DSLContext context;

    /**
     * Creates a new PlanProjectedEntityToReservedInstanceMappingStoreImpl, passing it the Jooq DSLContext to use for its database queries.
     *
     * @param context The Jooq DSLContext to use for database queries
     */
    public PlanReservedInstanceBoughtStoreImpl(DSLContext context) {
        this.context = context;
    }

    /**
     * Saves the PlanReservedInstanceBought to the plan_reserved_instance_bought cost database table.
     *
     * @param planReservedInstanceBought The PlanReservedInstanceBought to save to the database
     * @return The resultant PlanReservedInstanceBoughtRecord
     */
    @Override
    public PlanReservedInstanceBoughtRecord save(PlanReservedInstanceBought planReservedInstanceBought) {
        PlanReservedInstanceBoughtRecord record = context.newRecord(PLAN_RESERVED_INSTANCE_BOUGHT,
                new PlanReservedInstanceBoughtRecord(
                        planReservedInstanceBought.getId(),
                        planReservedInstanceBought.getPlanId(),
                        planReservedInstanceBought.getReservedInstanceSpecId(),
                        planReservedInstanceBought.getReservedInstanceBoughtInfo(),
                        planReservedInstanceBought.getCount(),
                        planReservedInstanceBought.getPerInstanceFixedCost(),
                        planReservedInstanceBought.getPerInstanceRecurringCostHourly(),
                        planReservedInstanceBought.getPerInstanceAmortizedCostHourly()
                ));

        // Insert the record into the database
        context.batchInsert(record).execute();

        // Return the record back to the caller
        return record;
    }
}
