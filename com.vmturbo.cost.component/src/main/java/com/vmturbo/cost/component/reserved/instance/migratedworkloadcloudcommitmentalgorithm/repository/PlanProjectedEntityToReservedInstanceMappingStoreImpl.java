package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import static com.vmturbo.cost.component.db.Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import com.vmturbo.cost.component.db.tables.pojos.PlanProjectedEntityToReservedInstanceMapping;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;

/**
 * Repository implementation for interacting with the plan_projected_entity_to_reserved_instance_mapping cost database table.
 */
@Repository
public class PlanProjectedEntityToReservedInstanceMappingStoreImpl implements PlanProjectedEntityToReservedInstanceMappingStore {

    /**
     * JOOQ DSL Context.
     */
    private DSLContext context;

    /**
     * Creates a new PlanProjectedEntityToReservedInstanceMappingStoreImpl, passing it the Jooq DSLContext to use for its database queries.
     *
     * @param context The Jooq DSLContext to use for database queries
     */
    public PlanProjectedEntityToReservedInstanceMappingStoreImpl(DSLContext context) {
        this.context = context;
    }

    /**
     * Saves the PlanProjectedEntityToReservedInstanceMapping to the plan_projected_entity_to_reserved_instance_mapping
     * database table.
     *
     * @param mapping   The PlanProjectedEntityToReservedInstanceMapping to save
     * @return          The PlanProjectedEntityToReservedInstanceMappingRecord saved to the database
     */
    @Override
    public PlanProjectedEntityToReservedInstanceMappingRecord save(PlanProjectedEntityToReservedInstanceMapping mapping) {
        // Create the BuyReservedInstanceRecord
        PlanProjectedEntityToReservedInstanceMappingRecord record = context.newRecord(PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                new PlanProjectedEntityToReservedInstanceMappingRecord(
                        mapping.getEntityId(),
                        mapping.getPlanId(),
                        mapping.getReservedInstanceId(),
                        mapping.getUsedCoupons()));

        // Insert the record into the database
        context.batchInsert(record).execute();

        // Return the record back to the caller
        return record;
    }
}
