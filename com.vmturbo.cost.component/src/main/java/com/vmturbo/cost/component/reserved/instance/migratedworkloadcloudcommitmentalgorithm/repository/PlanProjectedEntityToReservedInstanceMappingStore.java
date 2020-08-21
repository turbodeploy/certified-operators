package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import com.vmturbo.cost.component.db.tables.pojos.PlanProjectedEntityToReservedInstanceMapping;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;

/**
 * Repository interface for interacting with the plan_projected_entity_to_reserved_instance_mapping cost database table.
 */
public interface PlanProjectedEntityToReservedInstanceMappingStore {
    /**
     * Saves the PlanProjectedEntityToReservedInstanceMapping to the plan_projected_entity_to_reserved_instance_mapping
     * database table.
     *
     * @param mapping   The PlanProjectedEntityToReservedInstanceMapping to save
     * @return          The PlanProjectedEntityToReservedInstanceMappingRecord saved to the database
     */
    PlanProjectedEntityToReservedInstanceMappingRecord save(PlanProjectedEntityToReservedInstanceMapping mapping);
}
