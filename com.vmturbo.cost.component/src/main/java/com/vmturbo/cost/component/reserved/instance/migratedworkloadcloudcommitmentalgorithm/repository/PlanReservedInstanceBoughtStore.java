package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import com.vmturbo.cost.component.db.tables.pojos.PlanReservedInstanceBought;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;

/**
 * Repository interface for interacting with the plan_reserved_instance_bought cost database table.
 */
public interface PlanReservedInstanceBoughtStore {
    /**
     * Saves the PlanReservedInstanceBought to the plan_reserved_instance_bought cost database table.
     *
     * @param planReservedInstanceBought The PlanReservedInstanceBought to save to the database
     * @return The resultant PlanReservedInstanceBoughtRecord
     */
    PlanReservedInstanceBoughtRecord save(PlanReservedInstanceBought planReservedInstanceBought);
}
