package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import com.vmturbo.cost.component.db.tables.pojos.ActionContextRiBuy;
import com.vmturbo.cost.component.db.tables.records.ActionContextRiBuyRecord;

/**
 * Repository interface for interacting with the action_context_ri_buy cost database table.
 */
public interface PlanActionContextRiBuyStore {
    /**
     * Inserts a new action context RI buy record into the database.
     *
     * @param actionContextRiBuy The action context RI buy record to insert
     * @return The resultant ActionContextRiBuyRecord database record
     */
    ActionContextRiBuyRecord save(ActionContextRiBuy actionContextRiBuy);
}

