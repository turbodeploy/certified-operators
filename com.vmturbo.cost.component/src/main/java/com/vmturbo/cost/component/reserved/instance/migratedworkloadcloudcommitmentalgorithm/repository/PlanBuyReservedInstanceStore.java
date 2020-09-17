package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import com.vmturbo.cost.component.db.tables.pojos.BuyReservedInstance;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;

/**
 * Repository interface for interacting with the buy_reserved_instance cost database table.
 */
public interface PlanBuyReservedInstanceStore {
    /**
     * Inserts a new BuyReservedInstance record into the buy_reserved_instance cost database table.
     *
     * @param buyReservedInstance The BuyReservedInstance to insert into the table
     * @return The BuyReservedInstanceRecord returned by Jooq for the inserted record
     */
    BuyReservedInstanceRecord save(BuyReservedInstance buyReservedInstance);
}
