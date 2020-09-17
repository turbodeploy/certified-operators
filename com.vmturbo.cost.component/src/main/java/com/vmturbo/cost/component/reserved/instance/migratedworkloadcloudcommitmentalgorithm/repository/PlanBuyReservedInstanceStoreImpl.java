package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import com.vmturbo.cost.component.db.tables.pojos.BuyReservedInstance;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;

/**
 * Spring Repository implementation used to interact with the buy_reserved_instance cost database table.
 */
@Repository
public class PlanBuyReservedInstanceStoreImpl implements PlanBuyReservedInstanceStore {

    /**
     * JOOQ DSL Context.
     */
    private DSLContext context;

    /**
     * Creates a new PlanBuyReservedInstanceStoreImpl, passing it the Jooq DSLContext to use for its database queries.
     *
     * @param context The Jooq DSLContext to use for database queries
     */
    public PlanBuyReservedInstanceStoreImpl(DSLContext context) {
        this.context = context;
    }

    /**
     * Inserts a new BuyReservedInstance record into the buy_reserved_instance cost database table.
     *
     * @param buyReservedInstance The BuyReservedInstance to insert into the table
     * @return The BuyReservedInstanceRecord returned by Jooq for the inserted record
     */
    @Override
    public BuyReservedInstanceRecord save(BuyReservedInstance buyReservedInstance) {
        // Create the BuyReservedInstanceRecord
        BuyReservedInstanceRecord record = context.newRecord(BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(
                        buyReservedInstance.getId(),
                        buyReservedInstance.getTopologyContextId(),
                        buyReservedInstance.getBusinessAccountId(),
                        buyReservedInstance.getRegionId(),
                        buyReservedInstance.getReservedInstanceSpecId(),
                        buyReservedInstance.getCount(),
                        buyReservedInstance.getReservedInstanceBoughtInfo(),
                        buyReservedInstance.getPerInstanceFixedCost(),
                        buyReservedInstance.getPerInstanceAmortizedCostHourly(),
                        buyReservedInstance.getPerInstanceAmortizedCostHourly()));

        // Insert the record into the database
        context.batchInsert(record).execute();

        // Return the record back to the caller
        return record;
    }
}
