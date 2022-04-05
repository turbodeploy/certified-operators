package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record10;

import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * DB implementation of BillingRecordStore.
 */
public class SqlBillingRecordStore implements BillingRecordStore {
    /**
     * DSL for DB query.
     */
    private final DSLContext dsl;

    /**
     * Creates a new instance of DB store.
     *
     * @param dslContext DSL for DB query.
     */
    public SqlBillingRecordStore(@Nonnull final DSLContext dslContext) {
        dsl = dslContext;
    }

    @Override
    public Stream<BillingChangeRecord> getBillingChangeRecords(long lastUpdatedStartTime,
            long lastUpdatedEndTime, @Nonnull final List<Long> entityIds) {
        final BilledCostDaily t1 = BilledCostDaily.BILLED_COST_DAILY.as("t1");
        final BilledCostDaily t2 = BilledCostDaily.BILLED_COST_DAILY.as("t2");

        // This inner query is getting the sample times (for the entities in question), that match
        // the last updated times. We need to get ALL the records for any records that match the
        // last updated time, so that we can re-process those days.
        final Condition condition = t1.ENTITY_ID.in(entityIds)
                .and(t1.SAMPLE_TIME.in(dsl.select(t2.SAMPLE_TIME)
                                .from(t2)
                        .where(t1.ENTITY_ID.eq(t2.ENTITY_ID)
                                .and(t2.LAST_UPDATED.greaterOrEqual(lastUpdatedStartTime))
                                .and(t2.LAST_UPDATED.lessThan(lastUpdatedEndTime)))
                ));
        return dsl.select(t1.SAMPLE_TIME,
                        t1.ENTITY_ID,
                        t1.ENTITY_TYPE,
                        t1.PRICE_MODEL,
                        t1.COST_CATEGORY,
                        t1.PROVIDER_ID,
                        t1.PROVIDER_TYPE,
                        t1.USAGE_AMOUNT,
                        t1.COST,
                        t1.LAST_UPDATED)
                .from(t1)
                .where(condition)
                .orderBy(t1.SAMPLE_TIME.asc(), t1.ENTITY_ID)
                .fetch()
                .stream()
                .map(this::createChangeRecord);
    }

    /**
     * Creates a new billing change record.
     *
     * @param dbRecord Record read from DB.
     * @return BillingChangeRecord instance.
     */
    @Nonnull
    private BillingChangeRecord createChangeRecord(
            Record10<LocalDateTime, Long, Short, Short, Short, Long, Short, Double, Double, Long> dbRecord) {
        return new BillingChangeRecord.Builder()
                .sampleTime(dbRecord.get(0, LocalDateTime.class))
                .entityId(dbRecord.get(1, Long.class))
                .entityType(dbRecord.get(2, Short.class))
                .priceModel(PriceModel.forNumber(dbRecord.get(3, Short.class)))
                .costCategory(CostCategory.forNumber(dbRecord.get(4, Short.class)))
                .providerId(dbRecord.get(5, Long.class))
                .providerType(dbRecord.get(6, Short.class))
                .usageAmount(dbRecord.get(7, Double.class))
                .cost(dbRecord.get(8, Double.class))
                .lastUpdated(dbRecord.get(9, Long.class)).build();
    }
}
