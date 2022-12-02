package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.BILLED_COST_DAILY;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record14;

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
    public Stream<BillingRecord> getUpdatedBillRecords(long lastUpdatedStartTime,
            LocalDateTime endTime, @Nonnull final Set<Long> entityIds, final int savingsDaysToSkip) {
        final BilledCostDaily t1 = BilledCostDaily.BILLED_COST_DAILY.as("t1");
        final BilledCostDaily t2 = BilledCostDaily.BILLED_COST_DAILY.as("t2");

        // Select records that were updated since the time savings was processed AND
        // the date of the bill record (sample_time) must be before the endTime.
        Condition timeCondition = t2.LAST_UPDATED.greaterThan(lastUpdatedStartTime)
                .and(t2.SAMPLE_TIME.lessThan(endTime));
        // If we are skipping at least one day of records, the day before the endTime must be
        // processed because records may have a last_updated timestamp that is older than the
        // lastUpdatedStartTime.
        if (savingsDaysToSkip >= 1) {
            timeCondition = timeCondition.or(t2.SAMPLE_TIME.greaterOrEqual(endTime.minusDays(1))
                    .and(t2.SAMPLE_TIME.lessThan(endTime)));
        }

        // This inner query is getting the sample times (for the entities in question), that match
        // the last updated times. We need to get ALL the records for any records that match the
        // last updated time, so that we can re-process those days.
        final Condition condition = t1.ENTITY_ID.in(entityIds)
                .and(t1.SAMPLE_TIME.in(dsl.select(t2.SAMPLE_TIME)
                                .from(t2)
                        .where(t1.ENTITY_ID.eq(t2.ENTITY_ID).and(timeCondition))
                ));
        return dsl.select(t1.SAMPLE_TIME,
                        t1.ENTITY_ID,
                        t1.ENTITY_TYPE,
                        t1.ACCOUNT_ID,
                        t1.REGION_ID,
                        t1.SERVICE_PROVIDER_ID,
                        t1.PRICE_MODEL,
                        t1.COST_CATEGORY,
                        t1.PROVIDER_ID,
                        t1.PROVIDER_TYPE,
                        t1.COMMODITY_TYPE,
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

    @Override
    public Stream<BillingRecord> getBillRecords(LocalDateTime startTime, LocalDateTime endTime,
            @Nonnull Set<Long> entityIds) {
        final Condition condition = BILLED_COST_DAILY.ENTITY_ID.in(entityIds)
                .and(BILLED_COST_DAILY.SAMPLE_TIME.between(startTime, endTime));
        return dsl.select(BILLED_COST_DAILY.SAMPLE_TIME,
                BILLED_COST_DAILY.ENTITY_ID,
                BILLED_COST_DAILY.ENTITY_TYPE,
                BILLED_COST_DAILY.ACCOUNT_ID,
                BILLED_COST_DAILY.REGION_ID,
                BILLED_COST_DAILY.SERVICE_PROVIDER_ID,
                BILLED_COST_DAILY.PRICE_MODEL,
                BILLED_COST_DAILY.COST_CATEGORY,
                BILLED_COST_DAILY.PROVIDER_ID,
                BILLED_COST_DAILY.PROVIDER_TYPE,
                BILLED_COST_DAILY.COMMODITY_TYPE,
                BILLED_COST_DAILY.USAGE_AMOUNT,
                BILLED_COST_DAILY.COST,
                BILLED_COST_DAILY.LAST_UPDATED)
                .from(BILLED_COST_DAILY)
                .where(condition)
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
    private BillingRecord createChangeRecord(
            Record14<LocalDateTime, Long, Short, Long, Long, Long, Short, Short, Long, Short, Short, Double, Double, Long> dbRecord) {
        return new BillingRecord.Builder()
                .sampleTime(dbRecord.get(0, LocalDateTime.class))
                .entityId(dbRecord.get(1, Long.class))
                .entityType(dbRecord.get(2, Short.class))
                .accountId(dbRecord.get(3, Long.class))
                .regionId(dbRecord.get(4, Long.class))
                .serviceProviderId(dbRecord.get(5, Long.class))
                .priceModel(PriceModel.forNumber(dbRecord.get(6, Short.class)))
                .costCategory(CostCategory.forNumber(dbRecord.get(7, Short.class)))
                .providerId(dbRecord.get(8, Long.class))
                .providerType(dbRecord.get(9, Short.class))
                .commodityType(dbRecord.get(10, Short.class))
                .usageAmount(dbRecord.get(11, Double.class))
                .cost(dbRecord.get(12, Double.class))
                .lastUpdated(dbRecord.get(13, Long.class)).build();
    }
}
