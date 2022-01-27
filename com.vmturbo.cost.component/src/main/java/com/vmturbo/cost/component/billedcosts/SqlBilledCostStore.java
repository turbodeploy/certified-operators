package com.vmturbo.cost.component.billedcosts;

import static com.vmturbo.cost.component.db.Tables.BILLED_COST_DAILY;
import static com.vmturbo.cost.component.db.Tables.BILLED_COST_MONTHLY;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record17;
import org.jooq.Select;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.db.Keys;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.db.tables.records.BilledCostDailyRecord;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupUtils;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.sql.utils.DbException;

/**
 * This object contains Sql operations for Billed Cost tables.
 */
public class SqlBilledCostStore implements BilledCostStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;
    private final BatchInserter batchInserter;
    private final TimeFrameCalculator timeFrameCalculator;

    /**
     * Creates an instance of SqlBilledCostStore.
     *
     * @param dslContext to execute sql queries.
     * @param batchInserter utility object for batch inserts.
     * @param timeFrameCalculator Time frame calculator used to identify appropriate table
     *                            (daily, monthly, etc.).
     */
    public SqlBilledCostStore(
            @Nonnull final DSLContext dslContext,
            @Nonnull final BatchInserter batchInserter,
            @Nonnull final TimeFrameCalculator timeFrameCalculator) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchInserter = Objects.requireNonNull(batchInserter);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
    }

    @Override
    public List<Future<Integer>> insertBillingDataPoints(
        @Nonnull List<Cost.UploadBilledCostRequest.BillingDataPoint> points,
        @Nonnull Map<Long, Long> discoveredTagGroupIdToOid,
        @Nonnull CostBilling.CloudBillingData.CloudBillingBucket.Granularity granularity) {
        if (granularity == CostBilling.CloudBillingData.CloudBillingBucket.Granularity.DAILY) {
            final List<Cost.UploadBilledCostRequest.BillingDataPoint> malformedPoints = new ArrayList<>();
            final AtomicLong minSampleTime = new AtomicLong(Long.MAX_VALUE);
            final List<BilledCostDailyRecord> recordSet = new ArrayList<>(points.stream()
                .map(point -> {
                    if (point.hasTimestampUtcMillis() && point.hasPriceModel() && point.hasCostCategory()
                        && point.hasCost()) {
                        final BilledCostDailyRecord record = new BilledCostDailyRecord();
                        final long sampleTime = point.getTimestampUtcMillis();
                        minSampleTime.set(Math.min(minSampleTime.get(), sampleTime));
                        record.setSampleTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(sampleTime),
                                ZoneId.from(ZoneOffset.UTC)));
                        record.setPriceModel((short)point.getPriceModel().getNumber());
                        record.setCostCategory((short)point.getCostCategory().getNumber());
                        record.setUsageAmount(point.getUsageAmount());
                        final CommonCost.CurrencyAmount currencyAmount = point.getCost();
                        record.setCost(currencyAmount.getAmount());
                        record.setCurrency((short)currencyAmount.getCurrency());
                        record.setEntityId(point.getEntityOid());
                        record.setAccountId(point.getAccountOid());
                        record.setRegionId(point.getRegionOid());
                        record.setCloudServiceId(point.getCloudServiceOid());
                        if (point.hasCostTagGroupId()) {
                            final long discoveredTagGroupId = point.getCostTagGroupId();
                            final Long tagGroupId = discoveredTagGroupIdToOid.get(discoveredTagGroupId);
                            if (tagGroupId != null) {
                                record.setTagGroupId(tagGroupId);
                            } else {
                                logger.warn("Discovered tag group set to {} but id could not be resolved. Point: {}",
                                    discoveredTagGroupId, point);
                                return null;
                            }
                        } else {
                            record.setTagGroupId(0L);
                        }
                        final short entityType = point.hasEntityType() ? (short)point.getEntityType() : (short)2047;
                        record.setEntityType(entityType);
                        // NOT NULL fields without DEFAULTS but currently not supported by BillingDataPoint
                        record.setUnit((short)0);
                        record.setServiceProviderId(0L);
                        return record;
                    } else {
                        malformedPoints.add(point);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(this::getUniqueKeys, r -> r, (a, b) -> {
                    a.setCost(a.getCost() + b.getCost());
                    return a;
                })).values());
            if (!malformedPoints.isEmpty()) {
                logger.warn("The following points are skipped due to missing fields: {}", malformedPoints);
            }
            logger.info("Inserting {} billing data points to the Billed Cost Daily table.", recordSet.size());
            return batchInserter.insertAsync(recordSet, BilledCostDaily.BILLED_COST_DAILY,
                    dslContext, true, minSampleTime.get());
        } else {
            //TODO(OM-78577) Implement insertion for billed_cost_hourly table
            return Collections.emptyList();
        }
    }

    @Override
    public List<CostStatsSnapshot> getBilledCostStats(
            @Nonnull final GetCloudBilledStatsRequest request)
            throws DbException {
        try {
            final BilledCostQueryExecutor queryExecutor = new BilledCostQueryExecutor(
                    dslContext, timeFrameCalculator);
            return queryExecutor.getBilledCostStats(request);
        } catch (DataAccessException e) {
            throw new DbException("Failed to get billed costs from DB", e);
        }
    }

    private List<Object> getUniqueKeys(final BilledCostDailyRecord record) {
        return Keys.KEY_BILLED_COST_DAILY_UNIQUE_CONSTRAINT_BILLING_ITEM.getFields().stream()
            .map(record::get)
            .collect(Collectors.toList());
    }

    /**
     * Roll up billed cost data points from source table to destination table.
     * Currently, implementation supports only daily to monthly roll-ups.
     *
     * @param rollupDurationType Rollup duration type (defines source and destination tables).
     * @param toTime Timestamp in the destination table.
     * @param fromTimeStart Start timestamp in the source table (inclusive).
     * @param fromTimeEnd End timestamp in the source table (exclusive).
     */
    @Override
    public void performRollup(
            @Nonnull final RollupDurationType rollupDurationType,
            @Nonnull final LocalDateTime toTime,
            @Nonnull final LocalDateTime fromTimeStart,
            @Nonnull final LocalDateTime fromTimeEnd) {
        logger.info("Billed costs {} rollup started for {}", rollupDurationType, toTime);

        final Select<Record17<LocalDateTime, Long, Short, Long, Long, Long, Long, Long, Short, Short, Long, Short, Short, Double, Short, Short, Double>>
                embeddedSelect = dslContext.select(
                        DSL.val(toTime).as(BILLED_COST_MONTHLY.SAMPLE_TIME),
                        BILLED_COST_DAILY.ENTITY_ID,
                        BILLED_COST_DAILY.ENTITY_TYPE,
                        BILLED_COST_DAILY.ACCOUNT_ID,
                        BILLED_COST_DAILY.REGION_ID,
                        BILLED_COST_DAILY.CLOUD_SERVICE_ID,
                        BILLED_COST_DAILY.SERVICE_PROVIDER_ID,
                        BILLED_COST_DAILY.TAG_GROUP_ID,
                        BILLED_COST_DAILY.PRICE_MODEL,
                        BILLED_COST_DAILY.COST_CATEGORY,
                        BILLED_COST_DAILY.PROVIDER_ID,
                        BILLED_COST_DAILY.PROVIDER_TYPE,
                        BILLED_COST_DAILY.COMMODITY_TYPE,
                        DSL.sum(BILLED_COST_DAILY.USAGE_AMOUNT).coerce(Double.class).as("qq"),
                        BILLED_COST_DAILY.UNIT,
                        BILLED_COST_DAILY.CURRENCY,
                        DSL.sum(BILLED_COST_DAILY.COST).coerce(Double.class).as("ww"))
                .from(BILLED_COST_DAILY)
                .where(BILLED_COST_DAILY.SAMPLE_TIME.ge(fromTimeStart))
                .and(BILLED_COST_DAILY.SAMPLE_TIME.lessThan(fromTimeEnd))
                .groupBy(BILLED_COST_DAILY.ENTITY_ID,
                        BILLED_COST_DAILY.ENTITY_TYPE,
                        BILLED_COST_DAILY.ACCOUNT_ID,
                        BILLED_COST_DAILY.REGION_ID,
                        BILLED_COST_DAILY.CLOUD_SERVICE_ID,
                        BILLED_COST_DAILY.SERVICE_PROVIDER_ID,
                        BILLED_COST_DAILY.TAG_GROUP_ID,
                        BILLED_COST_DAILY.PRICE_MODEL,
                        BILLED_COST_DAILY.COST_CATEGORY,
                        BILLED_COST_DAILY.PROVIDER_ID,
                        BILLED_COST_DAILY.PROVIDER_TYPE,
                        BILLED_COST_DAILY.COMMODITY_TYPE,
                        BILLED_COST_DAILY.UNIT,
                        BILLED_COST_DAILY.CURRENCY);
        dslContext.insertInto(BILLED_COST_MONTHLY)
                .columns(BILLED_COST_MONTHLY.SAMPLE_TIME,
                        BILLED_COST_MONTHLY.ENTITY_ID,
                        BILLED_COST_MONTHLY.ENTITY_TYPE,
                        BILLED_COST_MONTHLY.ACCOUNT_ID,
                        BILLED_COST_MONTHLY.REGION_ID,
                        BILLED_COST_MONTHLY.CLOUD_SERVICE_ID,
                        BILLED_COST_MONTHLY.SERVICE_PROVIDER_ID,
                        BILLED_COST_MONTHLY.TAG_GROUP_ID,
                        BILLED_COST_MONTHLY.PRICE_MODEL,
                        BILLED_COST_MONTHLY.COST_CATEGORY,
                        BILLED_COST_MONTHLY.PROVIDER_ID,
                        BILLED_COST_MONTHLY.PROVIDER_TYPE,
                        BILLED_COST_MONTHLY.COMMODITY_TYPE,
                        BILLED_COST_MONTHLY.USAGE_AMOUNT,
                        BILLED_COST_MONTHLY.UNIT,
                        BILLED_COST_MONTHLY.CURRENCY,
                        BILLED_COST_MONTHLY.COST)
                .select(embeddedSelect)
                .onConflict(Keys.KEY_BILLED_COST_MONTHLY_UNIQUE_CONSTRAINT_BILLING_ITEM.getFields())
                .doUpdate()
                .set(BILLED_COST_MONTHLY.USAGE_AMOUNT, RollupUtils.values(BILLED_COST_MONTHLY.USAGE_AMOUNT))
                .set(BILLED_COST_MONTHLY.COST, RollupUtils.values(BILLED_COST_MONTHLY.COST))
                .execute();

        logger.info("Billed costs {} rollup completed for {}", rollupDurationType, toTime);
    }
}
