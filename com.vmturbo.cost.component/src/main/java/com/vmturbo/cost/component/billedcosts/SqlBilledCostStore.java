package com.vmturbo.cost.component.billedcosts;

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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.db.Keys;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.db.tables.records.BilledCostDailyRecord;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.platform.sdk.common.CostBilling;

/**
 * This object contains Sql operations for Billed Cost tables.
 */
public class SqlBilledCostStore implements BilledCostStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;
    private final BatchInserter batchInserter;

    /**
     * Creates an instance of SqlBilledCostStore.
     *
     * @param dslContext to execute sql queries.
     * @param batchInserter utility object for batch inserts.
     */
    public SqlBilledCostStore(@Nonnull final DSLContext dslContext, @Nonnull BatchInserter batchInserter) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchInserter = Objects.requireNonNull(batchInserter);
    }

    @Override
    public List<Future<Integer>> insertBillingDataPoints(
        @Nonnull List<Cost.UploadBilledCostRequest.BillingDataPoint> points,
        @Nonnull Map<Long, Long> discoveredTagGroupIdToOid,
        @Nonnull CostBilling.CloudBillingData.CloudBillingBucket.Granularity granularity) {
        if (granularity == CostBilling.CloudBillingData.CloudBillingBucket.Granularity.DAILY) {
            final List<Cost.UploadBilledCostRequest.BillingDataPoint> malformedPoints = new ArrayList<>();
            final List<BilledCostDailyRecord> recordSet = new ArrayList<>(points.stream()
                .map(point -> {
                    if (point.hasTimestampUtcMillis() && point.hasPriceModel() && point.hasCostCategory()
                        && point.hasCost()) {
                        final BilledCostDailyRecord record = new BilledCostDailyRecord();
                        record.setSampleTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(point.getTimestampUtcMillis()),
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
            return batchInserter
                .insertAsync(recordSet, BilledCostDaily.BILLED_COST_DAILY, dslContext, true);
        } else {
            //TODO(OM-78577) Implement insertion for billed_cost_hourly table
            return Collections.emptyList();
        }
    }

    private List<Object> getUniqueKeys(final BilledCostDailyRecord record) {
        return Keys.KEY_BILLED_COST_DAILY_UNIQUE_CONSTRAINT_BILLING_ITEM.getFields().stream()
            .map(record::get)
            .collect(Collectors.toList());
    }
}