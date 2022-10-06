package com.vmturbo.cost.component.billed.cost;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.jooq.Record;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * A wrapper around cloud cost data records.
 */
class BilledCostRecordAccessor {

    /**
     * Maps record fields presented in a {@link BilledCostRecordAccessor} instance to the corresponding field
     * in {@link BilledCostItem}.
     */
    private static final List<RecordProtoMapper<BilledCostItem.Builder, ?>> RECORD_ITEM_MAPPING_LIST = ImmutableList.<RecordProtoMapper<BilledCostItem.Builder, ?>>builder()
            .add(RecordProtoMapper.of(BilledCostTableAccessor::resourceId, BilledCostItem.Builder::setEntityId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::resourceType, (builder, val) -> builder.setEntityType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::accountId, BilledCostItem.Builder::setAccountId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::regionId, BilledCostItem.Builder::setRegionId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::cloudServiceId, BilledCostItem.Builder::setCloudServiceId))
            // Only set the tag group ID if it is not zero. Zero represents null (MySQL will not allow null values
            // as part of a primary key)
            .add(RecordProtoMapper.of(BilledCostTableAccessor::tagGroupId, (builder, tagGroupId) -> {
                if (tagGroupId != 0) {
                    builder.setCostTagGroupId(tagGroupId);
                }
            }))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::providerId, BilledCostItem.Builder::setProviderId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::providerType, (builder, val) -> builder.setProviderType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::costCategory, (builder, costCategory) ->
                    builder.setCostCategory(CostCategory.forNumber(costCategory))))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::priceModel, (builder, priceModel) ->
                    builder.setPriceModel(PriceModel.forNumber(priceModel))))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::commodityType, (builder, val) -> builder.setCommodityType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::usageAmount, BilledCostItem.Builder::setUsageAmount))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::currency, (builder, currency) ->
                    builder.getCostBuilder().setCurrency(currency)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::cost, (builder, cost) ->
                    builder.getCostBuilder().setAmount(cost)))
            .build();

    private final BilledCostTableAccessor<?> tableAccessor;

    private final Record record;

    BilledCostRecordAccessor(@Nonnull BilledCostTableAccessor<?> tableAccessor,
                             @Nonnull Record record) {

        this.tableAccessor = Objects.requireNonNull(tableAccessor);
        this.record = Objects.requireNonNull(record);
    }

    @Nonnull
    public Instant sampleTs() {
        return record.get(tableAccessor.sampleTs());
    }

    @Nullable
    public Long tagGroupId() {
        return record.get(tableAccessor.tagGroupId());
    }

    @Nonnull
    public Long serviceProviderId() {
        return record.get(tableAccessor.serviceProviderId());
    }

    @Nullable
    public Long billingFamilyId() {
        return record.get(tableAccessor.billingFamilyId());
    }

    @Nonnull
    public BilledCostItem toCostItem() {

        final BilledCostItem.Builder costItem = BilledCostItem.newBuilder();

        RECORD_ITEM_MAPPING_LIST.forEach(recordProtoMapping ->
            recordProtoMapping.updateProtoBuilder(record, tableAccessor, costItem));

        return costItem.build();
    }

}
