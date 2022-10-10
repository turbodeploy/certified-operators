package com.vmturbo.cost.component.billed.cost;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.jooq.Record;

import com.vmturbo.common.protobuf.cloud.CloudCommon.StatValues;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;

/**
 * A wrapper around cloud cost stat records.
 */
class BilledCostStatRecordAccessor {

    protected static final String SAMPLE_COUNT_FIELD = "sample_count";

    protected static final String MIN_USAGE_AMOUNT_FIELD = "min_usage_amount";
    protected static final String MAX_USAGE_AMOUNT_FIELD = "max_usage_amount";
    protected static final String SUM_USAGE_AMOUNT_FIELD = "sum_usage_amount";
    protected static final String AVG_USAGE_AMOUNT_FIELD = "avg_usage_amount";

    protected static final String MIN_COST_FIELD = "min_cost_capacity";
    protected static final String MAX_COST_FIELD = "max_cost_capacity";
    protected static final String SUM_COST_FIELD = "sum_cost_capacity";
    protected static final String AVG_COST_FIELD = "avg_cost_capacity";

    private static final List<RecordProtoMapper<BilledCostStat.Builder, ?>> RECORD_STAT_MAPPING_LIST = ImmutableList.<RecordProtoMapper<BilledCostStat.Builder, ?>>builder()
            .add(RecordProtoMapper.of(BilledCostTableAccessor::sampleTs, (builder, sampleTs) -> builder.setSampleTsUtc(sampleTs.toEpochMilli())))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::resourceId, BilledCostStat.Builder::setEntityId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::resourceType, (builder, val) -> builder.setEntityType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::accountId, BilledCostStat.Builder::setAccountId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::regionId, BilledCostStat.Builder::setRegionId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::cloudServiceId, BilledCostStat.Builder::setCloudServiceId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::serviceProviderId, BilledCostStat.Builder::setServiceProviderId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::providerId, BilledCostStat.Builder::setProviderId))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::providerType, (builder, val) -> builder.setProviderType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::commodityType, (builder, val) -> builder.setCommodityType((int)val)))
            .add(RecordProtoMapper.of(BilledCostTableAccessor::currency, (builder, val) -> builder.setCurrency((int)val)))
            .build();

    private final BilledCostTableAccessor<?> tableAccessor;

    private final Record record;

    BilledCostStatRecordAccessor(@Nonnull BilledCostTableAccessor<?> tableAccessor,
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

    @Nonnull BilledCostStat.Builder toCostStatBuilder() {

        final BilledCostStat.Builder costStatBuilder =  BilledCostStat.newBuilder()
                .setGranularity(tableAccessor.granularity())
                .setUsageAmountStats(StatValues.newBuilder()
                        .setSampleCount(record.get(SAMPLE_COUNT_FIELD, Long.class))
                        .setMax(record.get(MAX_USAGE_AMOUNT_FIELD, Double.class))
                        .setMin(record.get(MIN_USAGE_AMOUNT_FIELD, Double.class))
                        .setAvg(record.get(AVG_USAGE_AMOUNT_FIELD, Double.class))
                        .setSum(record.get(SUM_USAGE_AMOUNT_FIELD, Double.class)))
                .setCostStats(StatValues.newBuilder()
                        .setSampleCount(record.get(SAMPLE_COUNT_FIELD, Long.class))
                        .setMax(record.get(MAX_COST_FIELD, Double.class))
                        .setMin(record.get(MIN_COST_FIELD, Double.class))
                        .setAvg(record.get(AVG_COST_FIELD, Double.class))
                        .setSum(record.get(SUM_COST_FIELD, Double.class)));

        // handle the special case where we've selected a single tag key/value
        if (record.field(Tables.COST_TAG.TAG_KEY) != null) {
            final String tagKey = record.get(Tables.COST_TAG.TAG_KEY);
            final String tagValue = record.get(Tables.COST_TAG.TAG_VALUE);
            if (tagKey != null && tagValue != null) {
                costStatBuilder.setTagGroup(CostTagGroup.newBuilder().putTags(tagKey, tagValue));
            }
        }

        RECORD_STAT_MAPPING_LIST.forEach(recordProtoMapping ->
                recordProtoMapping.updateProtoBuilder(record, tableAccessor, costStatBuilder));

        return costStatBuilder;
    }
}
