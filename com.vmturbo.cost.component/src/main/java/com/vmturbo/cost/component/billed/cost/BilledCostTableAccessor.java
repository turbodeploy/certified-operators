package com.vmturbo.cost.component.billed.cost;

import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.jooq.Comparator;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.UpdatableRecord;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostFilter;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.CloudCostDailyRecord;
import com.vmturbo.cost.component.db.tables.records.CloudCostHourlyRecord;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.sql.utils.jooq.filter.JooqFilterMapper;

/**
 * A wrapper around a jOOQ cloud cost table.
 * @param <TableRecordT> The table record type.
 */
@HiddenImmutableImplementation
@Immutable
interface BilledCostTableAccessor<TableRecordT extends UpdatableRecord<TableRecordT>> {

    BilledCostTableAccessor<CloudCostHourlyRecord> CLOUD_COST_HOURLY = BilledCostTableAccessor.<CloudCostHourlyRecord>builder()
            .table(Tables.CLOUD_COST_HOURLY)
            .granularity(Granularity.HOURLY)
            .recordCreator(CloudCostHourlyRecord::new)
            .build();

    BilledCostTableAccessor<CloudCostDailyRecord> CLOUD_COST_DAILY = BilledCostTableAccessor.<CloudCostDailyRecord>builder()
            .table(Tables.CLOUD_COST_DAILY)
            .granularity(Granularity.DAILY)
            .recordCreator(CloudCostDailyRecord::new)
            .build();

    Table<TableRecordT> table();

    RecordCreator<TableRecordT> recordCreator();

    Granularity granularity();

    @Default
    default Field<Instant> sampleTs() {
        // A cast is required here, rather than .field("field_name", type). If .field("field_name", type)
        // is used, jOOQ will return the field with the default binding and converter, which will attempt to convert
        // the millisecond value to OffsetDateTime and fail.
        return (Field<Instant>)table().field("sample_ms_utc");
    }

    @Default
    default Field<Long> scopeId() {
        return table().field("scope_id", Long.class);
    }

    @Default
    default Field<Long> resourceId() {
        return Tables.CLOUD_SCOPE.RESOURCE_ID;
    }

    @Default
    default Field<Short> resourceType() {
        return Tables.CLOUD_SCOPE.RESOURCE_TYPE;
    }

    @Default
    default Field<Long> resourceGroupId() {
        return Tables.CLOUD_SCOPE.RESOURCE_GROUP_ID;
    }

    @Default
    default Field<Long> accountId() {
        return Tables.CLOUD_SCOPE.ACCOUNT_ID;
    }

    @Default
    default Field<Long> regionId() {
        return Tables.CLOUD_SCOPE.REGION_ID;
    }

    @Default
    default Field<Long> cloudServiceId() {
        return Tables.CLOUD_SCOPE.CLOUD_SERVICE_ID;
    }

    @Default
    default Field<Long> serviceProviderId() {
        return Tables.CLOUD_SCOPE.SERVICE_PROVIDER_ID;
    }

    @Default
    default Field<Long> tagGroupId() {
        return table().field("tag_group_id", Long.class);
    }

    @Default
    default Field<Long> billingFamilyId() {
        return table().field("billing_family_id", Long.class);
    }

    @Default
    default Field<Short> priceModel() {
        return table().field("price_model", Short.class);
    }

    @Default
    default Field<Short> costCategory() {
        return table().field("cost_category", Short.class);
    }

    @Default
    default Field<Long> providerId() {
        return table().field("provider_id", Long.class);
    }

    @Default
    default Field<Short> providerType() {
        return table().field("provider_type", Short.class);
    }

    @Nonnull
    default Field<Short> commodityType() {
        return table().field("commodity_type", Short.class);
    }

    @Default
    default Field<Double> usageAmount() {
        return table().field("usage_amount", Double.class);
    }

    @Default
    default Field<Short> currency() {
        return table().field("currency", Short.class);
    }

    @Default
    default Field<Double> cost() {
        return table().field("cost", Double.class);
    }

    @Derived
    default JooqFilterMapper<BilledCostFilter> filterMapper() {
        return JooqFilterMapper.<BilledCostFilter>builder()
                .addValueComparison(BilledCostFilter::hasSampleTsStart,
                        BilledCostFilter::getSampleTsStart,
                        Instant::ofEpochMilli,
                        sampleTs(),
                        Comparator.GREATER_OR_EQUAL)
                .addValueComparison(BilledCostFilter::hasSampleTsEnd,
                        BilledCostFilter::getSampleTsEnd,
                        Instant::ofEpochMilli,
                        sampleTs(),
                        Comparator.LESS)
                // Filter for resource ID is intentionally targeting the scope ID column to
                // use indexes.
                .addInCollection(BilledCostFilter::getResourceIdList, Tables.CLOUD_SCOPE.SCOPE_ID)
                .addInCollection(BilledCostFilter::getAccountIdList, accountId())
                .addInCollection(BilledCostFilter::getRegionIdList, regionId())
                .addInCollection(BilledCostFilter::getCloudServiceIdList, cloudServiceId())
                .addInCollection(BilledCostFilter::getResourceGroupIdList, resourceGroupId())
                .addInCollection(BilledCostFilter::getServiceProviderIdList, serviceProviderId())
                .addInCollection(BilledCostFilter::getProviderIdList, providerId())
                .addProtoEnumInShortCollection(BilledCostFilter::getPriceModelList, priceModel())
                .addProtoEnumInShortCollection(BilledCostFilter::getCostCategoryList, costCategory())
                .addValueComparison(BilledCostFilter::hasTagFilter,
                        costFilter -> costFilter.getTagFilter().getTagKey(),
                        Tables.COST_TAG.TAG_KEY,
                        Comparator.EQUALS)
                .addInCollection(costFilter -> costFilter.getTagFilter().getTagValueList(), Tables.COST_TAG.TAG_VALUE)
                .build();
    }

    @Nonnull
    default BilledCostRecordAccessor createRecordAccessor(@Nonnull Record record) {
        return new BilledCostRecordAccessor(this, record);
    }

    @Nonnull
    default BilledCostStatRecordAccessor createStatRecordAccessor(@Nonnull Record record) {
        return new BilledCostStatRecordAccessor(this, record);
    }

    @Nonnull
    default TableRecordT createTableRecord(@Nonnull BilledCostItem costItem,
                                           @Nonnull Instant sampleTs,
                                           long billingFamilyId,
                                           long cloudScopeId,
                                           @Nullable Long tagGroupId) {

        return recordCreator().createRecord(
                sampleTs,
                cloudScopeId,
                tagGroupId,
                billingFamilyId,
                (short)costItem.getPriceModel().getNumber(),
                (short)costItem.getCostCategory().getNumber(),
                // will default to 0 if a provider ID is not provided
                costItem.getProviderId(),
                costItem.hasProviderType() ? (short)costItem.getProviderType() : null,
                (short)costItem.getCommodityType(),
                costItem.getUsageAmount(),
                (short)costItem.getCost().getCurrency(),
                costItem.getCost().getAmount());
    }

    /**
     * Interface for constructing a table record instance from the provided information.
     * @param <TableRecordT> The table record type.
     */
    interface RecordCreator<TableRecordT extends UpdatableRecord<TableRecordT>> {

        TableRecordT createRecord(@Nonnull Instant sampleTs,
                                  long scopeId,
                                  @Nullable Long tagGroupId,
                                  @Nullable Long billingFamilyId,
                                  short pricingModel,
                                  short costCategory,
                                  @Nullable Long providerId,
                                  @Nullable Short providerType,
                                  short commodityType,
                                  double usageAmount,
                                  short currency,
                                  double cost);

    }

    @Nonnull
    static <TableRecordT extends UpdatableRecord<TableRecordT>> Builder<TableRecordT> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for constructing immutable {@link BilledCostTableAccessor} instances.
     * @param <TableRecordT> The table record type.
     */
    class Builder<TableRecordT extends UpdatableRecord<TableRecordT>> extends ImmutableBilledCostTableAccessor.Builder<TableRecordT> {}
}
