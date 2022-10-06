package com.vmturbo.cost.component.billed.cost;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;

/**
 * A builder of {@link BilledCostData} instances, supplementing the {@link BilledCostData.Builder} functionality to provide
 * helper methods for adding buckets and cost items.
 */
public class BilledCostDataBuilder {

    private final Granularity costDataGranularity;

    private final TagGroupResolver tagGroupResolver;

    private final Map<BilledCostDataKey, BilledCostData.Builder> costDataBuilderMap = new ConcurrentHashMap<>();

    private final Map<BilledCostBucketKey, BilledCostBucket.Builder> costBucketBuilderMap = new ConcurrentHashMap<>();

    private final SetMultimap<BilledCostDataKey, Long> tagGroupReferenceMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private BilledCostDataBuilder(@Nonnull Granularity costDataGranularity,
                                  @Nonnull TagGroupResolver tagGroupResolver) {

        this.costDataGranularity = Objects.requireNonNull(costDataGranularity);
        this.tagGroupResolver = Objects.requireNonNull(tagGroupResolver);
    }

    /**
     * Creates a new {@link BilledCostDataBuilder} instances.
     * @param costDataGranularity The granularity of cost data.
     * @param tagGroupResolver The tag group resolver, used to map tag ID references in cost items
     * added to this builder to the associated {@link com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup}.
     * @return The new {@link BilledCostDataBuilder} instance.
     */
    @Nonnull
    public static BilledCostDataBuilder create(@Nonnull Granularity costDataGranularity,
                                              @Nonnull TagGroupResolver tagGroupResolver) {
        return new BilledCostDataBuilder(costDataGranularity, tagGroupResolver);
    }

    /**
     * Adds the cost item to the builder.
     * @param billingFamilyId The billing family ID.
     * @param serviceProviderId The service provider ID.
     * @param sampleTs The sample timestamp.
     * @param tagGroupId The tag group ID.
     * @param costItem The cost item to add.
     */
    public void addCostItem(@Nullable Long billingFamilyId,
                                 @Nonnull long serviceProviderId,
                                 @Nonnull Instant sampleTs,
                                 @Nullable Long tagGroupId,
                                 @Nonnull BilledCostItem costItem) {

        final BilledCostDataKey costDataKey = BilledCostDataKey.of(billingFamilyId, serviceProviderId);
        final BilledCostData.Builder costDataBuilder = costDataBuilderMap.computeIfAbsent(costDataKey, this::createCostDataBuilder);

        final BilledCostBucketKey bucketKey = BilledCostBucketKey.of(costDataKey, sampleTs);
        final BilledCostBucket.Builder bucketBuilder = costBucketBuilderMap.computeIfAbsent(bucketKey, key ->
                costDataBuilder.addCostBucketsBuilder()
                        .setSampleTsUtc(sampleTs.toEpochMilli()));

        bucketBuilder.addCostItems(costItem);
        if (tagGroupId != null && tagGroupId != 0) {
            tagGroupReferenceMap.get(costDataKey).add(tagGroupId);
        }
    }

    /**
     * Builds {@link BilledCostData} instances with the {@link BilledCostData} data added through
     * {@link #addCostItem(Long, long, Instant, Long, BilledCostItem)}.
     * @param resolveTagReferences Whether to resolve tag group ID references.
     * @return The list of {@link BilledCostData} instances.
     */
    @Nonnull
    public List<BilledCostData> buildCostDataList(boolean resolveTagReferences) {

        return costDataBuilderMap.entrySet().stream()
                .map(costDataEntry -> {
                    final BilledCostData.Builder costDataBuilder = costDataEntry.getValue();
                    if (resolveTagReferences) {
                        final Set<Long> tagGroupIds = tagGroupReferenceMap.get(costDataEntry.getKey());
                        costDataBuilder.putAllCostTagGroup(tagGroupResolver.getTagGroupsById(tagGroupIds));
                    }

                    return costDataBuilder;
                })
                .map(BilledCostData.Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

    private BilledCostData.Builder createCostDataBuilder(@Nonnull BilledCostDataKey costDataKey) {

        final BilledCostData.Builder builder = BilledCostData.newBuilder()
                .setServiceProviderId(costDataKey.serviceProviderId())
                .setGranularity(costDataGranularity);

        if (costDataKey.billingFamilyId() != null) {
            builder.setBillingFamilyId(costDataKey.billingFamilyId());
        }

        return builder;
    }

    /**
     * A tag group ID resolver.
     */
    @FunctionalInterface
    public interface TagGroupResolver {

        /**
         * Resolves the tag groups associated with the provided IDs.
         * @param tagGroupIds The tag group IDs.
         * @return A map of the tag group IDs to their associated tag groups.
         */
        @Nonnull
        Map<Long, CostBilling.CostTagGroup> getTagGroupsById(@Nonnull Set<Long> tagGroupIds);
    }

    /**
     * A key identifying unique {@link BilledCostData} instances.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(prehash = true)
    interface BilledCostDataKey {

        @Nullable
        Long billingFamilyId();

        long serviceProviderId();

        @Nonnull
        static BilledCostDataKey of(@Nullable Long billingFamilyId,
                                   long serviceProviderId) {
            return BilledCostDataKeyTuple.of(billingFamilyId, serviceProviderId);
        }
    }

    /**
     * A key identifying unique {@link BilledCostBucket} instances.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(prehash = true)
    interface BilledCostBucketKey {

        BilledCostDataKey costDataKey();

        Instant sampleTs();

        static BilledCostBucketKey of(@Nonnull BilledCostDataKey costDataKey,
                                     @Nonnull Instant sampleTs) {
            return BilledCostBucketKeyTuple.of(costDataKey, sampleTs);
        }
    }
}
