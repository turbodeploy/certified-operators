package com.vmturbo.cost.component.billed.cost;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.TableRecord;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.scope.CloudScope;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.IdentityOperationException;
import com.vmturbo.cloud.common.scope.IdentityUninitializedException;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.scope.ScopeIdReplacementLog;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.partition.IPartitioningManager;
import com.vmturbo.sql.utils.partition.PartitionProcessingException;

/**
 * Responsible for persisting {@link BilledCostData} to a cloud cost table represented as through a {@link BilledCostTableAccessor}.
 */
class BilledCostWriter {

    private static final Map<Granularity, BilledCostTableAccessor<?>> TABLE_ACCESSOR_MAP = ImmutableMap.of(
            Granularity.HOURLY, BilledCostTableAccessor.CLOUD_COST_HOURLY,
            Granularity.DAILY, BilledCostTableAccessor.CLOUD_COST_DAILY);

    private final Logger logger = LogManager.getLogger();

    private final IPartitioningManager partitioningManager;

    private final TagGroupIdentityService tagGroupIdentityService;

    private final CloudScopeIdentityProvider scopeIdentityProvider;

    private final ScopeIdReplacementLog scopeIdReplacementLog;

    private final DSLContext dsl;

    protected BilledCostWriter(@Nonnull IPartitioningManager partitioningManager,
                               @Nonnull TagGroupIdentityService tagGroupIdentityService,
                               @Nonnull CloudScopeIdentityProvider scopeIdentityProvider,
                               @Nonnull ScopeIdReplacementLog scopeIdReplacementLog,
                               @Nonnull DSLContext dsl) {

        this.partitioningManager = Objects.requireNonNull(partitioningManager);
        this.tagGroupIdentityService = Objects.requireNonNull(tagGroupIdentityService);
        this.scopeIdentityProvider = Objects.requireNonNull(scopeIdentityProvider);
        this.scopeIdReplacementLog = Objects.requireNonNull(scopeIdReplacementLog);
        this.dsl = Objects.requireNonNull(dsl);
    }

    protected void persistCostData(@Nonnull BilledCostData costData)
            throws IdentityOperationException, IdentityUninitializedException, DbException, PartitionProcessingException {
        persistCostData(ImmutableList.of(costData), costData.getGranularity());
    }


    protected BilledCostPersistenceStats persistCostData(@Nonnull List<BilledCostData> billedCostDataList,
                                                         @Nonnull Granularity granularity)
            throws IdentityOperationException, IdentityUninitializedException, DbException, PartitionProcessingException {

        final BilledCostTableAccessor<?> tableAccessor = getTableAccessorOrThrow(granularity);

        final List<ExpandedCostItem> expandedCostItems = expandedCostItems(billedCostDataList);

        final Map<CostTagGroup, Long> tagGroupIdMap = persistTagGroupings(billedCostDataList);

        final Set<CloudScope> cloudScopes = expandedCostItems.stream()
                .map(ExpandedCostItem::scope)
                .collect(ImmutableSet.toImmutableSet());
        final Map<CloudScope, CloudScopeIdentity> scopeIdentityMap =
                scopeIdentityProvider.getOrCreateScopeIdentities(cloudScopes);

        ensurePartitions(billedCostDataList, tableAccessor);
        persistCostItems(expandedCostItems, tagGroupIdMap, scopeIdentityMap, tableAccessor);

        return BilledCostPersistenceStats.builder()
                .billingBucketCount(billedCostDataList.stream()
                        .mapToLong(BilledCostData::getCostBucketsCount)
                        .sum())
                .billingItemCount(expandedCostItems.size())
                .cloudScopeCount(scopeIdentityMap.size())
                .tagGroupCount(tagGroupIdMap.size())
                .build();
    }

    private BilledCostTableAccessor<?> getTableAccessorOrThrow(@Nonnull Granularity granularity) {
        if (TABLE_ACCESSOR_MAP.containsKey(granularity)) {
            return TABLE_ACCESSOR_MAP.get(granularity);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported granularity: %s", granularity));
        }
    }

    private List<ExpandedCostItem> expandedCostItems(@Nonnull List<BilledCostData> billedCostDataList) {

        final ImmutableList.Builder<ExpandedCostItem> expandedCostItems = ImmutableList.builder();
        billedCostDataList.forEach(billedCostData ->
                billedCostData.getCostBucketsList().forEach(costBucket ->
                        costBucket.getCostItemsList().forEach(costItem -> {
                            final CostTagGroup costTagGroup = costItem.hasCostTagGroupId()
                                    ? billedCostData.getCostTagGroupMap().get(costItem.getCostTagGroupId())
                                    : null;

                            expandedCostItems.add(ExpandedCostItem.builder()
                                    .billingFamilyId(billedCostData.getBillingFamilyId())
                                    .serviceProviderId(billedCostData.getServiceProviderId())
                                    .costItem(costItem)
                                    .costTagGroup(costTagGroup)
                                    .parentBucket(costBucket)
                                    .build());
                        })));

        return expandedCostItems.build();
    }

    private Map<CostTagGroup, Long> persistTagGroupings(@Nonnull List<BilledCostData> billedCostDataList) throws DbException {

        final Set<CostTagGroup> costTagGroups = billedCostDataList.stream()
                .flatMap(billedCostData -> billedCostData.getCostTagGroupMap().values().stream())
                .collect(ImmutableSet.toImmutableSet());

        // remap cost tags to new local IDs to guarantee there there is no overlap in local IDs
        // within the cloud cost data list.
        final MutableLong localIdProvider = new MutableLong();
        final Map<Long, CostTagGroup> localTagGroupMap = costTagGroups.stream()
                .collect(ImmutableMap.toImmutableMap(
                        (costTagGroup) -> localIdProvider.getAndIncrement(),
                        Function.identity()));

        final Map<Long, Long> localToPersistentIdMap = tagGroupIdentityService.resolveIdForDiscoveredTagGroups(localTagGroupMap);

        return localTagGroupMap.entrySet().stream()
                // filter out those tag groups that were not successfully resolved.
                .filter(localTagGroupEntry -> localToPersistentIdMap.containsKey(localTagGroupEntry.getKey()))
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getValue,
                        localTagGroupEntry -> localToPersistentIdMap.get(localTagGroupEntry.getKey())));
    }

    private void ensurePartitions(@Nonnull final List<BilledCostData> costDataList,
                                  @Nonnull final BilledCostTableAccessor<?> tableAccessor) throws PartitionProcessingException {

        final Set<Instant> bucketSampleTimes = costDataList.stream()
                .flatMap(billedCostData -> billedCostData.getCostBucketsList().stream())
                .map(costBucket -> Instant.ofEpochMilli(costBucket.getSampleTsUtc()))
                .collect(ImmutableSet.toImmutableSet());

        logger.debug("Ensuring partition existence for {} billed cost samples times", bucketSampleTimes.size());

        for (Instant bucketSampleTime : bucketSampleTimes) {

            logger.trace("Ensuring partition existence for {}", bucketSampleTime);
            partitioningManager.prepareForInsertion(tableAccessor.table(), Timestamp.from(bucketSampleTime));
        }
    }

    private void persistCostItems(@Nonnull List<ExpandedCostItem> costItems,
                                  @Nonnull Map<CostTagGroup, Long> tagGroupIdMap,
                                  @Nonnull Map<CloudScope, CloudScopeIdentity> scopeIdentityMap,
                                  @Nonnull BilledCostTableAccessor<?> tableAccessor) {

        logger.debug("Persisting {} cost items to {}", costItems.size(), tableAccessor.table().getName());

        dsl.transaction(configuration -> {
            final DSLContext context = DSL.using(configuration);

            final List<Insert<?>> recordUpsertStatements = costItems.stream()
                    .filter(expandedItem -> scopeIdentityMap.containsKey(expandedItem.scope()))
                    .map(expandedItem -> {
                        final CloudScopeIdentity scopeIdentity = scopeIdentityMap.get(expandedItem.scope());
                        final long scopeId = scopeIdReplacementLog.getReplacedScopeId(
                            scopeIdentity.scopeId(), expandedItem.sampleTs());
                        final long costTagGroupId = expandedItem.hasCostTagGroup()
                                ? tagGroupIdMap.get(expandedItem.costTagGroup())
                                // If a cost item does not have a tag group, set the tag group ID to zero (indicative of null).
                                // MySQL does not allow nullable primary key columns
                                : 0L;

                        // Skip adding the item.
                        if (expandedItem.hasCostTagGroup() && costTagGroupId == 0L) {
                            logger.warn("Unable to resolve tag group persistent ID for the following group:\n{}", expandedItem.costTagGroup());
                            return null;
                        } else {
                            final TableRecord<?> tableRecord =
                                    tableAccessor.createTableRecord(expandedItem.costItem(), expandedItem.sampleTs(), expandedItem.billingFamilyId(),
                                        scopeId, costTagGroupId);
                            return context.insertInto(tableAccessor.table())
                                    .set(tableRecord)
                                    .onDuplicateKeyUpdate()
                                    .set(tableAccessor.usageAmount(), expandedItem.costItem().getUsageAmount())
                                    .set(tableAccessor.cost(), expandedItem.costItem().getCost().getAmount());
                        }
                    }).filter(Objects::nonNull)
                    .collect(ImmutableList.toImmutableList());

            context.batch(recordUpsertStatements).execute();
        });
    }
}