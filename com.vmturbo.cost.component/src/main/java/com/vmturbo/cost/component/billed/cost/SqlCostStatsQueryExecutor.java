package com.vmturbo.cost.component.billed.cost;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery.BilledCostGroupBy;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;

/**
 * Helper class for executing {@link BilledCostStatsQuery} in SQL.
 */
class SqlCostStatsQueryExecutor {

    private static final Map<BilledCostGroupBy, GroupByFieldMapper> GROUP_BY_FIELD_MAPPERS = ImmutableMap.<BilledCostGroupBy, GroupByFieldMapper>builder()
            .put(BilledCostGroupBy.ENTITY,
                    tableAccessor -> ImmutableList.of(
                            tableAccessor.resourceId(),
                            tableAccessor.resourceType(),
                            tableAccessor.accountId(),
                            tableAccessor.regionId(),
                            tableAccessor.cloudServiceId(),
                            tableAccessor.serviceProviderId()))
            .put(BilledCostGroupBy.ENTITY_TYPE, tableAccessor -> ImmutableList.of(tableAccessor.resourceType()))
            .put(BilledCostGroupBy.REGION,
                    tableAccessor -> ImmutableList.of(
                            tableAccessor.regionId(),
                            tableAccessor.serviceProviderId()))
            .put(BilledCostGroupBy.ACCOUNT,
                    tableAccessor -> ImmutableList.of(
                            tableAccessor.accountId(),
                            tableAccessor.serviceProviderId()))
            .put(BilledCostGroupBy.CLOUD_SERVICE,
                    tableAccessor -> ImmutableList.of(
                            tableAccessor.cloudServiceId(),
                            tableAccessor.serviceProviderId()))
            .put(BilledCostGroupBy.SERVICE_PROVIDER, tableAccessor -> ImmutableList.of(tableAccessor.serviceProviderId()))
            .put(BilledCostGroupBy.CLOUD_TIER_PROVIDER, tableAccessor -> ImmutableList.of(tableAccessor.providerId()))
            .put(BilledCostGroupBy.PRICING_MODEL, tableAccessor -> ImmutableList.of(tableAccessor.priceModel()))
            .put(BilledCostGroupBy.COST_CATEGORY, tableAccessor -> ImmutableList.of(tableAccessor.costCategory()))
            .put(BilledCostGroupBy.COMMODITY_TYPE, tableAccessor -> ImmutableList.of(tableAccessor.commodityType()))
            .put(BilledCostGroupBy.TAG_GROUP, tableAccessor -> ImmutableList.of(tableAccessor.tagGroupId()))
            .put(BilledCostGroupBy.TAG,
                    tableAccessor -> ImmutableList.of(
                            Tables.COST_TAG.TAG_KEY,
                            Tables.COST_TAG.TAG_VALUE))
            .build();

    private static final Map<TimeFrame, Granularity> TIME_FRAME_GRANULARITY_MAP = ImmutableMap.of(
            TimeFrame.LATEST, Granularity.HOURLY,
            TimeFrame.HOUR, Granularity.HOURLY,
            TimeFrame.DAY, Granularity.DAILY,
            TimeFrame.MONTH, Granularity.MONTHLY);

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final TagGroupIdentityService tagGroupIdentityService;

    private final TimeFrameCalculator timeFrameCalculator;

    SqlCostStatsQueryExecutor(@Nonnull DSLContext dsl,
                              @Nonnull TagGroupIdentityService tagGroupIdentityService,
                              @Nonnull TimeFrameCalculator timeFrameCalculator) {

        this.dsl = Objects.requireNonNull(dsl);
        this.tagGroupIdentityService = Objects.requireNonNull(tagGroupIdentityService);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
    }


    public List<BilledCostStat> getCostStats(@Nonnull BilledCostStatsQuery costStatsQuery) {

        logger.debug("Executing cloud cost stats query: {}", costStatsQuery);

        validateStatsQuery(costStatsQuery);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final BilledCostTableAccessor<?> tableAccessor = resolveTableAccessor(costStatsQuery);

        final ImmutableList.Builder<BilledCostStat> costStatsList = ImmutableList.builder();
        try (Stream<Record> recordStream = dsl.select(createSelectForStatsQuery(tableAccessor, costStatsQuery))
                .from(createTableJoins(tableAccessor, costStatsQuery))
                .where(tableAccessor.filterMapper().generateConditions(costStatsQuery.getFilter()))
                .groupBy(createGroupFields(tableAccessor, costStatsQuery))
                .stream()) {


            final boolean requiresTagGroupResolution = costStatsQuery.getGroupByList().contains(BilledCostGroupBy.TAG_GROUP);

            if (requiresTagGroupResolution) {
                final ListMultimap<Long, BilledCostStat.Builder> costStatsByTagGroup = recordStream
                        .map(tableAccessor::createStatRecordAccessor)
                        .collect(ImmutableListMultimap.toImmutableListMultimap(
                                BilledCostStatRecordAccessor::tagGroupId,
                                BilledCostStatRecordAccessor::toCostStatBuilder));

                // Remove tag group zero - it represents an empty tag group
                final Set<Long> tagGroupIds = Sets.difference(costStatsByTagGroup.keySet(), Collections.singleton(0L));
                final Map<Long, CostTagGroup> tagGroupsMap = tagGroupIdentityService.getTagGroupsById(tagGroupIds);

                costStatsByTagGroup.asMap().forEach((tagGroupId, costStatBuilders) -> {
                    costStatBuilders.stream()
                            .peek(costStatsBuilder -> {
                                if (tagGroupsMap.containsKey(tagGroupId)) {
                                    costStatsBuilder.setTagGroup(tagGroupsMap.get(tagGroupId));
                                }
                            }).map(BilledCostStat.Builder::build)
                            .forEach(costStatsList::add);
                });

            } else {
                recordStream.map(tableAccessor::createStatRecordAccessor)
                        .map(BilledCostStatRecordAccessor::toCostStatBuilder)
                        .map(BilledCostStat.Builder::build)
                        .forEach(costStatsList::add);
            }

        }

        logger.info("Executed the following cost stats query in {}:\n{}", stopwatch, costStatsQuery);

        return costStatsList.build();
    }


    private BilledCostTableAccessor<?> resolveTableAccessor(@Nonnull BilledCostStatsQuery statsQuery) {

        final Granularity granularity = statsQuery.hasGranularity()
                ? statsQuery.getGranularity()
                : TIME_FRAME_GRANULARITY_MAP.get(timeFrameCalculator.millis2TimeFrame(statsQuery.getFilter().getSampleTsStart()));

        switch (granularity) {
            case HOURLY:
                return BilledCostTableAccessor.CLOUD_COST_HOURLY;
            case DAILY:
                return BilledCostTableAccessor.CLOUD_COST_DAILY;
            default:
                throw new UnsupportedOperationException(
                        String.format("Granularity %s is not supported cost cloud cost", granularity));
        }
    }

    private Table<Record> createTableJoins(@Nonnull BilledCostTableAccessor<?> costTableAccessor,
                                           @Nonnull BilledCostStatsQuery statsQuery) {

        Table<Record> aggregateTable = costTableAccessor.table()
                .join(Tables.CLOUD_SCOPE)
                .on(costTableAccessor.scopeId().eq(Tables.CLOUD_SCOPE.SCOPE_ID));

        final boolean requiresTagJoin = statsQuery.getGroupByList().contains(BilledCostGroupBy.TAG)
                || statsQuery.getFilter().hasTagFilter();
        if (requiresTagJoin) {
            aggregateTable = aggregateTable.join(Tables.COST_TAG_GROUPING)
                    .on(costTableAccessor.tagGroupId().eq(Tables.COST_TAG_GROUPING.TAG_GROUP_ID))
                    .join(Tables.COST_TAG)
                    .on(Tables.COST_TAG_GROUPING.TAG_ID.eq(Tables.COST_TAG.TAG_ID));
        }

        return aggregateTable;
    }


    private void validateStatsQuery(@Nonnull BilledCostStatsQuery statsQuery) {

        Preconditions.checkArgument(statsQuery.hasGranularity() || statsQuery.getFilter().hasSampleTsStart(),
                "Cost query must have either a start time or granularity set");

        if (statsQuery.getGroupByList().contains(BilledCostGroupBy.TAG)) {
            Preconditions.checkArgument(statsQuery.getFilter().hasTagFilter(),
                    "Cloud cost stats must include a tag filter when grouping by tag");
        }
    }

    private List<Field<?>> createSelectForStatsQuery(@Nonnull BilledCostTableAccessor<?> tableAccessor,
                                                     @Nonnull BilledCostStatsQuery statsQuery) {

        final ImmutableList.Builder<Field<?>> selectFields = ImmutableList.<Field<?>>builder()
                .add(tableAccessor.sampleTs())
                .add(tableAccessor.currency())
                .add(DSL.count().as(BilledCostStatRecordAccessor.SAMPLE_COUNT_FIELD))
                .add(DSL.max(tableAccessor.usageAmount()).as(BilledCostStatRecordAccessor.MAX_USAGE_AMOUNT_FIELD))
                .add(DSL.min(tableAccessor.usageAmount()).as(BilledCostStatRecordAccessor.MIN_USAGE_AMOUNT_FIELD))
                .add(DSL.avg(tableAccessor.usageAmount()).as(BilledCostStatRecordAccessor.AVG_USAGE_AMOUNT_FIELD))
                .add(DSL.sum(tableAccessor.usageAmount()).as(BilledCostStatRecordAccessor.SUM_USAGE_AMOUNT_FIELD))
                .add(DSL.max(tableAccessor.cost()).as(BilledCostStatRecordAccessor.MAX_COST_FIELD))
                .add(DSL.min(tableAccessor.cost()).as(BilledCostStatRecordAccessor.MIN_COST_FIELD))
                .add(DSL.avg(tableAccessor.cost()).as(BilledCostStatRecordAccessor.AVG_COST_FIELD))
                .add(DSL.sum(tableAccessor.cost()).as(BilledCostStatRecordAccessor.SUM_COST_FIELD));

        statsQuery.getGroupByList().forEach(groupBy -> {

            if (GROUP_BY_FIELD_MAPPERS.containsKey(groupBy)) {
                final GroupByFieldMapper groupByFieldMapper = GROUP_BY_FIELD_MAPPERS.get(groupBy);
                selectFields.addAll(groupByFieldMapper.getFields(tableAccessor));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Group by condition %s is not supported for cloud cost", groupBy));
            }
        });

        return selectFields.build();
    }

    private Set<GroupField> createGroupFields(@Nonnull BilledCostTableAccessor<?> tableAccessor,
                                               @Nonnull BilledCostStatsQuery statsQuery) {

        // Always group by both time stamp and currency (to avoid mixing currencies)
        final ImmutableSet.Builder<GroupField> groupFields = ImmutableSet.<GroupField>builder()
                .add(tableAccessor.sampleTs())
                .add(tableAccessor.currency());

        statsQuery.getGroupByList().forEach(groupBy -> {

            if (GROUP_BY_FIELD_MAPPERS.containsKey(groupBy)) {
                final GroupByFieldMapper groupByFieldMapper = GROUP_BY_FIELD_MAPPERS.get(groupBy);
                groupFields.addAll(groupByFieldMapper.getFields(tableAccessor));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Group by condition %s is not supported for cloud cost", groupBy));
            }
        });

        return groupFields.build();
    }

    /**
     * Maps a group by query condition to the associated table fields.
     */
    private interface GroupByFieldMapper {

        List<Field<?>> getFields(@Nonnull BilledCostTableAccessor<?> tableAccessor);
    }
}
