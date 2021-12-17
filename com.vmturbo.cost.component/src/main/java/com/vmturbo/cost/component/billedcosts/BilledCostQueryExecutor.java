package com.vmturbo.cost.component.billedcosts;

import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.SelectJoinStep;
import org.jooq.SelectWhereStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord.TagKeyValuePair;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.TagFilter;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.db.tables.records.BilledCostDailyRecord;

/**
 * This class retrieves billed costs stats from the database.
 */
class BilledCostQueryExecutor {

    private static final BilledCostDaily DEFAULT_TABLE = Tables.BILLED_COST_DAILY;
    private static final Map<TimeFrame, Table<?>> TIME_FRAME_TO_TABLE = ImmutableMap.of(
            TimeFrame.LATEST, DEFAULT_TABLE,
            TimeFrame.HOUR, Tables.BILLED_COST_HOURLY,
            TimeFrame.DAY, Tables.BILLED_COST_DAILY,
            TimeFrame.MONTH, Tables.BILLED_COST_MONTHLY,
            TimeFrame.YEAR, Tables.BILLED_COST_MONTHLY);

    private final DSLContext dsl;
    private final TimeFrameCalculator timeFrameCalculator;

    /**
     * Create new instance of {@link BilledCostQueryExecutor}.
     *
     * @param dsl DSL context.
     * @param timeFrameCalculator Calculator to identify time frame based on specified start date.
     */
    BilledCostQueryExecutor(
            @Nonnull final DSLContext dsl,
            @Nonnull final TimeFrameCalculator timeFrameCalculator) {
        this.dsl = Objects.requireNonNull(dsl);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
    }

    /**
     * Get billed entity cost snapshots for the given request.
     *
     * @param request Request object.
     * @return List of stats snapshots.
     */
    List<CostStatsSnapshot> getBilledCostStats(@Nonnull final GetCloudBilledStatsRequest request) {
        final Table<?> table = getTable(request);
        final Field<LocalDateTime> sampleTime = getField(table, DEFAULT_TABLE.SAMPLE_TIME);
        final Field<Long> tagGroupId = getField(table, DEFAULT_TABLE.TAG_GROUP_ID);
        final Field<Double> cost = getField(table, DEFAULT_TABLE.COST);

        final Set<Long> tagGroupIds = new HashSet<>();
        final Map<Long, TagKeyValuePair> tags = new HashMap<>();
        queryTags(request, tagGroupIds, tags);

        // Build GROUP BY clause
        final ImmutableList.Builder<Field<?>> groupByFieldsBuilder = ImmutableList.<Field<?>>builder()
                .add(sampleTime);
        final List<GroupByType> groupByList = request.getGroupByList();
        if (!groupByList.isEmpty()) {
            validateGroupByList(groupByList);
            // Currently, we only support grouping by tag.
            groupByFieldsBuilder.add(Tables.COST_TAG_GROUPING.TAG_ID);
        }
        final List<Field<?>> groupByFields = groupByFieldsBuilder.build();

        // Build WHERE clause
        final ImmutableList.Builder<Condition> conditions = ImmutableList.builder();
        if (request.hasStartDate()) {
            conditions.add(sampleTime.greaterOrEqual(convertTimestamp(request.getStartDate())));
        }
        if (request.hasEndDate()) {
            conditions.add(sampleTime.lessOrEqual(convertTimestamp(request.getEndDate())));
        }
        if (request.getTagFilterCount() > 0) {
            if (groupByList.isEmpty()) {
                conditions.add(tagGroupId.in(tagGroupIds));
            } else {
                conditions.add(Tables.COST_TAG_GROUPING.TAG_ID.in(tags.keySet()));
            }
        }
        if (request.hasEntityFilter()) {
            final Field<Long> entityId = getField(table, DEFAULT_TABLE.ENTITY_ID);
            conditions.add(entityId.in(request.getEntityFilter().getEntityIdList()));
        }
        if (request.hasRegionFilter()) {
            final Field<Long> regionId = getField(table, DEFAULT_TABLE.REGION_ID);
            conditions.add(regionId.in(request.getRegionFilter().getRegionIdList()));
        }
        if (request.hasAccountFilter()) {
            final Field<Long> accountId = getField(table, DEFAULT_TABLE.ACCOUNT_ID);
            conditions.add(accountId.in(request.getAccountFilter().getAccountIdList()));
        }

        // Build SELECT clause
        final List<Field<?>> selectFields = ImmutableList.<Field<?>>builder()
                .add(sum(cost), avg(cost), min(cost), max(cost))
                .addAll(groupByFields)
                .build();

        final SelectJoinStep<Record> joinStep = dsl.select(selectFields).from(table);

        // Join tag_grouping table if we need to group by tag
        final SelectWhereStep<Record> whereStep = groupByList.isEmpty()
                ? joinStep
                : joinStep.leftJoin(Tables.COST_TAG_GROUPING)
                    .on(Tables.COST_TAG_GROUPING.TAG_GROUP_ID.eq(tagGroupId));

        return whereStep.where(conditions.build())
                .groupBy(groupByFields)
                .fetch()
                .stream()
                .collect(
                        ArrayListMultimap::<LocalDateTime, StatRecord>create,
                        (map, record) -> accumulateBilledCostResult(map, record, tags),
                        Multimap::putAll)
                .asMap()
                .entrySet()
                .stream()
                .map(BilledCostQueryExecutor::toCostStatsSnapshot)
                .sorted(Comparator.comparing(CostStatsSnapshot::getSnapshotDate))
                .collect(Collectors.toList());
    }

    @Nonnull
    private Table<?> getTable(@Nonnull final GetCloudBilledStatsRequest request) {
        if (!request.hasStartDate()) {
            return DEFAULT_TABLE;
        }
        final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(
                request.getStartDate());
        return TIME_FRAME_TO_TABLE.get(timeFrame);
    }

    @Nonnull
    private static <T> Field<T> getField(
            @Nonnull final Table<?> table,
            @Nonnull final TableField<BilledCostDailyRecord, T> field) {
        return (Field<T>)table.field(field.getName());
    }

    @Nonnull
    private static LocalDateTime convertTimestamp(final long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                ZoneId.from(ZoneOffset.UTC));
    }

    private void queryTags(
            @Nonnull final GetCloudBilledStatsRequest request,
            @Nonnull final Set<Long> tagGroupIds,
            @Nonnull final Map<Long, TagKeyValuePair> tags) {
        final Condition where = request.getTagFilterList().stream()
                .map(BilledCostQueryExecutor::tagFilterToCondition)
                .reduce(Condition::and)
                .orElse(DSL.trueCondition());
        dsl.select(Tables.COST_TAG_GROUPING.TAG_GROUP_ID,
                        Tables.COST_TAG.TAG_ID,
                        Tables.COST_TAG.TAG_KEY,
                        Tables.COST_TAG.TAG_VALUE)
                .from(Tables.COST_TAG)
                .join(Tables.COST_TAG_GROUPING).onKey()
                .where(where)
                .fetch()
                .forEach(record -> {
                    tagGroupIds.add(record.value1());
                    tags.putIfAbsent(record.value2(), toTagKeyValuePair(record));
                });
    }

    @Nonnull
    private static Condition tagFilterToCondition(@Nonnull final TagFilter tagFilter) {
        final String tagKey = tagFilter.getTagKey();
        final Condition tagValueCondition = tagFilter.getTagValueList().stream()
                .map(Tables.COST_TAG.TAG_VALUE::eq)
                .reduce(Condition::or)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid TagFilter: no tag values provided for tag key: " + tagKey));
        return Tables.COST_TAG.TAG_KEY.eq(tagKey).and(tagValueCondition);
    }

    @Nonnull
    private static TagKeyValuePair toTagKeyValuePair(
            @Nonnull final Record4<Long, Long, String, String> record) {
        return TagKeyValuePair.newBuilder()
                .setKey(record.value3())
                .setValue(record.value4())
                .build();
    }

    private static void validateGroupByList(@Nonnull final List<GroupByType> groupByList) {
        if (!groupByList.isEmpty()) {
            if (groupByList.size() > 1) {
                throw new IllegalArgumentException("Multiple group by fields are not supported");
            }
            final GroupByType groupBy = groupByList.get(0);
            if (groupBy != GroupByType.TAG) {
                throw new IllegalArgumentException("Unsupported group by: " + groupBy);
            }
        }
    }

    private static void accumulateBilledCostResult(
            @Nonnull final Multimap<LocalDateTime, StatRecord> map,
            @Nonnull final Record record,
            @Nonnull final Map<Long, TagKeyValuePair> tags) {
        final float total = record.getValue(0, BigDecimal.class).floatValue();
        final float avg = record.getValue(1, BigDecimal.class).floatValue();
        final float min = record.getValue(2, Double.class).floatValue();
        final float max = record.getValue(3, Double.class).floatValue();
        final StatRecord.Builder statRecord = StatRecord.newBuilder()
                .setName(StringConstants.BILLED_COST)
                .setValue(StatValue.newBuilder()
                        .setTotal(total)
                        .setAvg(avg)
                        .setMin(min)
                        .setMax(max)
                        .build());
        if (record.size() >= 6) {
            final Long tagId = record.getValue(5, Long.class);
            if (tagId != null) {
                statRecord.addTag(tags.get(tagId));
            }
        }
        final LocalDateTime sampleTime = record.getValue(4, LocalDateTime.class);
        map.put(sampleTime, statRecord.build());
    }

    private static CostStatsSnapshot toCostStatsSnapshot(
            @Nonnull final Map.Entry<LocalDateTime, Collection<StatRecord>> entry) {
        return CostStatsSnapshot.newBuilder()
                .setSnapshotDate(entry.getKey().atOffset(ZoneOffset.UTC).toInstant().toEpochMilli())
                .addAllStatRecords(entry.getValue())
                .build();
    }
}
