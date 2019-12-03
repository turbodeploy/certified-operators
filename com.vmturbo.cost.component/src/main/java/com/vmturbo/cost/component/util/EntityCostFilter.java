package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Table;

import com.google.common.collect.Sets;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the entity cost records from the
 * {@link com.vmturbo.cost.component.entity.cost.EntityCostStore}.
 * It provider a easier way to define simple search over entity cost records
 * in the tables.
 */
public class EntityCostFilter extends CostFilter {

    private static final String CREATED_TIME = ENTITY_COST.CREATED_TIME.getName();

    private final List<Condition> conditions;

    @Nullable
    private final CostGroupBy costGroupBy;

    public EntityCostFilter(final Set<Long> entityFilters,
                            final Set<Integer> entityTypeFilters,
                            final long startDateMillis,
                            final long endDateMillis,
                            @Nullable final TimeFrame timeFrame,
                            @Nonnull Set<String> groupByFields) {
        super(entityFilters, entityTypeFilters, startDateMillis, endDateMillis, timeFrame, CREATED_TIME);
        this.conditions = generateConditions();
        this.costGroupBy = createGroupByFieldString(groupByFields);
    }

    @Nullable
    private CostGroupBy createGroupByFieldString(@Nonnull Set<String> items ) {
        Set<String> listOfFields = Sets.newHashSet(items);
        listOfFields.add(CREATED_TIME);
        return items.isEmpty() ?
                null :
                new CostGroupBy(listOfFields.stream().map(columnName -> columnName.toLowerCase(Locale.getDefault()))
                        .collect(Collectors.toSet()),
                        timeFrame);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    public List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();


        final Table<?> table = getTable();

        if (startDateMillis > 0 && endDateMillis > 0) {
            conditions.add(getTable().field(ENTITY_COST.CREATED_TIME).between(getLocalDateTime(startDateMillis),
                    getLocalDateTime(endDateMillis)));
        }

        if (!entityTypeFilters.isEmpty()) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_TYPE.getName()).in(entityTypeFilters));
        }

        if (!entityFilters.isEmpty()) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()).in(entityFilters));
        }
        return conditions;
    }

    @Override
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    public Table<?> getTable() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.ENTITY_COST;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.ENTITY_COST_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.ENTITY_COST_BY_DAY;
        } else {
            return Tables.ENTITY_COST_BY_MONTH;
        }
    }

    /**
     * GroupBy for {@link com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord}.
     * @return null if there was no groupBy property in request.
     */
    @Nullable
    public CostGroupBy getCostGroupBy() {
        return costGroupBy;
    }

    /**
     * Convert date time to local date time.
     *
     * @param dateTime date time with long type.
     * @return local date time with LocalDateTime type.
     */
    private LocalDateTime getLocalDateTime(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime),
                TimeZone.getDefault().toZoneId());
    }
}
