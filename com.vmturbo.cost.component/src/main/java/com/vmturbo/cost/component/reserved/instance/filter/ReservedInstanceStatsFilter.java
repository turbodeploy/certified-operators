package com.vmturbo.cost.component.reserved.instance.filter;

import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.AVAILABILITY_ZONE_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.BUSINESS_ACCOUNT_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.REGION_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A abstract class represent a filter object which will be used to query reserved instance stats
 * related tables.
 */
public abstract class ReservedInstanceStatsFilter extends ReservedInstanceFilter {

    protected final long startDateMillis;

    protected final long endDateMillis;

    private final List<Condition> conditions;

    protected final TimeFrame timeFrame;

    /**
     * Constructor for ReservedInstanceStatsFilter.
     *
     * @param scopeIds The scope(s) Ids.
     * @param scopeEntityType The scope(s) entity type.
     * @param startDateMillis Start time in ms.
     * @param endDateMillis End time in ms.
     * @param timeFrame The timeframe for which to obtain stats.
     */
    public ReservedInstanceStatsFilter(@Nonnull final Set<Long> scopeIds,
                                       final int scopeEntityType,
                                       final long startDateMillis,
                                       final long endDateMillis,
                                       @Nullable final TimeFrame timeFrame) {
        super(scopeIds, scopeEntityType);
        this.startDateMillis = startDateMillis;
        this.endDateMillis = endDateMillis;
        this.timeFrame = timeFrame;
        this.conditions = generateConditions(scopeIds, scopeEntityType);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * Note that the where condition is only containing one filter clause at present.
     * To have multiple filters, there would need to be AND's in the where clause.
     *
     * @param scopeIds scope ids need to filter by.
     * @return a list of {@link Condition}.
     */
    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> scopeIds,
                                                 @Nonnull final int scopeEntityType) {
        final List<Condition> conditions = new ArrayList<>();
        if (scopeIds.isEmpty()) {
            return conditions;
        }
        final Table<?> table = getTableName();
        switch (scopeEntityType) {
            case EntityType.REGION_VALUE:
                conditions.add(table.field(REGION_ID).in(scopeIds));
                break;
            case EntityType.AVAILABILITY_ZONE_VALUE:
                conditions.add(table.field(AVAILABILITY_ZONE_ID).in(scopeIds));
                break;
            case EntityType.BUSINESS_ACCOUNT_VALUE:
                conditions.add(table.field(BUSINESS_ACCOUNT_ID).in(scopeIds));
                break;
            default:
                break;
        }
        if (startDateMillis > 0 && endDateMillis > 0) {
            conditions.add(((Field<Timestamp>)table.field(SNAPSHOT_TIME))
                    .between(new Timestamp(startDateMillis), new Timestamp(endDateMillis)));
        }
        return conditions;
    }

    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    abstract Table<?> getTableName();
}
