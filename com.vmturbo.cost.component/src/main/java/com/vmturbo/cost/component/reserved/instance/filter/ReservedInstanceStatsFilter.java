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


/**
 * A abstract class represent a filter object which will be used to query reserved instance stats
 * related tables.
 */
public abstract class ReservedInstanceStatsFilter extends ReservedInstanceFilter {

    protected final long startDateMillis;

    protected final long endDateMillis;

    private final List<Condition> conditions;

    protected final TimeFrame timeFrame;

    public ReservedInstanceStatsFilter(@Nonnull final Set<Long> regionIds,
                                       @Nonnull final Set<Long> availabilityZoneIds,
                                       @Nonnull final Set<Long> businessAccountIds,
                                       final long startDateMillis,
                                       final long endDateMillis,
                                       @Nullable final TimeFrame timeFrame) {
        super(regionIds, availabilityZoneIds, businessAccountIds);
        this.startDateMillis = startDateMillis;
        this.endDateMillis = endDateMillis;
        this.timeFrame = timeFrame;
        this.conditions = generateConditions(regionIds, availabilityZoneIds, businessAccountIds);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @param regionIds regions ids need to filter by.
     * @param availabilityZoneIds availability zone ids need to filter by.
     * @param businessAccountIds business account ids need to filter by.
     * @return a list of {@link Condition}.
     */
    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> regionIds,
                                              @Nonnull final Set<Long> availabilityZoneIds,
                                              @Nonnull final Set<Long> businessAccountIds) {
        final List<Condition> conditions = new ArrayList<>();
        final Table<?> table = getTableName();
        if (!regionIds.isEmpty()) {
            conditions.add(table.field(REGION_ID).in(regionIds));
        }

        if (!availabilityZoneIds.isEmpty()) {
            conditions.add(table.field(AVAILABILITY_ZONE_ID).in(availabilityZoneIds));
        }

        if (!businessAccountIds.isEmpty()) {
            conditions.add(table.field(BUSINESS_ACCOUNT_ID).in(businessAccountIds));
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
