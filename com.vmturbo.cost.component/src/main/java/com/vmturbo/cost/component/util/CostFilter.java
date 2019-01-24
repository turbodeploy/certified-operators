package com.vmturbo.cost.component.util;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Table;

import com.vmturbo.components.api.TimeFrameCalculator.TimeFrame;


/**
 * A abstract class represents the filter object.
 * Current it's only used with entity and expense cost tables.
 * TODO there is discussion to remove this abstract class.
 * See https://rbcommons.com/s/VMTurbo/r/26851/.
 */
public abstract class CostFilter {

    protected final long startDateMillis;
    protected final long endDateMillis;
    protected final Set<Integer> entityTypeFilters;
    protected final Set<Long> entityFilters;
    protected TimeFrame timeFrame;
    protected String snapshotTime;


    public CostFilter(@Nonnull final Set<Long> entityFilters,
                      @Nonnull final Set<Integer> entityTypeFilters,
                      final long startDateMillis,
                      final long endDateMillis,
                      @Nullable final TimeFrame timeFrame,
                      @Nonnull String snapshotTime) {
        this.startDateMillis = startDateMillis;
        this.endDateMillis = endDateMillis;
        this.timeFrame = timeFrame;
        this.snapshotTime = snapshotTime;
        this.entityFilters = entityFilters;
        this.entityTypeFilters = entityTypeFilters;
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    abstract public List<Condition> generateConditions();

    abstract public Condition[] getConditions();

    public abstract Table<?> getTable();
}
