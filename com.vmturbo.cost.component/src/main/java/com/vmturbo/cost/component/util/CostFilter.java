package com.vmturbo.cost.component.util;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Table;

import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator.TimeFrame;

/**
 * A abstract class represents the filter object.
 * Current it's only used with entity cost tables.
 */
public abstract class CostFilter {
    public static final String ASSOCIATED_ENTITY_TYPE = "associated_entity_type";
    protected final long startDateMillis;
    protected final long endDateMillis;
    private final Set<Long> entityTypeFilters;
    protected TimeFrame timeFrame;
    protected String snapshotTime;


    public CostFilter(@Nonnull final Set<Long> entityTypeFilters,
                      final long startDateMillis,
                      final long endDateMillis,
                      @Nullable final TimeFrame timeFrame,
                      @Nonnull String snapshotTime) {
        this.startDateMillis = startDateMillis;
        this.endDateMillis = endDateMillis;
        this.timeFrame = timeFrame;
        this.snapshotTime = snapshotTime;
        this.entityTypeFilters = entityTypeFilters;

    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @param startDateMillis
     * @param endDateMillis
     * @param entityTypeFilters
     * @return a list of {@link Condition}.
     */
    abstract public List<Condition> generateConditions(final long startDateMillis,
                                                       final long endDateMillis,
                                                       final Set<Long> entityTypeFilters);

    abstract public Condition[] getConditions();

    abstract Table<?> getTable();
}
