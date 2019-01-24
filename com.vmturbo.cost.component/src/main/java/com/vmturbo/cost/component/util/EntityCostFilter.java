package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the entity cost records from the
 * {@link com.vmturbo.cost.component.entity.cost.EntityCostStore}.
 * It provider a easier way to define simple search over entity cost records
 * in the tables.
 */
public class EntityCostFilter extends CostFilter {

    private static final String CREATED_TIME = "created_time";

    private final List<Condition> conditions;

    public EntityCostFilter(final Set<Long> entityFilters,
                            final Set<Integer> entityTypeFilters,
                            final long startDateMillis,
                            final long endDateMillis,
                            @Nullable final TimeFrame timeFrame) {
        super(entityFilters, entityTypeFilters, startDateMillis, endDateMillis, timeFrame, CREATED_TIME);
        this.conditions = generateConditions();
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
            conditions.add(((Field<Timestamp>) table.field(snapshotTime))
                    .between(new Timestamp(this.startDateMillis), new Timestamp(this.endDateMillis)));
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
}
