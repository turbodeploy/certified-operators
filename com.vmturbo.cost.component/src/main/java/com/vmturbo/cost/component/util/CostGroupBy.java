package com.vmturbo.cost.component.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Field;
import org.jooq.Table;

import com.google.common.collect.Sets;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * Class to define groupBy conditions in a {@link com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest}.
 */
public class CostGroupBy {
    /**
     * {@link Tables#ENTITY_COST #CREATED_TIME} constant used in DB.
     */
    public final String CREATED_TIME = Tables.ENTITY_COST.CREATED_TIME.getName();

    private final Collection<String> groupByFields;
    private final TimeFrame timeFrame;

    /**
     * Constructor.
     *
     * @param items     items to group By. See {@link com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy}.
     * @param timeFrame {@link TimeFrame}.
     */
    public CostGroupBy(@Nonnull final Set<String> items, @Nonnull final TimeFrame timeFrame) {
        Set<String> listOfFields = Sets.newHashSet(items);
        listOfFields.add(CREATED_TIME);
        groupByFields = listOfFields.stream().map(i2 -> i2.toLowerCase(Locale.getDefault())).collect(Collectors.toSet());
        this.timeFrame = timeFrame;
    }

    /**
     * Get Fields used for groupBy.
     *
     * @return Set of Fields.
     */
    public Set<Field<?>> getGroupByFields() {
        return Arrays.stream(Tables.ENTITY_COST.fields()).filter(item -> groupByFields
                .contains(item.getName().toLowerCase(Locale.getDefault()))).collect(Collectors.toSet());
    }

    /**
     * Method which determine which table to use for fetching Entity cost.
     *
     * @return Field name of amount in the table.
     */
    public Field<? extends Number> getAmountFieldInTable() {
        return (Field<? extends Number>)getTable().field(Tables.ENTITY_COST.AMOUNT.getName());
    }

    /**
     * Method which determine which table to use for fetching Entity cost.
     *
     * @return Table to be used for storing /querying against.
     */
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
