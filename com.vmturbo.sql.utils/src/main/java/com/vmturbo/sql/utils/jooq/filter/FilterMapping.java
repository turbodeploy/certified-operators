package com.vmturbo.sql.utils.jooq.filter;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;

/**
 * A comparison of a filter attribute to a table field.
 * @param <FilterT> The filter type.
 * @param <AttributeTypeT> The attribute type being compared.
 */
interface FilterMapping<FilterT, AttributeTypeT> {

    /**
     * The table field.
     * @return The table field.
     */
    @Nonnull
    Field<AttributeTypeT> tableField();

    /**
     * Checks whether the target filter attribute is set for the provided filter.
     * @param filter The filter instance.
     * @return True, if the filter attribute is set. False otherwise.
     */
    boolean isFilterAttributeSet(@Nonnull FilterT filter);

    /**
     * Generates a condition, based on the provided filter. Note the expectation is {@link #isFilterAttributeSet(Object)}
     * has been checked prior to invoking this method.
     * @param filter The filter instance.
     * @return The generated condition.
     */
    @Nonnull
    Condition generateCondition(@Nonnull FilterT filter);
}
