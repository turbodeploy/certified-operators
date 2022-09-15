package com.vmturbo.sql.utils.jooq.filter;

import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;
import org.jooq.Comparator;
import org.jooq.Condition;

/**
 * A filter mapping between a single value of a filter instance and a table field through a configurable
 * {@link Comparator}.
 * @param <FilterT> The filter type.
 * @param <AttributeTypeT> The attribute type of both the filter and table field.
 */
@Style(visibility = ImplementationVisibility.PACKAGE,
        overshadowImplementation = true,
        depluralize = true)
@Immutable
interface FilterValueMapping<FilterT, AttributeTypeT> extends FilterMapping<FilterT, AttributeTypeT> {

    Predicate<FilterT> filterPredicate();

    Function<FilterT, AttributeTypeT> filterExtractor();

    Comparator comparator();

    @Override
    default boolean isFilterAttributeSet(@Nonnull FilterT filter) {
        return filterPredicate().test(filter);
    }

    @Override
    default Condition generateCondition(@Nonnull FilterT filter) {
        return tableField().compare(comparator(), filterExtractor().apply((filter)));
    }

    @Nonnull
    static <FilterT, AttributeTypeT> Builder<FilterT, AttributeTypeT> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for constructing immutable {@link FilterValueMapping} instances.
     * @param <FilterT> The filter type.
     * @param <AttributeTypeT> The attribute type.
     */
    class Builder<FilterT, AttributeTypeT> extends ImmutableFilterValueMapping.Builder<FilterT, AttributeTypeT> {}
}
