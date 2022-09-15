package com.vmturbo.sql.utils.jooq.filter;

import java.util.Collection;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;
import org.jooq.Condition;

/**
 * A filter mapping for generating an IN condition to a collection attribute of the filter.
 * @param <FilterT> The filter type.
 * @param <AttributeTypeT> The attribute type.
 */
@Style(visibility = ImplementationVisibility.PACKAGE,
        overshadowImplementation = true,
        depluralize = true)
@Immutable
interface FilterCollectionMapping<FilterT, AttributeTypeT> extends FilterMapping<FilterT, AttributeTypeT> {

    Function<FilterT, Collection<AttributeTypeT>> filterExtractor();

    default boolean isFilterAttributeSet(@Nonnull FilterT filter) {
        return !filterExtractor().apply(filter).isEmpty();
    }

    default Collection<AttributeTypeT> extractRecordValues(@Nonnull FilterT filter) {

        return filterExtractor().apply(filter);
    }

    @Override
    default Condition generateCondition(@Nonnull FilterT filter) {
        return tableField().in(extractRecordValues(filter));
    }

    @Nonnull
    static <FilterT, AttributeTypeT> Builder<FilterT, AttributeTypeT> builder() {
        return new Builder<>();
    }

    /**
     * A builder class for constructing immutable {@link FilterCollectionMapping} instances.
     * @param <FilterT> The filter type.
     * @param <AttributeTypeT> The attribute type.
     */
    class Builder<FilterT, AttributeTypeT> extends ImmutableFilterCollectionMapping.Builder<FilterT, AttributeTypeT> {}
}
