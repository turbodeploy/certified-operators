package com.vmturbo.sql.utils.jooq.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Internal.EnumLite;

import org.jooq.Comparator;
import org.jooq.Condition;
import org.jooq.Field;

/**
 * Helper class for creating jOOQ {@link Condition}s from a filter of type {@link FilterT}, based on preconfigured
 * mappings between filter attributes and table fields.
 * @param <FilterT> The data filter type.
 */
public class JooqFilterMapper<FilterT> {

    private final List<FilterMapping<FilterT, ?>> filterMappings;

    private JooqFilterMapper(@Nonnull Builder<FilterT> builder) {
        this.filterMappings = ImmutableList.copyOf(builder.filterMappings);
    }

    /**
     * Generates conditions, based on the provided {@code filter}. The linking of filter attributes
     * to table fields are preconfigured as part of building this {@link JooqFilterMapper} instance.
     * @param filter The filter.
     * @return The list of filter conditions.
     */
    @Nonnull
    public List<Condition> generateConditions(@Nonnull FilterT filter) {

        final ImmutableList.Builder<Condition> conditions = ImmutableList.builder();

        filterMappings.forEach(filterMapping -> {

            if (filterMapping.isFilterAttributeSet(filter)) {
                conditions.add(filterMapping.generateCondition(filter));
            }
        });

        return conditions.build();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @param <FilterTypeT> The filter type.
     * @return The newly constructed {@link Builder}.
     */
    @Nonnull
    public static <FilterTypeT> Builder<FilterTypeT> builder() {
        return new Builder<>();
    }

    /**
     * A builder class for constructing {@link JooqFilterMapper} instances.
     * @param <FilterTypeT> The filter type.
     */
    public static class Builder<FilterTypeT> {

        private final List<FilterMapping<FilterTypeT, ?>> filterMappings = new ArrayList<>();

        /**
         * Adds a condition to check that the target {@code tableField} is contained IN the provided
         * collection of the filter, extracted through {@code filterExtractor}. If the filter collection
         * is empty, no condition will be generated.
         * @param filterExtractor The attribute extractor from a filter instance.
         * @param tableField The target table field to compare to the filter attribute.
         * @param <AttributeTypeT> The attribute type.
         * @return This builder instance for method chaining.
         */
        @Nonnull
        public <AttributeTypeT> Builder<FilterTypeT> addInCollection(
                @Nonnull Function<FilterTypeT, Collection<AttributeTypeT>> filterExtractor,
                @Nonnull Field<AttributeTypeT> tableField) {

            filterMappings.add(FilterCollectionMapping.<FilterTypeT, AttributeTypeT>builder()
                    .filterExtractor(filterExtractor)
                    .tableField(tableField)
                    .build());
            return this;
        }

        /**
         * Similar to {@link #addInCollection(Function, Field)}, except this method handles mapping
         * of a protobuf {@link EnumLite} to a {@link Short} as the assumed database column type.
         * @param filterExtractor The filter extractor, expected to produce a collection of {@link EnumLite}
         * values.
         * @param tableField The table field.
         * @param <EnumTypeT> The enum type.
         * @return This builder instance for method chaining.
         */
        @Nonnull
        public <EnumTypeT extends EnumLite> Builder<FilterTypeT> addProtoEnumInShortCollection(
                @Nonnull Function<FilterTypeT, Collection<EnumTypeT>> filterExtractor,
                @Nonnull Field<Short> tableField) {

            filterMappings.add(FilterCollectionMapping.<FilterTypeT, Short>builder()
                    .filterExtractor(filterExtractor.andThen((filterEnumVals) -> filterEnumVals.stream()
                            .map((filterEnumVal) -> (short)filterEnumVal.getNumber())
                            .collect(ImmutableList.toImmutableList())))
                    .tableField(tableField)
                    .build());

            return this;
        }

        /**
         * Similar to {@link #addInCollection(Function, Field)}, except this method handles mapping
         * of an {@link Enum} to a {@link Short} as the assumed database column type.
         * @param filterExtractor The filter extractor, expected to produce a collection of {@link EnumLite}
         * values.
         * @param tableField The table field.
         * @param <EnumTypeT> The enum type.
         * @return This builder instance for method chaining.
         */
        @Nonnull
        public <EnumTypeT extends Enum<EnumTypeT>> Builder<FilterTypeT> addEnumInShortCollection(
                @Nonnull Function<FilterTypeT, Collection<EnumTypeT>> filterExtractor,
                @Nonnull Field<Short> tableField) {

            filterMappings.add(FilterCollectionMapping.<FilterTypeT, Short>builder()
                    .filterExtractor(filterExtractor.andThen((filterEnumVals) -> filterEnumVals.stream()
                            .map((filterEnumVal) -> (short)filterEnumVal.ordinal())
                            .collect(ImmutableList.toImmutableList())))
                    .tableField(tableField)
                    .build());

            return this;
        }

        /**
         * Adds a comparison (through {@code comparator}) of a single filter value to a table field.
         * The mapping created through this method will create {@code tableField} {@code comparator} field attributes.
         *
         * @param filterPredicate Filter predicate indicating whether the filter attribute is set. If
         * this predicate returns false, no {@link Condition} will be generated for this mapping.
         * @param filterExtractor The filter attribute extractor.
         * @param tableField The table field.
         * @param comparator The comparator between the filter and table field.
         * @param <AttributeTypeT> The attribute type.
         * @return This builder instance for method chaining.
         */
        @Nonnull
        public <AttributeTypeT> Builder<FilterTypeT> addValueComparison(
                @Nonnull Predicate<FilterTypeT> filterPredicate,
                @Nonnull Function<FilterTypeT, AttributeTypeT> filterExtractor,
                @Nonnull Field<AttributeTypeT> tableField,
                @Nonnull Comparator comparator) {

            return addValueComparison(filterPredicate, filterExtractor, Function.identity(), tableField, comparator);
        }

        /**
         * Similar to {@link #addValueComparison(Predicate, Function, Field, Comparator)}, expect this method supports
         * an additional {@code attributeTransformer} for translating between the filter attribute type and the table
         * field type.
         * @param filterPredicate The filter predicate, checking whether the attribute of the filter is set.
         * @param filterExtractor The filter attribute extractor.
         * @param attributeTransformer The attribute transformer between the filter attribute type and the
         * table field type.
         * @param tableField The table field.
         * @param comparator The comparator between the filter attribute and the table field.
         * @param <FilterAttributeT> The filter attribute type.
         * @param <RecordAttributeT> The table field type.
         * @return This builder instance for method chaining.
         */
        public <FilterAttributeT, RecordAttributeT> Builder<FilterTypeT> addValueComparison(
                @Nonnull Predicate<FilterTypeT> filterPredicate,
                @Nonnull Function<FilterTypeT, FilterAttributeT> filterExtractor,
                @Nonnull Function<FilterAttributeT, RecordAttributeT> attributeTransformer,
                @Nonnull Field<RecordAttributeT> tableField,
                @Nonnull Comparator comparator) {


            filterMappings.add(FilterValueMapping.<FilterTypeT, RecordAttributeT>builder()
                    .filterPredicate(filterPredicate)
                    .filterExtractor(filterExtractor.andThen(attributeTransformer))
                    .tableField(tableField)
                    .comparator(comparator)
                    .build());

            return this;
        }

        /**
         * Builds a {@link JooqFilterMapper} instance based on the configured mappings between a filter object and
         * jOOQ table fields.
         * @return The newly built filter mapper instance.
         */
        @Nonnull
        public JooqFilterMapper<FilterTypeT> build() {
            return new JooqFilterMapper<>(this);
        }
    }
}
