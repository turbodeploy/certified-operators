package com.vmturbo.repository.search;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.derive4j.Data;
import org.derive4j.Derive;
import org.derive4j.ExportAsPublic;
import org.derive4j.Flavour;
import org.derive4j.Visibility;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.repository.graph.parameter.EdgeParameter.EdgeType;

/**
 * The representation of different search filters.
 *
 * @param <PH_FILTER_TYPE> A phantom type. It is only used by the compiler.
 */
@Data(flavour = Flavour.Javaslang, value = @Derive(withVisibility = Visibility.Smart))
public abstract class Filter<PH_FILTER_TYPE> implements AQLConverter {

    enum Type {
        PROPERTY_STRING, PROPERTY_NUMERIC, PROPERTY_MAP, TRAVERSAL_HOP, TRAVERSAL_COND
    }

    enum NumericOperator implements AQLConverter {
        EQ {
            @Override
            public String toAQLString() {
                return "==";
            }
        },

        NEQ {
            @Override
            public String toAQLString() {
                return "!=";
            }
        },

        LT {
            @Override
            public String toAQLString() {
                return "<";
            }
        },

        LTE {
            @Override
            public String toAQLString() {
                return "<=";
            }
        },

        GT {
            @Override
            public String toAQLString() {
                return ">";
            }
        },

        GTE {
            @Override
            public String toAQLString() {
                return ">=";
            }
        }
    }

    enum TraversalDirection implements AQLConverter {
        PROVIDER(EdgeType.CONSUMES) {
            @Override
            public String toAQLString() {
                return "INBOUND";
            }
        },

        CONSUMER(EdgeType.CONSUMES) {
            @Override
            public String toAQLString() {
                return "OUTBOUND";
            }
        },

        CONNECTED_TO(EdgeType.CONNECTED) {
            @Override
            public String toAQLString() {
                return "INBOUND";
            }
        },

        CONNECTED_FROM(EdgeType.CONNECTED) {
            @Override
            public String toAQLString() {
                return "OUTBOUND";
            }
        };

        private EdgeType edgeType;

        TraversalDirection(@Nonnull EdgeType edgeType) {
            this.edgeType = edgeType;
        }

        public String getEdgeType() {
            return edgeType.name();
        }
    }

    interface Cases<R> {
        R StringPropertyFilter(String propName, StringFilter stringFilter);
        R NumericPropertyFilter(String numPropName, NumericOperator numOp, Number numValue);
        R MapPropertyFilter(String mapPropName, String key, List<String> values, boolean multi);
        R TraverseHopFilter(TraversalDirection direction, int hop);
        R TraverseCondFilter(TraversalDirection direction, Filter filter);
    }

    public abstract <R> R match(Cases<R> cases);

    /**
     * Return type of the filter.
     *
     * @return The filter's {@link Type}.
     */
    @SuppressWarnings("unchecked")
    public Type getType() {
        return Filters.cases()
                .StringPropertyFilter(Type.PROPERTY_STRING)
                .NumericPropertyFilter(Type.PROPERTY_NUMERIC)
                .MapPropertyFilter(Type.PROPERTY_MAP)
                .TraverseHopFilter(Type.TRAVERSAL_HOP)
                .TraverseCondFilter(Type.TRAVERSAL_COND)
                .apply((Filter<Object>) this);
    }

    /**
     * Make copy of the filter.
     *
     * @return A new copy of the filter.
     */
    public Filter<? extends AnyFilterType> copy() {
        final Cases<Filter<? extends AnyFilterType>> cases = Filters.cases(
                Filter::stringPropertyFilter,
                Filter::numericPropertyFilter,
                Filter::mapPropertyFilter,
                Filter::traversalHopFilter,
                Filter::traversalCondFilter);
        return this.match(cases);
    }

    /**
     * Widen the current {@link Filter} which can be parameterized as {@link PropertyFilterType} or
     * {@link TraversalFilterType} to a {@link Filter} that is parameterized to {@link AnyFilterType}.
     *
     * @return A {@link Filter} with a bounded type parameter.
     */
    @SuppressWarnings("unchecked")
    public Filter<? extends AnyFilterType> widen() {
        return (Filter<? extends AnyFilterType>) this;
    }

    /**
     * Return the AQL for this filter.
     *
     * @return The AQL string.
     */
    @Override
    public String toAQLString() {
        // TODO (here and elsewhere): SANITIZE the input before inserting it into the query (OM-38634)
        return this.match(Filters.cases(
                (pName, stringFilter) -> {
                    // See: https://docs.arangodb.com/devel/AQL/Functions/String.html#regextest
                    return String.format("FILTER %s REGEX_TEST(service_entity.%s, \"%s\", %s)",
                            stringFilter.getMatch() ? "" : "NOT",
                            pName,
                            stringFilter.getStringPropertyRegex(),
                            stringFilter.getCaseSensitive() ? "false" : "true");
                },
                (pName, numOp, numVal) ->
                    String.format("FILTER service_entity.%s %s %s", pName, numOp.toAQLString(), numVal),
                (pName, key, values, multi) -> {
                    // construct AQL filter for (multi)-map search
                    // the value filter depends on whether this is a map or a multimap
                    // the value in a map entry is a single string, while in multimap is an array
                    if (values == null || values.isEmpty()) {
                        return String.format("FILTER \"%s\" IN ATTRIBUTES(service_entity.%s)", key, pName);
                    } else {
                        // turn the list of values to an AQL string representation of that list
                        final String valuesInQuotes =
                                values.stream().map(x -> "\"" + x + "\"").collect(Collectors.joining(", "));
                        return String.format(
                                "FILTER service_entity.%s[\"%s\"] %s %s",
                                pName,
                                key,
                                multi ? "ANY IN" : "IN",
                                "[" + valuesInQuotes + "]"
                        );
                    }
                },
                (direction, hop) -> "",
                (direction, filter) -> ""));
    }

    /**
     * Smart constructor for creating a string property filter.
     *
     * @param propName The property name.
     * @param stringFilter The string filter the property value should match.
     *
     * @return A {@link Filter<PropertyFilterType>}.
     */
    @ExportAsPublic
    public static Filter<PropertyFilterType> stringPropertyFilter(final String propName,
                                                                  final StringFilter stringFilter) {
        return Filters.StringPropertyFilter0(propName, stringFilter);
    }

    /**
     * Smart constructor for creating a multimap property filter.
     *
     * @param propName The property name.
     * @param key The key.
     * @param values The values.
     * @param multi iff this is a multi-map.
     *
     * @return A {@link Filter<PropertyFilterType>}.
     */
    @ExportAsPublic
    public static Filter<PropertyFilterType> mapPropertyFilter(
            final String propName,
            final String key,
            final List<String> values,
            final boolean multi) {
        return Filters.MapPropertyFilter0(propName, key, values, multi);
    }

    /**
     * Smart constructor for creating a numeric property filter.
     *
     * @param propName The property name.
     * @param op The numeric operator.
     * @param value The value for the operator.
     *
     * @return A {@link Filter<PropertyFilterType>}.
     */
    @ExportAsPublic
    public static Filter<PropertyFilterType> numericPropertyFilter(final String propName,
                                                                   final NumericOperator op,
                                                                   final Number value) {
        return Filters.NumericPropertyFilter0(propName, op, value);
    }

    /**
     * Smart constructor for creating a hop based traversal filter.
     *
     * @param direction The traversal direction.
     * @param hops The number of hops.
     *
     * @return A {@link Filter<TraversalFilterType>}.
     */
    @ExportAsPublic
    public static Filter<TraversalFilterType> traversalHopFilter(final TraversalDirection direction,
                                                                 final int hops) {
        return Filters.TraverseHopFilter0(direction, hops);
    }

    /**
     * Smart constructor for creating a condition based traversal filter.
     *
     * @param direction The traversal direction.
     * @param stoppingFilter The stopping condition for the traversal.
     *
     * @return A {@link Filter<PropertyFilterType>}.
     */
    @ExportAsPublic
    public static Filter<TraversalFilterType> traversalCondFilter(final TraversalDirection direction,
                                                                  final Filter<PropertyFilterType> stoppingFilter) {
        return Filters.TraverseCondFilter0(direction, stoppingFilter);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
