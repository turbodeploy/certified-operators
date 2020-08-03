package com.vmturbo.repository.search;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.derive4j.Data;
import org.derive4j.Derive;
import org.derive4j.ExportAsPublic;
import org.derive4j.Flavour;
import org.derive4j.Visibility;

import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.repository.graph.parameter.EdgeParameter.EdgeType;

/**
 * The representation of different search filters.
 *
 * @param <PH_FILTER_TYPE> A phantom type. It is only used by the compiler.
 */
@Data(flavour = Flavour.Javaslang, value = @Derive(withVisibility = Visibility.Smart))
public abstract class Filter<PH_FILTER_TYPE> implements AQLConverter {

    enum Type {
        PROPERTY, TRAVERSAL_HOP, TRAVERSAL_COND,
        TRAVERSAL_HOP_NUM_CONNECTED_VERTICES, TRAVERSAL_COND_NUM_CONNECTED_VERTICES
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
        R PropertyFilter(PropertyFilter propertyFilter);
        R TraverseHopFilter(TraversalDirection direction, int hop);
        R TraverseHopNumConnectedVerticesFilter(TraversalDirection direction, int hop, VerticesCondition verticesCondition);
        R TraverseCondFilter(TraversalDirection direction, Filter filter);
        R TraverseCondNumConnectedVerticesFilter(TraversalDirection direction, Filter filter, VerticesCondition verticesCondition);
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
                .PropertyFilter(Type.PROPERTY)
                .TraverseHopFilter(Type.TRAVERSAL_HOP)
                .TraverseHopNumConnectedVerticesFilter(Type.TRAVERSAL_HOP_NUM_CONNECTED_VERTICES)
                .TraverseCondFilter(Type.TRAVERSAL_COND)
                .TraverseCondNumConnectedVerticesFilter(Type.TRAVERSAL_COND_NUM_CONNECTED_VERTICES)
                .apply((Filter<Object>) this);
    }

    /**
     * Make copy of the filter.
     *
     * @return A new copy of the filter.
     */
    public Filter<? extends AnyFilterType> copy() {
        final Cases<Filter<? extends AnyFilterType>> cases = Filters.cases(
                Filter::propertyFilter,
                Filter::traversalHopFilter,
                Filter::traversalHopNumConnectedVerticesFilter,
                Filter::traversalCondFilter,
                Filter::traversalCondNumConnectedVerticesFilter);
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
     * Create AQL for the property filter. If there are nested properties defined though the
     * ObjectFilter, it builds AQL for nested filter recursively.
     *
     * @param propertyFilter the PropertyFilter to create AQL for
     * @param objectName name of the object which contains this property, if it is null, just use
     * the property name to check
     * @return AQL for the property filter
     */
    private static String createAQLForPropertyFilter(@Nonnull PropertyFilter propertyFilter,
            @Nullable String objectName) {
        String propertyName = propertyFilter.getPropertyName();
        // for example: if propertyName is numCpus, objectName is virtualMachineInfoRepoDTO
        // the AQL for this object should be "virtualMachineInfoRepoDTO.numCpus"
        // for list filter, the objectName can be null
        String objectNameAql = objectName == null ? propertyName : objectName + "." + propertyName;

        StringBuilder stringBuilder = new StringBuilder();
        switch (propertyFilter.getPropertyTypeCase()) {
            case OBJECT_FILTER:
                propertyFilter.getObjectFilter().getFiltersList().forEach(filter ->
                        stringBuilder.append(createAQLForPropertyFilter(filter, objectNameAql)));
                break;

            case STRING_FILTER:
                final StringFilter stringFilter = propertyFilter.getStringFilter();
                if (stringFilter.hasStringPropertyRegex()) {
                    stringBuilder.append(
                            String.format(
                                    "FILTER %s REGEX_TEST(%s, \"%s\", %s)\n",
                                    stringFilter.getPositiveMatch() ? "" : "NOT", objectNameAql,
                                    stringFilter.getStringPropertyRegex(),
                                    stringFilter.getCaseSensitive() ? "false" : "true"));
                } else {
                    // if the comparison is not case-sensitive, then the AQL function should be
                    // applied to all the strings involved
                    final Function<String, String> caseSensitivityFunction =
                            stringFilter.getCaseSensitive() ? (x->x) : (x -> "LOWER(" + x + ")");

                    final String objectNameAqlToFind = caseSensitivityFunction.apply(objectNameAql);
                    final String listOfOptions =
                            "[" +
                            stringFilter.getOptionsList().stream()
                                    .map(x -> "\"" + x + "\"")
                                    .map(caseSensitivityFunction)
                                    .collect(Collectors.joining(", ")) +
                            "]";
                    stringBuilder.append(
                        String.format(
                            "FILTER %s%s IN %s\n",
                            objectNameAqlToFind,
                            stringFilter.getPositiveMatch() ? "" : " NOT", listOfOptions));
                }
                break;

            case NUMERIC_FILTER:
                final NumericFilter numericFilter = propertyFilter.getNumericFilter();
                if (!numericFilter.hasValue()) {
                    throw new IllegalArgumentException("Numeric filter value is not set");
                }

                if (!numericFilter.hasComparisonOperator()) {
                    throw new IllegalArgumentException("Numeric filter comparison operator is not set");
                }

                if (propertyName.equals("entityType")) {
                    // translate the entityType number to a string; make sure matches entire value
                    // Match the case as well - entity type is driven by enum.
                    PropertyFilter pf = PropertyFilter.newBuilder()
                        .setPropertyName(propertyName)
                        .setStringFilter(
                            StringFilter.newBuilder()
                                .addOptions(ApiEntityType.fromType(
                                    Math.toIntExact(numericFilter.getValue())).apiStr())
                                .setCaseSensitive(true)
                                .build())
                        .build();
                    return createAQLForPropertyFilter(pf, objectName);
                }

                stringBuilder.append(String.format("FILTER %s %s %s\n",
                        objectNameAql,
                        getAqlForComparisonOperator(numericFilter.getComparisonOperator()),
                        numericFilter.getValue()));
                break;

            case MAP_FILTER:
                // map filter
                final MapFilter mapFilter = propertyFilter.getMapFilter();

                // list of values to check for exact string matches
                final List<String> values = mapFilter.getValuesList();

                // pattern for regex matching of values
                final String regex = mapFilter.hasRegex() ? mapFilter.getRegex() : "";

                // key
                final String key = mapFilter.getKey();

                // should the filter be a negation of the condition
                final boolean negation = !mapFilter.getPositiveMatch();

                // AQL condition to filter with (ignoring negation)
                final String condition;

                // construct AQL condition for (multi)-map search
                if (values.isEmpty()) {
                    if (regex.isEmpty()) {
                        // empty values list and empty regex
                        // this filter only checks for the existence of the key
                        // for example (assume the attribute name is "key" and the property name is "tags"):
                        //   "key" IN ATTRIBUTES(object.tags)
                        // in the case of negation, the filter checks for the
                        // non-existence of the key
                        condition =
                            String.format("\"%s\" IN ATTRIBUTES(%s.%s)", key, objectName, propertyName);
                    } else {
                        // empty values list and non-empty regex
                        // this filter checks for pattern matching of (one of the) value(s)
                        // under the key with the given regex

                        //   multimap: object.tags["key"] ANY =~ "^.*$"
                        if (mapFilter.getIsMultimap()) {
                            // pattern match on all the items of a list of strings
                            // the list of all matching items should be constructed first
                            // then we check if the list is empty
                            // for example assume the attribute name is "key", the property name is "tags",
                            // and the regex is "^.*$".  the condition should be
                            //   LENGTH(
                            //     FILTER object.tags["key"] != null
                            //     FOR tagValue IN object.tags["key"]
                            //     FILTER tagValue =~ "^.*$"
                            //     RETURN tagValue) == 0
                            final String allTags =
                                String.format("%s.%s[\"%s\"]", objectName, propertyName, key);
                            final String listOfMatches =
                                String.format(
                                    "FILTER %s != null FOR tagValue IN %s FILTER tagValue =~ \"%s\" RETURN tagValue",
                                    allTags, allTags, regex);
                            condition = String.format("LENGTH(%s)>0", listOfMatches);
                        } else {
                            // pattern match on a single entity
                            // for example assume the attribute name is "key", the property name is "tags",
                            // and the regex is "^.*$".  the condition should be
                            //   object.tags["key"] =~ "^.*$"
                            condition =
                                String.format(
                                    "%s.%s[\"%s\"] =~ \"%s\"",
                                    objectName, propertyName, key, regex);
                        }
                    }
                } else {
                    // non-empty values list.  regex is ignored
                    // this filter checks for exact string matching of (one of the) value(s)
                    // under the key with one of the items in the given list
                    // for example (assume the attribute name is "key", the property name is "tags",
                    // and the values list contains the strings "a" and "b"):
                    //   normal map: object.tags["key"] IN ["a", "b"]
                    //   multimap: object.tags["key"] ANY IN ["a", "b"]
                    final String valuesInQuotes =
                            values.stream().map(x -> "\"" + x + "\"").collect(Collectors.joining(", "));
                    condition =
                        String.format(
                            "%s.%s[\"%s\"] %s %s",
                            objectName, propertyName, key, mapFilter.getIsMultimap() ? "ANY IN" : "IN",
                            "[" + valuesInQuotes + "]");
                }

                // turn the condition into an AQL filter
                // add negation, if mapFilter specifies it
                stringBuilder
                        .append("FILTER ")
                        .append(negation ? "!(" : "")
                        .append(condition)
                        .append(negation ? ")" : "");
                break;

            case LIST_FILTER:
                final ListFilter listFilter = propertyFilter.getListFilter();
                switch (listFilter.getListElementTypeCase()) {
                    case OBJECT_FILTER:
                        ObjectFilter objFilter = listFilter.getObjectFilter();
                        stringBuilder.append(String.format("FILTER HAS(%s, \"%s\")\n",
                                objectName, propertyName));
                        stringBuilder.append("FILTER LENGTH(\n");
                        stringBuilder.append(String.format("FOR %s IN %s\n", propertyName, objectNameAql));
                        objFilter.getFiltersList().forEach(filter ->
                                stringBuilder.append(createAQLForPropertyFilter(filter, propertyName)));
                        stringBuilder.append("RETURN 1\n");
                        stringBuilder.append(") > 0\n");
                        break;

                    case NUMERIC_FILTER:
                        NumericFilter numericListFilter = listFilter.getNumericFilter();
                        stringBuilder.append("FILTER LENGTH(\n");
                        stringBuilder.append(String.format("FOR %s IN %s\n", propertyName, objectNameAql));
                        stringBuilder.append(createAQLForPropertyFilter(
                                PropertyFilter.newBuilder()
                                        .setPropertyName(propertyName)
                                        .setNumericFilter(numericListFilter)
                                        .build(), null));
                        stringBuilder.append("RETURN 1\n");
                        stringBuilder.append(") > 0\n");
                        break;

                    case STRING_FILTER:
                        StringFilter stringListFilter = listFilter.getStringFilter();
                        stringBuilder.append(String.format("FILTER HAS(%s, \"%s\")\n",
                                objectName, propertyName));
                        stringBuilder.append("FILTER LENGTH(\n");
                        stringBuilder.append(String.format("FOR %s IN %s\n", propertyName, objectNameAql));
                        stringBuilder.append(createAQLForPropertyFilter(
                                PropertyFilter.newBuilder()
                                        .setPropertyName(propertyName)
                                        .setStringFilter(stringListFilter)
                                        .build(), null));
                        stringBuilder.append("RETURN 1\n");
                        stringBuilder.append(") > 0\n");
                        break;

                    default:
                        throw new UnsupportedOperationException("List element type: " +
                                listFilter.getListElementTypeCase() + " is not supported");
                }
                break;
        }
        return stringBuilder.toString();
    }

    /**
     * Get the AQL string for the given ComparisonOperator.
     *
     * @param comparisonOperator the {@link ComparisonOperator} enum to get AQL for
     * @return AQL string for a specific ComparisonOperator
     */
    public static String getAqlForComparisonOperator(@Nonnull ComparisonOperator comparisonOperator) {
        final NumericOperator numOp;
        switch (comparisonOperator) {
            case EQ:
                numOp = Filter.NumericOperator.EQ;
                break;
            case NE:
                numOp = Filter.NumericOperator.NEQ;
                break;
            case GT:
                numOp = Filter.NumericOperator.GT;
                break;
            case GTE:
                numOp = Filter.NumericOperator.GTE;
                break;
            case LT:
                numOp = Filter.NumericOperator.LT;
                break;
            case LTE:
                numOp = Filter.NumericOperator.LTE;
                break;
            default:
                throw new IllegalArgumentException("ComparisonOperator: " + comparisonOperator +
                        " is not supported");
        }
        return numOp.toAQLString();
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
                (propertyFilter) -> {
                    // name of the starting object is "service_entity" by default
                    return createAQLForPropertyFilter(propertyFilter, "service_entity");
                },
                (direction, hop) -> "",
                (direction, hop, verticesCondition) -> "",
                (direction, filter) -> "",
                (direction, filter, verticesCondition) -> ""));
    }

    /**
     * Smart constructor for creating a property filter.
     *
     * @param propertyFilter The property filter the property value should match.
     *
     * @return A {@link Filter<PropertyFilterType>}.
     */
    @ExportAsPublic
    public static Filter<PropertyFilterType> propertyFilter(final PropertyFilter propertyFilter) {
        return Filters.PropertyFilter0(propertyFilter);
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

    @ExportAsPublic
    public static Filter<TraversalFilterType> traversalHopNumConnectedVerticesFilter(
            final TraversalDirection direction,
            final int hops,
            final VerticesCondition verticesCondition) {
        return Filters.TraverseHopNumConnectedVerticesFilter0(direction, hops, verticesCondition);
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

    @ExportAsPublic
    public static Filter<TraversalFilterType> traversalCondNumConnectedVerticesFilter(final TraversalDirection direction,
            final Filter<PropertyFilterType> stoppingFilter,
            final VerticesCondition verticesCondition) {
        return Filters.TraverseCondNumConnectedVerticesFilter0(direction, stoppingFilter, verticesCondition);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
