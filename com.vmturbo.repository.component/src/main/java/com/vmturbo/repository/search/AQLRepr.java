package com.vmturbo.repository.search;

import com.github.jknack.handlebars.Template;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.AQLs;
import javaslang.collection.List;
import javaslang.control.Option;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnull;

/**
 * A Java representation of AQL.
 */
public class AQLRepr implements Iterable<Filter<? extends AnyFilterType>> {

    private static final Logger LOG = LoggerFactory.getLogger(AQLRepr.class);

    private static final Set<Filter.Type> PROPERTY_FILTER_TYPE =
            ImmutableSet.of(Filter.Type.PROPERTY_NUMERIC, Filter.Type.PROPERTY_STRING);

    private final List<Filter<? extends AnyFilterType>> filters;

    private final Optional<AQLPagination> pagination;

    public AQLRepr(@Nonnull final List<Filter<? extends AnyFilterType>> filtersArg) {
        checkArgument(filtersArg.tail().forAll(f -> PROPERTY_FILTER_TYPE.contains(f.getType())),
                "Only the first filter can contain a traversal filter");
        this.filters = Objects.requireNonNull(filtersArg);
        this.pagination = Optional.empty();
    }

    public AQLRepr(@Nonnull final List<Filter<? extends AnyFilterType>> filtersArg,
                   @Nonnull final AQLPagination pagination) {
        checkArgument(filtersArg.tail().forAll(f -> PROPERTY_FILTER_TYPE.contains(f.getType())),
                "Only the first filter can contain a traversal filter");
        this.filters = Objects.requireNonNull(filtersArg);
        this.pagination = Optional.of(pagination);
    }

    public List<Filter<? extends AnyFilterType>> getFilters() {
        return filters;
    }

    @Override
    public Iterator<Filter<? extends AnyFilterType>> iterator() {
        return filters.iterator();
    }

    public AQL toAQL() {
        return filters.headOption().map(firstFilter -> {
            final AQL aql = firstFilter.<AQL>match(Filters.cases(
                    (strPropName, strOp, strValue) -> constructPropertyAQL(firstFilter),
                    (numPropName, numOp, numValue) -> constructPropertyAQL(firstFilter),
                    this::constructTraversalHopAQL,
                    this::constructTraversalCondAQL));

            LOG.debug("Converted to AQL - {}", AQLs.getQuery(aql));

            return aql;
        })
        // `filters` list is empty, return an empty string.
        .getOrElse(AQLs.of("", Collections.emptyList()));
    }

    /**
     * Convert the {@link #filters} using the property filter template.
     *
     * @param firstFilter The first filter in {@link #filters}.
     *
     * @return The AQL of all {@link #filters}.
     */
    private AQL constructPropertyAQL(final Filter<? extends AnyFilterType> firstFilter) {
        final Template template = AQLTemplate.templateMapper.get(firstFilter.getType());
        final Collection<String> bindVars = AQLTemplate.bindVarsMapper.get(firstFilter.getType());

        final Map<String, Object> ctx = ImmutableMap.of(
                "filters", filters.map(f -> ImmutableMap.of("filter", f.toAQLString())),
                "pagination", pagination.map(AQLPagination::toAQLString).orElse(""));

        return AQLs.of(applyTemplate(template, ctx).getOrElse(""), bindVars);
    }

    /**
     * Convert the {@link #filters} using the traversal hop template.
     *
     * @param direction The direction of traversal.
     * @param hops Num of hops.
     * @return The AQL of all {@link #filters}.
     */
    private AQL constructTraversalHopAQL(final Filter.TraversalDirection direction, final int hops) {
        final Template template = AQLTemplate.templateMapper.get(Filter.Type.TRAVERSAL_HOP);
        final Collection<String> bindVars = AQLTemplate.bindVarsMapper.get(Filter.Type.TRAVERSAL_HOP);
        final Map<String, Object> ctx = ImmutableMap.of(
                "direction", direction.toAQLString(),
                "hops", hops,
                "filters", filters.map(f -> ImmutableMap.of("filter", f.toAQLString())),
                "pagination", pagination.map(AQLPagination::toAQLString).orElse(""));

        return AQLs.of(applyTemplate(template, ctx).getOrElse(""), bindVars);
    }

    /**
     * Convert the {@link #filters} using the traversal condition template.
     *
     * @param direction The direction of traversal.
     * @param condition The condition we are searching for during traversal.
     * @return The AQL of all {@link #filters}
     */
    private AQL constructTraversalCondAQL(final Filter.TraversalDirection direction,
                                          final Filter<PropertyFilterType> condition) {
        final Template template = AQLTemplate.templateMapper.get(Filter.Type.TRAVERSAL_COND);
        final Collection<String> bindVars = AQLTemplate.bindVarsMapper.get(Filter.Type.TRAVERSAL_COND);
        final Map<String, Object> ctx = ImmutableMap.of(
                "direction", direction.toAQLString(),
                "condition", condition.toAQLString(),
                "filters", filters.map(f -> ImmutableMap.of("filter", f.toAQLString())),
                "pagination", pagination.map(AQLPagination::toAQLString).orElse(""));
        return AQLs.of(applyTemplate(template, ctx).getOrElse(""), bindVars);
    }

    /**
     * Apply the given {@link Template} with the given context.
     *
     * @param template The template we want to apply.
     * @param context The context for the template.
     * @return The AQL or <code>none()</code>.
     */
    private Option<String> applyTemplate(final Template template,
                                         final Map<String, Object> context) {
        try {
            final String result = template.apply(context);
            return Option.of(result);
        } catch (IOException e) {
            LOG.error("Exception while applying AQL template with ctx" + context, e);
            return Option.none();
        }
    }

    /**
     * A convenient method to create a {@link AQLRepr} from a array of {@link Filter}.
     *
     * @param filters The array of {@link Filter}.
     * @return A {@link AQLRepr}.
     */
    public static AQLRepr fromFilters(final Filter<? extends AnyFilterType>... filters) {
        return new AQLRepr(List.of(filters));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AQLRepr)) return false;
        AQLRepr filters1 = (AQLRepr) o;
        return Objects.equals(filters, filters1.filters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filters);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("filters", filters)
                .toString();
    }

    /**
     * A helper class to define AQL pagination parameters. And it will be converted to LIMIT and
     * SORT query.
     */
    public static class AQLPagination implements AQLConverter {
        private static final Logger logger = LoggerFactory.getLogger(AQLPagination.class);

        private final String ASC = "ASC";

        private final String DESC = "DESC";

        private final String DISPLAY_NAME = "displayName";

        private final SearchOrderBy searchOrderby;

        private final boolean isAscending;

        private final long skipNum;

        private final Optional<Long >limitNum;

        public AQLPagination (final SearchOrderBy searchOrderby,
                           final boolean isAscending,
                           final long skipNum,
                           final Optional<Long> limitNum) {
            this.searchOrderby = searchOrderby;
            this.isAscending = isAscending;
            this.skipNum = skipNum;
            this.limitNum = limitNum;
        }

        @Override
        public String toAQLString() {
            final String sortOrder = this.isAscending ? ASC : DESC;
            final String orderByFieldName = getOrderByFieldName(searchOrderby);
            final String sortStr = String.format("SORT service_entity.%s %s\n", orderByFieldName, sortOrder);
            final String limitStr = limitNum.isPresent()
                    ? String.format("LIMIT %d,%d", skipNum, limitNum.get())
                    : "";
            return sortStr + limitStr;
        }

        private String getOrderByFieldName(@Nonnull final SearchOrderBy searchOrderBy) {
            if (searchOrderBy.equals(SearchOrderBy.ENTITY_NAME)) {
                return DISPLAY_NAME;
            } else {
                // search order by utilization and cost are not implemented yet, for now it will be also
                // set to entity name.
                // TODO: Implement search order by utilization and cost.
                logger.warn("Search query sort by {} is not implemented yet.", searchOrderBy);
                return DISPLAY_NAME;
            }
        }
    }
}
