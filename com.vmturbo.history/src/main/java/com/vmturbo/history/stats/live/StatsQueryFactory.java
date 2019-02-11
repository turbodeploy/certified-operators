package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.CAPACITY;
import static com.vmturbo.components.common.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.components.common.utils.StringConstants.EFFECTIVE_CAPACITY;
import static com.vmturbo.components.common.utils.StringConstants.ENVIRONMENT_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.MIN_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.PRODUCER_UUID;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.RELATION;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.dField;
import static com.vmturbo.history.db.jooq.JooqUtils.envType;
import static com.vmturbo.history.db.jooq.JooqUtils.floorDateTime;
import static com.vmturbo.history.db.jooq.JooqUtils.number;
import static com.vmturbo.history.db.jooq.JooqUtils.relation;
import static com.vmturbo.history.db.jooq.JooqUtils.str;
import static com.vmturbo.history.utils.HistoryStatsUtils.betweenStartEndTimestampCond;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Select;
import org.jooq.Table;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.utils.HistoryStatsUtils;

/**
 * A utility class to create jOOQ queries/conditions for live stats requests.
 */
public interface StatsQueryFactory {
    /**
     * Indicate an aggregation style for this query; defined in legacy.
     */
    enum AGGREGATE {NO_AGG, AVG_ALL, AVG_MIN_MAX}

    /**
     * Formulate a query string for commodities of a given entity.
     *
     * <p>note: when the stats roll-up is implemented, the time-frame must be used to iterate over
     * the different stats tables, e.g. _latest, _hourly, _daily, etc.
     *
     * @param entities the Entity OID to which the commodities belong
     * @param table the table to query.
     * @param commodityRequests a list of commodity information to gather; default is all known commodities
     * @param timeRange the time range to consider.
     * @param aggregate whether or not to aggregate results
     * @return a optional with a Jooq query ready to capture the desired stats, or empty()
     * if there is no db table for this entity type and time frame, e.g.
     * CLUSTER has no Hourly and Latest tables.
     */
    @Nonnull
    Optional<Select<?>> createStatsQuery(@Nonnull final List<String> entities,
                                         @Nonnull final Table<?> table,
                                         @Nonnull final List<CommodityRequest> commodityRequests,
                                         @Nonnull final TimeRange timeRange,
                                         @Nonnull final AGGREGATE aggregate);

    /**
     * Create a Jooq conditional clause to include only the desired commodity names.
     *
     * If commodityNames is empty, return an empty {@link Optional}
     * indicating there should be no selection condition on the commodity name. In other words,
     * all commodities will be returned.
     *
     * @param commodityRequests a list of commodity names to include in the result set, and optionally
     *                          a filter to apply to the commodity row, e.g. "relation==bought";
     *                          if there are more than commodity requests, then the filters
     *                          are 'or'ed together; an empty list implies no commodity names
     *                          filter condition at all, i.e. all commodities will be returned
     *                          TODO: Implement relatedEntity filtering
     * @param table the DB table from which these stats will be collected
     * @return an Optional containing a Jooq conditional to only include the desired commodities
     * with associated filters (if any) 'and'ed in; Optional.empty() if no commodity selection is desired
     */
    @Nonnull
    Optional<Condition> createCommodityRequestsCond(
            @Nonnull final List<CommodityRequest> commodityRequests,
            @Nonnull final Table<?> table);

    /**
     * The default implementation of {@link StatsQueryFactory} to use in production.
     */
    class DefaultStatsQueryFactory implements StatsQueryFactory {
        private final Logger logger = LogManager.getLogger();

        private final HistorydbIO historydbIO;

        public DefaultStatsQueryFactory(@Nonnull final HistorydbIO historydbIO) {
            this.historydbIO = Objects.requireNonNull(historydbIO);
        }

        @Override
        @Nonnull
        public Optional<Select<?>> createStatsQuery(@Nonnull final List<String> entities,
                                                    @Nonnull final Table<?> table,
                                                    @Nonnull final List<CommodityRequest> commodityRequests,
                                                    @Nonnull final TimeRange timeRange,
                                                    @Nonnull final AGGREGATE aggregate) {
            // check there is a table for this entityType and tFrame; and it has a SNAPSHOT_TIME column
            if (table.field(SNAPSHOT_TIME) == null) {
                return Optional.empty();
            }

            // accumulate the conditions for this query
            List<Condition> whereConditions = new ArrayList<>();

            // add where clause for time range; null if the timeframe cannot be determined
            final Condition timeRangeCondition = betweenStartEndTimestampCond(dField(table, SNAPSHOT_TIME),
                    timeRange.getTimeFrame(), timeRange.getStartTime(), timeRange.getEndTime());
            if (timeRangeCondition != null) {
                logger.debug("table {}, timeRangeCondition: {}", table.getName(), timeRangeCondition);
                whereConditions.add(timeRangeCondition);
            }

            // include an "in()" clause for uuids, if any
            if (entities.size() > 0) {
                whereConditions.add(str(dField(table, UUID)).in(entities));
            }

            // note: the legacy DB code defines expression conditions that are not used by new UI

            // add select on the given commodity reqyests; if no commodityRequests specified,
            // leave out the were clause and thereby include all commodities.
            Optional<Condition> commodityRequestsCond = createCommodityRequestsCond(commodityRequests, table);
            commodityRequestsCond.ifPresent(whereConditions::add);

            // whereConditions.add(propertyExprCond);  // TODO: implement expression conditions

            // the fields to return
            List<Field<?>> selectFields = Lists.newArrayList(
                    floorDateTime(dField(table, SNAPSHOT_TIME), timeRange.getTimeFrame()).as(SNAPSHOT_TIME),
                    dField(table, PROPERTY_TYPE),
                    dField(table, PROPERTY_SUBTYPE),
                    dField(table, PRODUCER_UUID),
                    dField(table, CAPACITY),
                    dField(table, EFFECTIVE_CAPACITY),
                    dField(table, RELATION),
                    dField(table, COMMODITY_KEY));

            // the fields to order by and group by
            Field<?>[] orderGroupFields = new Field<?>[]{
                    dField(table, SNAPSHOT_TIME),
                    dField(table, UUID),
                    dField(table, PROPERTY_TYPE),
                    dField(table, PROPERTY_SUBTYPE),
                    dField(table, RELATION)
            };

            Select<?> statsQueryString;
            switch (aggregate) {
                case NO_AGG:
                    selectFields.add(0, dField(table, UUID));
                    selectFields.add(0, dField(table, AVG_VALUE));
                    selectFields.add(0, dField(table, MIN_VALUE));
                    selectFields.add(0, dField(table, MAX_VALUE));

                    statsQueryString = historydbIO.getStatsSelect(table, selectFields, whereConditions,
                            orderGroupFields);
                    break;

                case AVG_ALL:
                    selectFields.add(0, avg(number(dField(table, AVG_VALUE))).as(AVG_VALUE));
                    selectFields.add(0, avg(number(dField(table, MIN_VALUE))).as(MIN_VALUE));
                    selectFields.add(0, avg(number(dField(table, MAX_VALUE))).as(MAX_VALUE));

                    statsQueryString = historydbIO.getStatsSelectWithGrouping(table, selectFields,
                            whereConditions, orderGroupFields);
                    break;

                case AVG_MIN_MAX:
                    selectFields.add(0, avg(number(dField(table, AVG_VALUE))).as(AVG_VALUE));
                    selectFields.add(0, min(number(dField(table, MIN_VALUE))).as(MIN_VALUE));
                    selectFields.add(0, max(number(dField(table, MAX_VALUE))).as(MAX_VALUE));

                    statsQueryString = historydbIO.getStatsSelectWithGrouping(table, selectFields,
                            whereConditions, orderGroupFields);
                    break;

                default:
                    throw new IllegalArgumentException("Illegal value for AGG: " + aggregate);
            }
            return Optional.of(statsQueryString);
        }

        @Override
        @Nonnull
        public Optional<Condition> createCommodityRequestsCond(
                @Nonnull final List<CommodityRequest> commodityRequests,
                @Nonnull final Table<?> table) {
            if (commodityRequests.isEmpty()) {
                return Optional.empty();
            }
            Condition commodityTests = null;
            for (CommodityRequest commodityRequest : commodityRequests) {
                // create a conditional for this commodity
                Condition commodityTest = str(dField(table, PROPERTY_TYPE))
                        .eq(commodityRequest.getCommodityName());
                // add an 'and' for each property value filter specified
                for (PropertyValueFilter propertyValueFilter : commodityRequest.getPropertyValueFilterList()) {
                    // add a relationType filter if specified
                    switch (propertyValueFilter.getProperty()) {
                        case RELATION:
                            // 'bought/sold' are represented in the DB by integers, so we need to map here
                            RelationType desiredRelation = RelationType.getApiRelationType(
                                    propertyValueFilter.getValue());
                            commodityTest = commodityTest.and(relation(dField(table, RELATION))
                                    .eq(desiredRelation));
                            break;
                        case ENVIRONMENT_TYPE:
                            // We only record the environment type in the database for the aggregate
                            // market stats tables.
                            if (HistoryStatsUtils.isMarketStatsTable(table)) {
                                final Optional<EnvironmentType> envType = UIEnvironmentType.fromString(
                                    propertyValueFilter.getValue()).toEnvType();
                                if (envType.isPresent()) {
                                    commodityTest = commodityTest.and(
                                        envType(dField(table, MarketStatsLatest.MARKET_STATS_LATEST.ENVIRONMENT_TYPE.getName())).
                                            eq(envType.get()));
                                }
                            } else {
                                // For "regular" tables we rely on the API component to only
                                // target the entities in the proper environment type.
                                logger.debug("Ignoring environment type filter (value: {}) for " +
                                    "non-market table {}",
                                    propertyValueFilter.getValue(), table.getName());
                            }
                            break;
                        default:
                            // default is to use 'property' as column name and perform a string match
                            commodityTest = commodityTest.and(
                                    str(dField(table, propertyValueFilter.getProperty()))
                                            .eq(propertyValueFilter.getValue()));
                    }
                }
                // construct the "or" of all the different commodityTests
                commodityTests = commodityTests == null
                        ? commodityTest
                        : commodityTests.or(commodityTest);
            }
            return Optional.of(commodityTests);
        }
    }
}
