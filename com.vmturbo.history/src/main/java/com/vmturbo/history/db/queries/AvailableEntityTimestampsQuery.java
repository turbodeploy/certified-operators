package com.vmturbo.history.db.queries;

import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.tables.MarketStatsByMonth.MARKET_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest.MARKET_STATS_LATEST;

import java.sql.Timestamp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SortOrder;
import org.jooq.Table;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.QueryBase;

/**
 * This class defines a query to retrieve snapshot_time values of available entity_stats data.
 *
 * <p>The query can be constrained by a variety of factors:</p>
 * <ul>
 *     <li>Timeframe (latest, hourly, daily, monthly)</li>
 *     <li>Entity type</li>
 *     <li>Entity OID, if entity type is provided</li>
 *     <li>Upper and/or lower time bound</li>
 *     <li>Property types that must/must not appear</li>
 * </ul>
 */
public class AvailableEntityTimestampsQuery extends QueryBase {

    /**
     * Create a new query instance.
     *
     * @param timeFrame         required {@link TimeFrame}
     * @param entityType        required {@link EntityType}
     * @param entityOid         required entity OID (only if entityType is provided)
     * @param limit             max number of returned values, or 0 for no limit
     * @param fromInclusive     inclusive lower bound on returned timestamps
     * @param toInclusive       inclusive upper bound on returned timestamps
     * @param excludeProperties true if listed properties must not appear, else they must appear
     * @param propertyTypes     property types that must/must not appear in stats records considered
     */
    public AvailableEntityTimestampsQuery(@Nonnull TimeFrame timeFrame,
            @Nullable EntityType entityType,
            @Nullable String entityOid,
            int limit,
            @Nullable Timestamp fromInclusive,
            @Nullable Timestamp toInclusive,
            boolean excludeProperties,
            String... propertyTypes) {
        Table<? extends Record> entityTable = entityType != null
                ? entityType.getTimeFrameTable(timeFrame)
                // This is a little goofy because market stats tables are not entity tables.
                // It's like this now to support existing usage, but this deserves some later redesign
                : getMarketStatsTimeFrameTable(timeFrame);
        Field<Timestamp> snapshotTimeField = getTimestampField(entityTable, StringConstants.SNAPSHOT_TIME);
        if (limit != 1) {
            // if we're getting more than one snapshot, filter out duplicates
            setDistinct();
        }
        addSelectFields(snapshotTimeField);
        addTable(entityTable);
        addFieldRangeCondition(snapshotTimeField, fromInclusive, toInclusive);
        if (propertyTypes.length > 0) {
            Field<String> propertyTypeField = getStringField(entityTable, StringConstants.PROPERTY_TYPE);
            addConditions(excludeProperties
                    ? propertyTypeField.notIn(propertyTypes)
                    : propertyTypeField.in(propertyTypes));
        }
        if (entityType != null && entityOid != null) {
            Field<String> uuidField = getStringField(entityTable, StringConstants.UUID);
            addConditions(uuidField.eq(entityOid));
            // make sure we use the uuid index when we're given an oid; a bad index choice
            // here can kill this query!
            // TODO: reinstate this hint after entity stats table anomalies have been fixed
            // (ref: OM-55219)
//            forceIndex(entityTable, StringConstants.UUID);
        }
        orderBy(snapshotTimeField, SortOrder.DESC);
        limit(limit);
    }

    /**
     * Get the market_stats table for a given time frame.
     *
     * <p>This is needed because market stats is not an entity type, so we can't use the methods of
     * {@link EntityType} to do this, but we do use market stats tables as the default tables to
     * search when entity type is not provided. As noted above, it's a little crummy as a design.
     * </p>
     *
     * @param timeFrame time frame of desired table
     * @return market stats table corresponding to time frame
     */
    private static Table<?> getMarketStatsTimeFrameTable(TimeFrame timeFrame) {
        switch (timeFrame) {
            case LATEST:
                return MARKET_STATS_LATEST;
            case HOUR:
                return MARKET_STATS_BY_HOUR;
            case DAY:
                return MARKET_STATS_BY_DAY;
            case MONTH:
                return MARKET_STATS_BY_MONTH;
            default:
                throw new IllegalArgumentException("Unknown timeframe: " + timeFrame.name());
        }
    }
}
