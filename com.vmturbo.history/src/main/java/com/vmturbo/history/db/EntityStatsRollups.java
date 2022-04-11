package com.vmturbo.history.db;

import java.sql.Timestamp;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.listeners.RollupProcessor.RollupType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * Class to perform entity stats rollups.
 */
public class EntityStatsRollups {

    private static final String HOUR_KEY_NAME = "hour_key";
    private static final String DAY_KEY_NAME = "day_key";
    private static final String MONTH_KEY_NAME = "month_key";

    private final Table<?> source;
    private final Table<?> rollup;
    private final Timestamp snapshotTime;
    private final Timestamp rollupTime;
    private final String low;
    private final String high;
    private final RollupType rollupType;
    private Field<Timestamp> fSnapshotTime;
    private Field<String> fUuid;
    private Field<String> fProducerUuid;
    private Field<String> fPropertyType;
    private Field<String> fPropertySubtype;
    private Field<RelationType> fRelation;
    private Field<String> fCommodityKey;
    private Field<Double> fCapacity;
    private Field<Double> fEffectiveCapacity;
    private Field<Double> fMaxValue;
    private Field<Double> fMinValue;
    private Field<Double> fAvgValue;
    private Field<Integer> fSamples;
    private Field<Integer> fSourceSamples;
    private Field<String> fSourceKey;
    private Field<String> fRollupKey;

    /**
     * Create a new instance for a rollup operation.
     *
     * @param source       table whose data is to be rolled up
     * @param rollup       table into which rollup data will be written
     * @param snapshotTime timestamp of source records to participate
     * @param rollupTime   timestamp of rollup records
     * @param rollupType   type of rollup (hourly, daily, monthly)
     * @param low          lower bound for source rollup keys to participate in this operation
     * @param high         upper bound for source rollup keys to participate in this operation
     */
    public EntityStatsRollups(Table<?> source, Table<?> rollup, Timestamp snapshotTime,
            Timestamp rollupTime, RollupType rollupType, String low, String high) {
        this.source = source;
        this.rollup = rollup;
        this.snapshotTime = snapshotTime;
        this.rollupTime = rollupTime;
        this.rollupType = rollupType;
        this.low = low;
        this.high = high;
        computeFields();
    }

    /**
     * Create and execute an upsert operation to perform this rollup operation.
     *
     * @param dsl {@link DSLContext} with access to the database
     */
    public void execute(DSLContext dsl) {
        getUpsert(dsl).execute();
    }

    /**
     * Build an UPSERT statement and return the resulting jOOQ query object, ready to execute.
     *
     * @param dsl DSLContext to use when building (and ultimately executing) the upsert
     * @return the jOOQ Query object representing the UPSERT
     */
    public Query getUpsert(DSLContext dsl) {
        return new UpsertBuilder().withSourceTable(source).withTargetTable(rollup)
                .withInsertFields(fSnapshotTime, fUuid, fProducerUuid,
                        fPropertyType, fPropertySubtype, fRelation, fCommodityKey,
                        fCapacity, fEffectiveCapacity, fMaxValue, fMinValue, fAvgValue,
                        fSamples, fRollupKey)
                .withInsertValue(fSnapshotTime, DSL.inline(rollupTime))
                .withInsertValue(fSamples, fSourceSamples)
                .withInsertValue(fRollupKey, fSourceKey)
                // for hourly rollups we need to guard against the possibilities of duplicates
                // hte latest table, which can happen if an ingestion is restarted after a crash
                .withDistinctSelect(rollupType == RollupType.BY_HOUR)
                .withSourceCondition(
                        UpsertBuilder.getSameNamedField(fSnapshotTime, source).eq(snapshotTime))
                .withSourceCondition(low != null ? fSourceKey.ge(low) : DSL.trueCondition())
                .withSourceCondition(high != null ? fSourceKey.le(high) : DSL.trueCondition())
                // we have an index on the first 8 chars of the hour_key, which is in the form of
                // a MariaDB "partial index" or a Postgres "expression index". For MariaDB, that
                // index will be used based on the above conditions. But for Postgres we must use
                // a matching expression in our query to enable use of the expression index. The
                // following conditions do that but do not affect row selection since their truth
                // is implied by truth of the conditions above.
                .withSourceCondition(dsl.dialect() == SQLDialect.POSTGRES && low != null
                                     ? DSL.left(fSourceKey, 8).ge(low.substring(0, 8))
                                     : DSL.trueCondition())
                .withSourceCondition(dsl.dialect() == SQLDialect.POSTGRES && high != null
                                     ? DSL.left(fSourceKey, 8).le(high.substring(0, 8))
                                     : DSL.trueCondition())
                .withUpdateValue(fCapacity, UpsertBuilder::max)
                .withUpdateValue(fEffectiveCapacity, UpsertBuilder::max)
                .withUpdateValue(fMaxValue, UpsertBuilder::max)
                .withUpdateValue(fMinValue, UpsertBuilder::min)
                .withUpdateValue(fAvgValue, UpsertBuilder.avg(fSamples))
                .withUpdateValue(fSamples, UpsertBuilder::sum)
                .getUpsert(dsl);
    }

    private void computeFields() {
        this.fSnapshotTime = JooqUtils.getTimestampField(rollup, StringConstants.SNAPSHOT_TIME);
        this.fUuid = JooqUtils.getStringField(rollup, StringConstants.UUID);
        this.fProducerUuid = JooqUtils.getStringField(rollup, StringConstants.PRODUCER_UUID);
        this.fPropertyType = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_TYPE);
        this.fPropertySubtype = JooqUtils.getStringField(rollup,
                StringConstants.PROPERTY_SUBTYPE);
        this.fRelation = JooqUtils.getRelationTypeField(rollup, StringConstants.RELATION);
        this.fCommodityKey = JooqUtils.getStringField(rollup, StringConstants.COMMODITY_KEY);
        this.fCapacity = JooqUtils.getDoubleField(rollup, StringConstants.CAPACITY);
        this.fEffectiveCapacity = JooqUtils.getDoubleField(rollup,
                StringConstants.EFFECTIVE_CAPACITY);
        this.fMaxValue = JooqUtils.getDoubleField(rollup, StringConstants.MAX_VALUE);
        this.fMinValue = JooqUtils.getDoubleField(rollup, StringConstants.MIN_VALUE);
        this.fAvgValue = JooqUtils.getDoubleField(rollup, StringConstants.AVG_VALUE);
        this.fSamples = JooqUtils.getIntField(rollup, StringConstants.SAMPLES);
        Table<?> latestTable = EntityType.fromTable(source)
                .flatMap(EntityType::getLatestTable)
                .orElse(null);
        this.fSourceSamples = source == latestTable
                              ? DSL.inline(1)
                              : JooqUtils.getIntField(source, StringConstants.SAMPLES);
        this.fSourceKey = getTableKey(source);
        this.fRollupKey = getTableKey(rollup);
    }

    private Field<String> getTableKey(Table<?> table) {
        Optional<EntityType> entityType = EntityType.fromTable(table);
        if (entityType.flatMap(EntityType::getLatestTable).orElse(null) == table
                || entityType.flatMap(EntityType::getHourTable).orElse(null) == table) {
            return JooqUtils.getStringField(table, HOUR_KEY_NAME);
        } else if (entityType.flatMap(EntityType::getDayTable).orElse(null) == table) {
            return JooqUtils.getStringField(table, DAY_KEY_NAME);
        } else if (entityType.flatMap(EntityType::getMonthTable).orElse(null) == table) {
            return JooqUtils.getStringField(table, MONTH_KEY_NAME);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown entity-stats table %s", table.getName()));
        }
    }
}
