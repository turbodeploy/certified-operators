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
    private final RollupType rollupType;
    private final String low;
    private final String high;
    private final DSLContext dsl;

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

    // Record key fields (hour_key, day_key, month_key) are handled differently for Postgres than
    // for MariaDB. The reason is that we took advantage of the opportunity to switch to a simpler
    // and more efficient approach for Postgres, having no customers with existing data that we
    // would need to accommodate.
    //
    // In MariaDB, we have all three key columns in latest and hourly tables, day_key and month_key
    // in daily tables, and just month_key in monthlies. In each rollup, we copy all keys that
    // appear in the rollup table from the source table (leaving out, e.g. hour_key when performing
    // rollups from hourly to daily).
    //
    // In Postgres we have just one key field in each table - hour_key in latest and hourlies,
    // day_key in dailies, and month_key in monthlies. When rolling up, we copy whichever key field
    // exists in the source table to whichever key field exists in the rollup table. (It would
    // have been nicer to be left with one less-specific key field name across all tables, but our
    // transitional jOOQ strategy could not accommodate that.)

    // This array will be populated with all the rollup-table fields that will receive keys from
    // the source table in this rollup operation. For Postgres this will always be a singleton
    // array.
    private Field<String>[] fRollupKeys;
    // This is the key field in the source table that will be copied to the rollup table in
    // the case of Postgres rollups. It's also needed for MariaDB, since it's the field that is
    // compared to key bounds to determine which source records will participate in the rollup.
    private Field<String> fSourceKey;

    // field needed to deal with a bug introduced in release 8.5.2 which can result in `day_key`
    // and `month_key` fields being null in some hourly records. In such cases we use the
    // `hour_key` value instead.
    private Field<String> fDayKey;
    private Field<String> fMonthKey;
    private Field<String> fSourceHourKey;
    private Field<String> fSourceDayKey;
    private Field<String> fSourceMonthKey;

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
     * @param dsl          Db access
     */
    public EntityStatsRollups(Table<?> source, Table<?> rollup, Timestamp snapshotTime,
            Timestamp rollupTime, RollupType rollupType, String low, String high, DSLContext dsl) {
        this.source = source;
        this.rollup = rollup;
        this.snapshotTime = snapshotTime;
        this.rollupTime = rollupTime;
        this.rollupType = rollupType;
        this.low = low;
        this.high = high;
        this.dsl = dsl;
        computeFields();
    }

    /**
     * Create and execute an upsert operation to perform this rollup operation.
     *
     * @param dsl {@link DSLContext} with access to the database
     */
    public void execute(DSLContext dsl) {
        getUpsert().execute();
    }

    /**
     * Build an UPSERT statement and return the resulting jOOQ query object, ready to execute.
     *
     * @return the jOOQ Query object representing the UPSERT
     */
    public Query getUpsert() {
        // incoming `snapshotTime` value is always truncated down to nearest second. Since MySQL
        // will have rounded - possibly up - when storing that time into the source records, we
        // need to include that as a possibility in our selection criteria, which we do below.
        // Here we just compute that potentially rounded-up second
        return new UpsertBuilder().withSourceTable(source).withTargetTable(rollup)
                .withInsertFields(fSnapshotTime, fUuid, fProducerUuid,
                        fPropertyType, fPropertySubtype, fRelation, fCommodityKey,
                        fCapacity, fEffectiveCapacity, fMaxValue, fMinValue, fAvgValue,
                        fSamples)
                .withInsertFields(fRollupKeys)
                .withInsertValue(fSnapshotTime, DSL.inline(rollupTime))
                .conditionally(dsl.dialect() == SQLDialect.POSTGRES,
                        builder -> builder.withInsertValue(fRollupKeys[0], fSourceKey))
                // following two conditional insert-values additions handle an issue that arose
                // with release 8.5.2. It included an ill-fated change for record rollup keys that
                // resulted in null `day_key` and `month_key` columns in hourly records. Those
                // records would cause the first daily/monthly rollup after an upgrade to fail,
                // because the 8.5.2 changes were reverted in 8.5.3 for other reasons.
                // This fix uses the `hour_key` field when the normal field is null. That's
                // actually consistent with the intent of the 8.5.2 change, but with the 8.5.3
                // reversion the nulls are now poisonous.
                .conditionally(
                        dsl.dialect() != SQLDialect.POSTGRES && rollupType == RollupType.BY_DAY,
                        builder -> builder.withInsertValue(fDayKey,
                                DSL.coalesce(fSourceDayKey, fSourceKey)))
                .conditionally(
                        dsl.dialect() != SQLDialect.POSTGRES && rollupType == RollupType.BY_MONTH,
                        builder -> builder.withInsertValue(fMonthKey,
                                DSL.coalesce(fSourceMonthKey, fSourceKey)))
                .withInsertValue(fSamples, fSourceSamples)
                // for hourly rollups we need to guard against the possibilities of duplicates
                // the latest table, which can happen if an ingestion is restarted after a crash
                .withDistinctSelect(rollupType == RollupType.BY_HOUR)
                .withSourceCondition(
                        UpsertBuilder.getSameNamedField(fSnapshotTime, source).eq(snapshotTime)
                                .or(UpsertBuilder.getSameNamedField(fSnapshotTime, source).eq(
                                        Timestamp.from(snapshotTime.toInstant().plusSeconds(1)))))
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
        this.fPropertySubtype = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_SUBTYPE);
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
        //noinspection unchecked
        this.fRollupKeys = (Field<String>[])getTableKeys(source);
        this.fSourceKey = hourKeyField(source);
        this.fDayKey = rollupType == RollupType.BY_DAY ? dayKeyField(rollup) : null;
        this.fMonthKey = rollupType == RollupType.BY_MONTH ? monthKeyfield(rollup) : null;
        this.fSourceHourKey = hourKeyField(source);
        this.fSourceDayKey = dayKeyField(source);
        this.fSourceMonthKey = monthKeyfield(source);
    }

    private Field<?>[] getTableKeys(Table<?> table) {
        Optional<EntityType> entityType = EntityType.fromTable(table);
        boolean isPostgres = dsl.dialect() == SQLDialect.POSTGRES;
        if (entityType.flatMap(EntityType::getHourTable).orElse(null) == rollup) {
            return isPostgres
                   ? new Field[]{hourKeyField(table)}
                   : new Field[]{hourKeyField(table), dayKeyField(table), monthKeyfield(table)};
        } else if (entityType.flatMap(EntityType::getDayTable).orElse(null) == rollup) {
            return isPostgres
                   ? new Field[]{dayKeyField(table)}
                   : new Field[]{dayKeyField(table), monthKeyfield(table)};
        } else if (entityType.flatMap(EntityType::getMonthTable).orElse(null) == rollup) {
            return new Field[]{monthKeyfield(table)};
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown rollup table %s", rollup.getName()));
        }
    }

    private Field<String> hourKeyField(Table<?> table) {
        return JooqUtils.getStringField(table, HOUR_KEY_NAME);
    }

    private Field<String> dayKeyField(Table<?> table) {
        return JooqUtils.getStringField(table, DAY_KEY_NAME);
    }

    private Field<String> monthKeyfield(Table<?> table) {
        return JooqUtils.getStringField(table, MONTH_KEY_NAME);
    }
}
