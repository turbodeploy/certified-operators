package com.vmturbo.history.listeners;

import static com.vmturbo.common.protobuf.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.EFFECTIVE_CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIN_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRODUCER_UUID;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.SAMPLES;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Name;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.schema.RelationType;

/**
 * Class to create the upsert operation used to perform a rollup.
 */
public class RollupUpsert {
    private static final Logger logger = LogManager.getLogger();

    private static final String HOUR_KEY = "hour_key";
    private static final String DAY_KEY = "day_key";
    private static final String MONTH_KEY = "month_key";
    private final Table<?> source;
    private final Table<?> rollup;
    private final Timestamp snapshotTime;
    private final Timestamp rollupTime;
    private final String low;
    private final String high;
    private final RollupProcessor.RollupType rollupType;
    private final Field<Timestamp> sSnapshotTime;
    private final Field<String> sUuid;
    private final Field<String> sProducerUuid;
    private final Field<String> sPropertyType;
    private final Field<String> sPropertySubtype;
    private final Field<RelationType> sRelation;
    private final Field<String> sCommodityKey;
    private final Field<Double> sCapacity;
    private final Field<Double> sEffectiveCapacity;
    private final Field<Double> sMaxValue;
    private final Field<Double> sMinValue;
    private final Field<Double> sAvgValue;
    private final Field<Integer> sSamples;
    private final Field<String> sRollupKey;
    private final Field<Timestamp> rSnapshotTime;
    private final Field<String> rUuid;
    private final Field<?> rProducerUuid;
    private final Field<String> rPropertyType;
    private final Field<String> rPropertySubtype;
    private final Field<RelationType> rRelation;
    private final Field<String> rCommodityKey;
    private final Field<Double> rCapacity;
    private final Field<Double> rEffectiveCapacity;
    private final Field<Double> rMaxValue;
    private final Field<Double> rMinValue;
    private final Field<Double> rAvgValue;
    private final Field<Integer> rSamples;
    private final Field<String> rRollupKey;

    /**
     * Create a new instance.
     *
     * @param source       table containing records to be rolled up
     * @param rollup       table that into which source data will be rolled up
     * @param snapshotTime time of source records to be rolled up
     * @param rollupTime   time of rolled up records
     * @param rollupType   type of this rollup operation
     * @param low          lower bound for time-series keys to participate in this operation, or
     *                     null for no lower bound
     * @param high         upper bound for time-series keys to participate in this operation, or
     *                     null for no upper bound
     */
    public RollupUpsert(Table<?> source, Table<?> rollup, Timestamp snapshotTime,
            Timestamp rollupTime, RollupProcessor.RollupType rollupType, String low, String high) {
        this.source = source;
        this.rollup = rollup;
        this.snapshotTime = snapshotTime;
        this.rollupTime = rollupTime;
        this.rollupType = rollupType;
        this.low = low;
        this.high = high;

        // get Field objects for source table
        this.sSnapshotTime = source.field(SNAPSHOT_TIME, Timestamp.class);
        this.sUuid = source.field(UUID, String.class);
        this.sProducerUuid = source.field(PRODUCER_UUID, String.class);
        this.sPropertyType = source.field(PROPERTY_TYPE, String.class);
        this.sPropertySubtype = source.field(PROPERTY_SUBTYPE, String.class);
        this.sRelation = source.field(RELATION, RelationType.class);
        this.sCommodityKey = source.field(COMMODITY_KEY, String.class);
        this.sCapacity = source.field(CAPACITY, Double.class);
        this.sEffectiveCapacity = source.field(EFFECTIVE_CAPACITY, Double.class);
        this.sMaxValue = source.field(MAX_VALUE, Double.class);
        this.sMinValue = source.field(MIN_VALUE, Double.class);
        this.sAvgValue = source.field(AVG_VALUE, Double.class);
        this.sSamples = source.field(SAMPLES, Integer.class);
        this.sRollupKey = getRollupKeyField(source);

        // Field objects for rollup table
        this.rSnapshotTime = rollup.field(SNAPSHOT_TIME, Timestamp.class);
        this.rUuid = rollup.field(UUID, String.class);
        this.rProducerUuid = rollup.field(PRODUCER_UUID, String.class);
        this.rPropertyType = rollup.field(PROPERTY_TYPE, String.class);
        this.rPropertySubtype = rollup.field(PROPERTY_SUBTYPE, String.class);
        this.rRelation = rollup.field(RELATION, RelationType.class);
        this.rCommodityKey = rollup.field(COMMODITY_KEY, String.class);
        this.rCapacity = rollup.field(CAPACITY, Double.class);
        this.rEffectiveCapacity = rollup.field(EFFECTIVE_CAPACITY, Double.class);
        this.rMaxValue = rollup.field(MAX_VALUE, Double.class);
        this.rMinValue = rollup.field(MIN_VALUE, Double.class);
        this.rAvgValue = rollup.field(AVG_VALUE, Double.class);
        this.rSamples = rollup.field(SAMPLES, Integer.class);
        this.rRollupKey = getRollupKeyField(rollup);
    }

    private Field getRollupKeyField(Table<?> table) {
        Optional<EntityType> type = EntityType.fromTable(rollup);
        return type.map(t -> {
            if (t.getLatestTable().orElseGet(null) == table
                    || t.getHourTable().orElse(null) == table) {
                return table.field(HOUR_KEY, String.class);
            } else if (t.getDayTable().orElse(null) == table) {
                return table.field(DAY_KEY, String.class);
            } else {
                return table.field(MONTH_KEY, String.class);
            }
        }).orElse(null);
    }

    /**
     * Execute the assembled upsert statement.
     *
     * @param dsl DSLContext providing DB acccess
     */
    public void execute(DSLContext dsl) {
        dsl.transaction(trans -> {
            setIsolation(trans, Connection.TRANSACTION_READ_UNCOMMITTED);
            createUpsert(trans.dsl())
                    .execute();
        });
    }

    private Query createUpsert(DSLContext dsl) {
        Field<Integer> inSamples = sSamples != null ? sSamples : DSL.inline(1);
        Select select = dsl.select(DSL.inline(rollupTime), sUuid, sProducerUuid,
                        sPropertyType, sPropertySubtype, sRelation, sCommodityKey, sCapacity,
                        sEffectiveCapacity, sMaxValue, sMinValue, sAvgValue, inSamples, sRollupKey)
                .from(source)
                .where(sSnapshotTime.eq(snapshotTime))
                .and(low == null ? DSL.trueCondition() : sRollupKey.ge(low))
                .and(high == null ? DSL.trueCondition() : sRollupKey.le(high));

        InsertOnDuplicateSetMoreStep upsert = dsl.insertInto(rollup).columns(
                        rSnapshotTime, rUuid, rProducerUuid, rPropertyType, rPropertySubtype,
                        rRelation, rCommodityKey, rCapacity, rEffectiveCapacity,
                        rMaxValue, rMinValue, rAvgValue, rSamples, rRollupKey)
                .select(select)
                .onDuplicateKeyUpdate()
                .set(rMaxValue,
                        DSL.case_().when(rMaxValue.gt(revive(rMaxValue, dsl)), rMaxValue)
                                .else_(revive(rMaxValue, dsl)))
                .set(rMinValue,
                        DSL.case_().when(rMinValue.lt(revive(rMinValue, dsl)), rMinValue).else_(
                                revive(rMinValue, dsl)))
                .set(rAvgValue, (rAvgValue.times(rSamples)
                        .plus(revive(rAvgValue, dsl).times(revive(rSamples, dsl))))
                        .divide(rSamples.plus(revive(rSamples, dsl))))
                .set(rSamples, rSamples.plus(revive(rSamples, dsl)))
                .set(rCapacity,
                        DSL.case_().when(rCapacity.gt(revive(rCapacity, dsl)), rCapacity)
                                .else_(revive(rCapacity, dsl)))
                .set(rEffectiveCapacity, DSL.case_()
                        .when(rEffectiveCapacity.gt(revive(rEffectiveCapacity, dsl)),
                                rEffectiveCapacity)
                        .else_(revive(rEffectiveCapacity, dsl)));
        return upsert;
    }

    private <T> Field<T> revive(Field<T> f, DSLContext dsl) {
        Name name = f.getUnqualifiedName().unquotedName();
        String reviver =
                dsl.dialect() == SQLDialect.POSTGRES ? "excluded." + name : "VALUES(" + name + ")";
        return DSL.field(reviver, f.getDataType());
    }

    private void setIsolation(Configuration trans, int isolationLevel) {
        trans.dsl().connection(conn ->
                conn.setTransactionIsolation(isolationLevel));
    }
}
