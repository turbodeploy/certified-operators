package com.vmturbo.history.db;

import java.sql.Timestamp;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * Class to perform market stats rollups.
 */
public class MarketStatsRollups {
    private static final String TIME_SERIES_KEY_NAME = "time_series_key";
    private static final String TOPOLOGY_CONTEXT_ID_NAME = "topology_context_id";
    private static final String ENVIRONMENT_TYPE_FIELD_NAME = "environment_type";
    private final Table<?> source;
    private final Table<?> rollup;
    private final Timestamp snapshotTime;
    private final Timestamp rollupTime;
    private Field<String> fTimeSeriesKey;
    private Field<Timestamp> fSnapshotTime;
    private Field<Long> fTopologyContextId;
    private Field<String> fEntityType;
    private Field<EnvironmentType> fEnvironmentType;
    private Field<String> fPropertyType;
    private Field<String> fPropertySubtype;
    private Field<RelationType> fRelation;
    private Field<Double> fCapacity;
    private Field<Double> fEffectiveCapacity;
    private Field<Double> fAvgValue;
    private Field<Double> fMinValue;
    private Field<Double> fMaxValue;
    private Field<Integer> fSamples;
    private Field<Integer> fSourceSamples;

    /**
     * Create a new instance for a rollup operation.
     *
     * @param source       table whose data is to be rolled up
     * @param rollup       table into which rollup data will be written
     * @param snapshotTime timestamp of source records to participate
     * @param rollupTime   timestamp of rollup records
     */
    public MarketStatsRollups(Table<?> source, Table<?> rollup, Timestamp snapshotTime,
            Timestamp rollupTime) {
        this.source = source;
        this.rollup = rollup;
        this.snapshotTime = snapshotTime;
        this.rollupTime = rollupTime;
        createFields();
    }

    /**
     * Create and execute an upsert operation to perform this rollup operation.
     *
     * @param dsl {@link DSLContext} with access to the database
     */
    public void execute(DSLContext dsl) {
        Query query = new UpsertBuilder().withSourceTable(source).withTargetTable(rollup)
                .withInsertFields(fSnapshotTime, fTimeSeriesKey,
                        fTopologyContextId, fEntityType, fEnvironmentType,
                        fPropertyType, fPropertySubtype, fRelation,
                        fCapacity, fEffectiveCapacity, fAvgValue, fMinValue, fMaxValue,
                        fSamples)
                .withInsertValue(fSnapshotTime, DSL.inline(rollupTime))
                .withInsertValue(fSamples, fSourceSamples)
                .withSourceCondition(
                        UpsertBuilder.getSameNamedField(fSnapshotTime, source).eq(snapshotTime))
                .withUpdateValue(fCapacity, UpsertBuilder::inserted)
                .withUpdateValue(fEffectiveCapacity, UpsertBuilder::inserted)
                .withUpdateValue(fAvgValue, UpsertBuilder.avg(fSamples))
                .withUpdateValue(fMaxValue, UpsertBuilder::max)
                .withUpdateValue(fMinValue, UpsertBuilder::min)
                .withUpdateValue(fSamples, UpsertBuilder::sum)
                .getUpsert(dsl);
        execute(query, dsl);
    }

    /**
     * Execute the upsert statement, with temporary fixup needed for Postgres dialect.
     *
     * <p>This method is required until the POSTGRES_PRIMARY_DB feature flag is retired. The reason
     * is that until that time, the "legacy" MariaDB migrations from which the jOOQ model is built
     * does not reflect that `(snapshot_time, time_series_key)` is a primary key of all the market-
     * stats tables, although this is true in the non-legacy migrations (both for MariaDB and
     * Postgres). Becuase jOOQ is unaware of this primary key, it is unaable to properly compose the
     * upsert statement for postgres, rendering a statement that includes "ON CONFLICT[unknown
     * primary key])". We fix that here.</p>
     *
     * <p>The fixup is not required for MariaDB because it lacks the syntax to specify a constraint
     * as is the case with Postgres. jOOQ generates an upsert with "ON DUPLICATE KEY" which works
     * just fine with the non-legacy MariaDB schema.</p>
     *
     * @param upsert upsert {@link Query} object composed by jOOQ
     * @param dsl    {@link DSLContext} used to build the query
     */
    private void execute(Query upsert, DSLContext dsl) {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() && dsl.dialect() == SQLDialect.POSTGRES) {
            String sql = upsert.getSQL(ParamType.INLINED)
                    .replace("[unknown primary key]", "snapshot_time, time_series_key");
            dsl.execute(sql);
        } else {
            upsert.execute();
        }
    }

    private void createFields() {
        this.fSnapshotTime = JooqUtils.getTimestampField(rollup, StringConstants.SNAPSHOT_TIME);
        this.fTimeSeriesKey = JooqUtils.getStringField(rollup, TIME_SERIES_KEY_NAME);
        this.fTopologyContextId = JooqUtils.getLongField(rollup, TOPOLOGY_CONTEXT_ID_NAME);
        this.fEntityType = JooqUtils.getStringField(rollup, StringConstants.ENTITY_TYPE);
        this.fEnvironmentType = JooqUtils.getEnvField(rollup, ENVIRONMENT_TYPE_FIELD_NAME);
        this.fPropertyType = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_TYPE);
        this.fPropertySubtype = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_SUBTYPE);
        this.fRelation = JooqUtils.getRelationTypeField(rollup, StringConstants.RELATION);
        this.fCapacity = JooqUtils.getDoubleField(rollup, StringConstants.CAPACITY);
        this.fEffectiveCapacity = JooqUtils.getDoubleField(rollup,
                StringConstants.EFFECTIVE_CAPACITY);
        this.fAvgValue = JooqUtils.getDoubleField(rollup, StringConstants.AVG_VALUE);
        this.fMinValue = JooqUtils.getDoubleField(rollup, StringConstants.MIN_VALUE);
        this.fMaxValue = JooqUtils.getDoubleField(rollup, StringConstants.MAX_VALUE);
        this.fSamples = JooqUtils.getIntField(rollup, StringConstants.SAMPLES);
        this.fSourceSamples = source == Tables.MARKET_STATS_LATEST
                              ? DSL.inline(1)
                              : JooqUtils.getIntField(source, StringConstants.SAMPLES);
    }
}
