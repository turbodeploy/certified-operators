package com.vmturbo.history.db;

import java.sql.Timestamp;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * Class to perform cluster stats rollups.
 */
public class ClusterStatsRollups {

    private final Table<?> source;
    private final Table<?> rollup;
    private final Timestamp snapshotTime;
    private final Timestamp rollupTime;
    private Field<Timestamp> fRecordedOn;
    private Field<String> fInternalName;
    private Field<String> fPropertyType;
    private Field<String> fPropertySubtype;
    private Field<Double> fValue;
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
    public ClusterStatsRollups(Table<?> source, Table<?> rollup, Timestamp snapshotTime,
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
        new UpsertBuilder().withSourceTable(source).withTargetTable(rollup)
                .withInsertFields(fRecordedOn, fInternalName, fPropertyType, fPropertySubtype,
                        fValue, fSamples)
                .withInsertValue(fRecordedOn, DSL.inline(rollupTime))
                .withInsertValue(fSamples, fSourceSamples)
                .withSourceCondition(
                        UpsertBuilder.getSameNamedField(fRecordedOn, source).eq(snapshotTime))
                .withUpdateValue(fValue, UpsertBuilder.avg(fSamples))
                .withUpdateValue(fSamples, UpsertBuilder::sum)
                .getUpsert(dsl)
                .execute();
    }

    private void createFields() {
        this.fRecordedOn = JooqUtils.getTimestampField(rollup, StringConstants.RECORDED_ON);
        this.fInternalName = JooqUtils.getStringField(rollup, StringConstants.INTERNAL_NAME);
        this.fPropertyType = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_TYPE);
        this.fPropertySubtype = JooqUtils.getStringField(rollup, StringConstants.PROPERTY_SUBTYPE);
        this.fValue = JooqUtils.getDoubleField(rollup, StringConstants.VALUE);
        this.fSamples = JooqUtils.getIntField(rollup, StringConstants.SAMPLES);
        this.fSourceSamples = source == Tables.CLUSTER_STATS_LATEST
                              ? DSL.inline(1)
                              : JooqUtils.getIntField(source, StringConstants.SAMPLES);
    }
}
