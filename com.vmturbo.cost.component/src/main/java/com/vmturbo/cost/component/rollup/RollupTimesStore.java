package com.vmturbo.cost.component.rollup;

import static com.vmturbo.cost.component.db.Tables.AGGREGATION_META_DATA;

import java.sql.Timestamp;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.cost.component.db.tables.records.AggregationMetaDataRecord;

/**
 * Store for rollup times.
 */
public class RollupTimesStore {

    private static final Logger logger = LogManager.getLogger();

    /**
     * JOOQ access.
     */
    protected final DSLContext dsl;

    private final RolledUpTable table;

    /**
     * Create new instance of {@code LastRollupTimesStore}.
     *
     * @param dsl DSL context.
     * @param table Table for rollup.
     */
    public RollupTimesStore(DSLContext dsl, RolledUpTable table) {
        this.dsl = dsl;
        this.table = table;
    }

    /**
     * Set last rollup times in DB.
     *
     * @param rollupTimes New rollup times.
     */
    public void setLastRollupTimes(@Nonnull final LastRollupTimes rollupTimes) {
        try {
            final AggregationMetaDataRecord record = new AggregationMetaDataRecord();
            record.setAggregateTable(table.getTableName());
            record.setLastAggregated(new Timestamp(rollupTimes.getLastTimeUpdated()));
            if (rollupTimes.hasLastTimeByHour()) {
                record.setLastAggregatedByHour(new Timestamp(rollupTimes.getLastTimeByHour()));
            }
            if (rollupTimes.hasLastTimeByDay()) {
                record.setLastAggregatedByDay(new Timestamp(rollupTimes.getLastTimeByDay()));
            }
            if (rollupTimes.hasLastTimeByMonth()) {
                record.setLastAggregatedByMonth(new Timestamp(rollupTimes.getLastTimeByMonth()));
            }

            dsl.insertInto(AGGREGATION_META_DATA)
                    .set(record)
                    .onDuplicateKeyUpdate()
                    .set(record)
                    .execute();
        } catch (Exception e) {
            logger.warn("Unable to set last rollup times to DB: {}", rollupTimes, e);
        }
    }

    /**
     * Get metadata about last time rollup was done.
     *
     * @return Last rollup times.
     */
    @Nonnull
    public LastRollupTimes getLastRollupTimes() {
        final LastRollupTimes rollupTimes = new LastRollupTimes();
        try {
            final AggregationMetaDataRecord record = dsl.selectFrom(AGGREGATION_META_DATA)
                    .where(AGGREGATION_META_DATA.AGGREGATE_TABLE.eq(table.getTableName()))
                    .fetchOne();
            if (record == null) {
                return rollupTimes;
            }
            if (record.getLastAggregated() != null) {
                rollupTimes.setLastTimeUpdated(record.getLastAggregated().getTime());
            }
            if (record.getLastAggregatedByHour() != null) {
                rollupTimes.setLastTimeByHour(record.getLastAggregatedByHour().getTime());
            }
            if (record.getLastAggregatedByDay() != null) {
                rollupTimes.setLastTimeByDay(record.getLastAggregatedByDay().getTime());
            }
            if (record.getLastAggregatedByMonth() != null) {
                rollupTimes.setLastTimeByMonth(record.getLastAggregatedByMonth().getTime());
            }
        } catch (Exception e) {
            logger.warn("Unable to fetch last rollup times from DB.", e);
        }
        return rollupTimes;
    }
}
