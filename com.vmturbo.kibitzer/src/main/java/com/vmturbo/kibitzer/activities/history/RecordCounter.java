package com.vmturbo.kibitzer.activities.history;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.springframework.context.ApplicationContext;

import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.kibitzer.KibitzerComponent;
import com.vmturbo.kibitzer.KibitzerDb;
import com.vmturbo.kibitzer.KibitzerDb.DbMode;
import com.vmturbo.kibitzer.activities.KibitzerActivity;

/**
 * Simple {@link KibitzerActivity} that reports a record count for a designated table. This can be
 * used with any target component.
 *
 * <p>The per-run result is the table's record count. The overall activity result is the average of
 * the per-run record counts.</p>
 */
public class RecordCounter extends KibitzerActivity<Integer, Double> {
    private static final Logger logger = LogManager.getLogger();
    private static final String RECORD_COUNTER_ACTIVITY_NAME = "record_counter";
    private static final String TABLE_PROPERTY_NAME = "table";
    private Table<?> table;

    /**
     * Create a new instance.
     *
     * @throws KibitzerActivityException if there's a problem creating the instance
     */
    public RecordCounter() throws KibitzerActivityException {
        super(KibitzerComponent.ANY, RECORD_COUNTER_ACTIVITY_NAME);
        // TODO: This should work with other components, so we'll need a way to specify a schema
        // dynamically rather than hard-coding it here
        config.add(config.tableProperty(TABLE_PROPERTY_NAME, Vmtdb.VMTDB)
                .withDescription("Table whose records are to be counted")
                .withDefault(Tables.VM_STATS_LATEST));
        config.setDefault(DESCRIPTION_CONFIG_KEY, "Count the records in the specified table");
        config.setDefault(DB_MODE_CONFIG_KEY, DbMode.COMPONENT);
        config.setDefault(RUNS_CONFIG_KEY, 5);
        config.setDefault(SCHEDULE_CONFIG_KEY, Duration.ofSeconds(10));
    }

    @Override
    public KibitzerActivity<Integer, Double> newInstance() throws KibitzerActivityException {
        return new RecordCounter();
    }

    @Override
    public void report(Double result) {
        logger.info("Activity {} found {} records in {} on average", this, result, table);
    }

    @Override
    public void init(DSLContext dsl, KibitzerDb db, ApplicationContext context)
            throws KibitzerActivityException {
        super.init(dsl, db, context);
        this.table = config.getTable(TABLE_PROPERTY_NAME);
    }

    @Override
    public Optional<Integer> run(int i) throws KibitzerActivityException {
        super.run(i);
        Integer count = (Integer)dsl.selectCount().from(table).fetchOne(0);
        logger.info("{} record count: {}", table, count);
        return Optional.of(count);
    }

    @Override
    public Optional<Double> finish(List<Integer> runResults) {
        return Optional.of(runResults.stream().collect(Collectors.averagingInt(i -> i)));
    }
}
