package com.vmturbo.sql.utils.sizemon;

import java.sql.Timestamp;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep5;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.DbSizeReport;
import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * {@link DbSizeReporter} that writes size info to a database table.
 */
public class PersistingReporter extends DbSizeReporter {

    private static final Field<Timestamp> TIME_FIELD = DSL.field("time", Timestamp.class);
    private static final Field<String> GRANULARITY_FIELD = DSL.field("granularity", String.class);
    private static final Field<String> TABLE_NAME_FIELD = DSL.field("table_name", String.class);
    private static final Field<String> DESCRIPTION_FIELD = DSL.field("description", String.class);
    private static final Field<Long> SIZE_FIELD = DSL.field("size", Long.class);
    private static final Table<Record> DB_SIZE_TABLE = DSL.table("db_size_info_v1");
    private static final String SCHEMA_TOTAL_DESCRIPTION = "schema total";
    private final Supplier<DSLContext> dslSupplier;
    private Timestamp now;

    /**
     * Create a new instance.
     *
     * @param dslSupplier supplier of a {@link DSLContext} for performing DB operations
     * @param schema      schema whose size info is being persisted
     * @param includes    regexes for tables to be included
     * @param excludes    regexes for tables to be excluded
     * @param granularity granularity at which to persist size info
     */
    public PersistingReporter(final Supplier<DSLContext> dslSupplier, final Schema schema,
            final Set<Pattern> includes, final Set<Pattern> excludes, final Granularity granularity) {
        super(schema, includes, excludes, granularity);
        this.dslSupplier = dslSupplier;
    }

    /**
     * Ensure that the table exists with the correct structure.
     */
    @Override
    public void processStart() {
        this.now = new Timestamp(System.currentTimeMillis());
        dslSupplier.get().createTableIfNotExists(DB_SIZE_TABLE)
                .column(TIME_FIELD, SQLDataType.TIMESTAMP(0))
                .column(TABLE_NAME_FIELD, SQLDataType.VARCHAR(50))
                .column(GRANULARITY_FIELD, SQLDataType.VARCHAR(20))
                .column(DESCRIPTION_FIELD, SQLDataType.VARCHAR(200))
                .column(SIZE_FIELD, SQLDataType.BIGINT)
                .execute();
    }

    @Override
    void processTableReport(final DbSizeReport report) {
        final InsertValuesStep5<Record, Timestamp, String, String, String, Long> insert =
                dslSupplier.get().insertInto(DB_SIZE_TABLE)
                        .columns(TIME_FIELD, TABLE_NAME_FIELD, GRANULARITY_FIELD,
                                DESCRIPTION_FIELD, SIZE_FIELD);
        report.getSizeItems(granularity).forEach(item ->
                insert.values(now, report.getTable().getName(), item.getGranularity().name(),
                        item.getDescription(), item.getSize()));
        insert.execute();
    }

    @Override
    public void processSchemaTotal(final long totalBytes) {
        dslSupplier.get().insertInto(DB_SIZE_TABLE)
                .columns(TIME_FIELD, TABLE_NAME_FIELD, GRANULARITY_FIELD,
                        DESCRIPTION_FIELD, SIZE_FIELD)
                .values(now, null, Granularity.SCHEMA.name(), SCHEMA_TOTAL_DESCRIPTION, totalBytes)
                .execute();
    }
}
