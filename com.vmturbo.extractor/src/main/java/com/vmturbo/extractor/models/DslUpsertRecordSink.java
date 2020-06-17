package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.jooq.DSLContext;

import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink that inserts into a temp table and then performs upserts from that table into
 * the target table.
 */
public class DslUpsertRecordSink extends DslRecordSink {

    private final List<String> updateColumns;
    private final List<String> conflictColumns;
    private final String tempSuffix;

    /**
     * Create a new instance.
     *
     * @param dsl             jOOQ {@link DSLContext}
     * @param table           table that will be the target of the upsert
     * @param model           model in which table appears
     * @param config          writer config
     * @param pool            thread pool
     * @param tempSuffix      suffix for the temp table
     * @param conflictColumns set of columns that should be unique in the target table
     * @param updateColumns   columns to update when insert fails and falls back to update
     */
    public DslUpsertRecordSink(final DSLContext dsl, final Table table, final Model model,
            final WriterConfig config, final ExecutorService pool, String tempSuffix,
            final List<String> conflictColumns, final List<String> updateColumns) {
        super(dsl, table, model, config, pool);
        this.tempSuffix = tempSuffix;
        this.conflictColumns = conflictColumns;
        this.updateColumns = updateColumns;
    }

    @Override
    protected String getWriteTableName() {
        return super.getWriteTableName() + "_" + tempSuffix;
    }

    @Override
    protected void preCopyHook(final Connection conn) throws SQLException {
        final String createSql = String.format("CREATE TEMPORARY TABLE %s (LIKE %s)",
                getWriteTableName(), table.getName());
        conn.createStatement().execute(createSql);
    }

    @Override
    protected void postCopyHook(final Connection conn) throws SQLException {
        final String sets = updateColumns.stream()
                .map(c -> String.format("%s = EXCLUDED.%s", c, c))
                .collect(Collectors.joining(", "));
        final String upsertSql = String.format(
                "INSERT INTO %s SELECT * FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
                table.getName(), getWriteTableName(), String.join(", ", conflictColumns), sets);
        final Statement statement = conn.createStatement();
        statement.execute(upsertSql);
        logger.info("Upserted {} records into table {}", statement.getUpdateCount(), table.getName());
    }
}
