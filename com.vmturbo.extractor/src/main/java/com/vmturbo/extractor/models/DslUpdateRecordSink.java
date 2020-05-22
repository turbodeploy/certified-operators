package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.jooq.DSLContext;

import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink that copies data to a temp table and then performs an update of records in the target
 * data setting corresponding values from the temp table.
 *
 * <p>This allows us to use the fast COPY TO mechanism to transmit the data to the server rather
 * than using slower batched update statements.</p>
 */
public class DslUpdateRecordSink extends DslRecordSink {
    private final List<String> matchColumns;
    private final List<String> updateColumns;
    private final String tempSuffix;
    private final List<String> includeColumns;

    /**
     * Create a new instance.
     *
     * @param dsl            {@link DSLContext} instance for DB connection
     * @param table          target table for updates
     * @param model          model containing target table
     * @param config         writer config
     * @param pool           thread pool
     * @param tempSuffix     suffix for temp table name
     * @param includeColumns columns to include in the temp table
     * @param matchColumns   columns to match between temp table and target table
     * @param updateColumns  columns to update from temp table into matching records in target
     *                       table
     */
    public DslUpdateRecordSink(final DSLContext dsl, final Table table, final Model model,
            WriterConfig config, final ExecutorService pool, String tempSuffix,
            final List<String> includeColumns, final List<String> matchColumns, final List<String> updateColumns) {
        super(dsl, table, model, config, pool);
        this.tempSuffix = tempSuffix;
        this.includeColumns = includeColumns != null ? includeColumns
                : table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        this.matchColumns = matchColumns;
        this.updateColumns = updateColumns;
    }

    @Override
    protected String getWriteTableName() {
        return super.getWriteTableName() + "_" + tempSuffix;
    }

    @Override
    protected Collection<Column<?>> getRecordColumns() {
        return includeColumns.stream()
                .map(table::getColumn)
                .collect(Collectors.toList());
    }

    @Override
    protected void preCopyHook(final Connection conn) throws SQLException {
        String colSpecs = includeColumns.stream()
                .map(table::getColumn)
                .map(c -> String.format("%s %s", c.getName(), c.getDbType()))
                .collect(Collectors.joining(", "));
        final String createSql = String.format("CREATE TEMPORARY TABLE %s (%s)",
                getWriteTableName(), colSpecs);
        conn.createStatement().execute(createSql);
    }

    @Override
    protected void postCopyHook(final Connection conn) throws SQLException {
        final String sets = updateColumns.stream()
                .map(c -> String.format("%s = _temp.%s", c, c))
                .collect(Collectors.joining(", "));
        final String conditions = matchColumns.stream()
                .map(c -> String.format("_t.%s = _temp.%s", c, c))
                .collect(Collectors.joining(" AND "));
        final String updateSql = String.format("UPDATE %s AS _t SET %s FROM %s AS _temp WHERE %s",
                table.getName(), sets, getWriteTableName(), conditions);
        logger.info("Update SQL: {}", updateSql);
        final Statement statement = conn.createStatement();
        statement.execute(updateSql);
        logger.info("Updated {} values in table {}", statement.getUpdateCount(), table.getName());
    }
}
