package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
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
    private final Collection<Column<?>> matchColumns;
    private final Collection<Column<?>> updateColumns;
    private final String tempSuffix;
    private final Collection<Column<?>> includeColumns;

    /**
     * Create a new instance.
     *
     * @param dsl            {@link DSLContext} instance for DB connection
     * @param table          target table for updates
     * @param config         writer config
     * @param pool           thread pool
     * @param tempSuffix     suffix for temp table name
     * @param includeColumns columns to include in the temp table
     * @param matchColumns   columns to match between temp table and target table
     * @param updateColumns  columns to update from temp table into matching records in target
     *                       table
     */
    public DslUpdateRecordSink(final DSLContext dsl, final Table table, WriterConfig config,
            final ExecutorService pool, String tempSuffix, final Collection<Column<?>> includeColumns,
            final Collection<Column<?>> matchColumns, final Collection<Column<?>> updateColumns) {
        super(dsl, table, config, pool);
        this.tempSuffix = tempSuffix;
        this.includeColumns = includeColumns != null ? includeColumns : table.getColumns();
        this.matchColumns = matchColumns;
        this.updateColumns = updateColumns;
    }

    @Override
    protected String getWriteTableName() {
        return super.getWriteTableName() + "_" + tempSuffix;
    }

    @Override
    protected Collection<Column<?>> getRecordColumns() {
        return includeColumns;
    }

    @Override
    protected List<String> getPreCopyHookSql(final Connection transConn) {
        String colSpecs = includeColumns.stream()
                .map(c -> String.format("%s %s", c.getName(), c.getDbType()))
                .collect(Collectors.joining(", "));
        final String createSql = String.format("CREATE TEMPORARY TABLE %s (%s)",
                getWriteTableName(), colSpecs);
        return Collections.singletonList(createSql);
    }

    @Override
    protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
        final String sets = updateColumns.stream()
                .map(Column::getName)
                .map(c -> String.format("%s = _temp.%s", c, c))
                .collect(Collectors.joining(", "));
        final String conditions = matchColumns.stream()
                .map(Column::getName)
                .map(c -> String.format("_t.%s = _temp.%s", c, c))
                .collect(Collectors.joining(" AND "));
        final String updateSql = String.format("UPDATE %s AS _t SET %s FROM %s AS _temp WHERE %s",
                table.getName(), sets, getWriteTableName(), conditions);
        return Collections.singletonList(updateSql);
    }
}
