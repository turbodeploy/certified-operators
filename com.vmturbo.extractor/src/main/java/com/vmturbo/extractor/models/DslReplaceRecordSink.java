package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink that replaces all old data in a table with new data. To ensure if has minimal effect
 * on the read, it follows the following steps:
 * 1. creates a new table like old table
 * 2. inserts new records into new table
 * 3. rename old table to table_old
 * 4. rename new table to old table name
 * 5. drop table_old
 */
public class DslReplaceRecordSink extends DslRecordSink {

    private final String tempSuffix;

    /**
     * Create a new instance.
     *
     * @param dsl             jOOQ {@link DSLContext}
     * @param table           table that will be the target of the upsert
     * @param config          writer config
     * @param pool            thread pool
     * @param tempSuffix      suffix for the temp table
     */
    public DslReplaceRecordSink(final DSLContext dsl, final Table table, final WriterConfig config,
            final ExecutorService pool, String tempSuffix) {
        super(dsl, table, config, pool);
        this.tempSuffix = tempSuffix;
    }

    @Override
    protected String getWriteTableName() {
        return super.getWriteTableName() + "_" + tempSuffix;
    }

    @Override
    protected List<String> getPreCopyHookSql(final Connection transConn) {
        final String createSql = String.format("CREATE TABLE %s (LIKE %s INCLUDING ALL)",
                getWriteTableName(), table.getName());
        return Collections.singletonList(createSql);
    }

    @Override
    protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
        // rename old table: search_entity -> search_entity_old
        final String renameOldTable = String.format("ALTER TABLE %s RENAME TO %s",
                table.getName(), renameOldTable(table.getName()));

        // rename new table to old table: search_entity_new -> search_entity
        final String renameNewTable = String.format("ALTER TABLE %s RENAME TO %s",
                getWriteTableName(), table.getName());

        // drop old table: search_entity_old
        final String dropOldTable = String.format("DROP TABLE %s", renameOldTable(table.getName()));

        logger.info("Replacing all records in table {} with table {}", table.getName(), getWriteTableName());
        return Arrays.asList(renameOldTable, renameNewTable, dropOldTable);
    }

    /**
     * Rename old table to a name with suffix "_old".
     *
     * @param table name of the old table
     * @return name with suffix "_old"
     */
    private String renameOldTable(@Nonnull String table) {
        return table + "_old";
    }

    protected String getTempSuffix() {
        return tempSuffix;
    }
}
