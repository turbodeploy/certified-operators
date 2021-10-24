package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.jooq.DSLContext;

import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.schema.SchemaCreator;

/**
 * Record sink that replaces all old data in a table with new data designed for new search data.
 */
public class DslReplaceSearchRecordSink extends JdbcBatchInsertRecordSink {
    private final SchemaCreator schemaCreator;
    private final Location location;
    private final String tempSuffix;

    /**
     * Create a new instance.
     *
     * @param dsl jOOQ {@link DSLContext}
     * @param table table that will be the target of the upsert
     * @param location table location
     * @param conn db connection
     * @param tempSuffix suffix for the temp table
     * @param batchSize batch size for bulk inserts
     */
    public DslReplaceSearchRecordSink(DSLContext dsl, Table table, Location location,
            Connection conn, String tempSuffix, int batchSize) {
        super(dsl, batchSize, conn);
        this.schemaCreator = new SchemaCreator(dsl);
        this.location = location;
        this.tempSuffix = "_" + tempSuffix;
    }

    @Override
    protected List<String> getPreCopyHookSql(final Connection transConn) {
        return schemaCreator.createWithoutIndexes(tempSuffix, location);
    }

    @Override
    protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
        return schemaCreator.replace(tempSuffix, "", location);
    }

    @Override
    protected Table getWriteTable(Table original) {
        return Table.named(original.getName() + tempSuffix).withColumns(original.getColumns()).build();
    }
}
