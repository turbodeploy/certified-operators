package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.jooq.DSLContext;

import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.schema.SchemaCreator;

/**
 * Record sink that replaces all old data in a table with new data designed for new search data.
 */
public class DslReplaceSearchRecordSink extends DslReplaceRecordSink {
    private final SchemaCreator schemaCreator;
    private final Location location;

    /**
     * Create a new instance.
     *
     * @param dsl jOOQ {@link DSLContext}
     * @param table table that will be the target of the upsert
     * @param location table location
     * @param config writer config
     * @param pool thread pool
     * @param tempSuffix suffix for the temp table
     */
    public DslReplaceSearchRecordSink(DSLContext dsl, Table table, Location location,
            WriterConfig config, ExecutorService pool, String tempSuffix) {
        super(dsl, table, config, pool, tempSuffix);
        this.schemaCreator = new SchemaCreator(dsl);
        this.location = location;
    }

    @Override
    protected List<String> getPreCopyHookSql(final Connection transConn) {
        return schemaCreator.createWithoutIndexes("_" + getTempSuffix(), location);
    }

    @Override
    protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
        return schemaCreator.replace("_" + getTempSuffix(), "", location);
    }
}
