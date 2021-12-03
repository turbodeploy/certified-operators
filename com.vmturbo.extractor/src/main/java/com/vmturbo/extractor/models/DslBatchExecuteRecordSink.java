package com.vmturbo.extractor.models;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.jooq.DSLContext;

import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink to do PostgresSql table operations in batches. This is an extension of
 * {@link DslRecordSink}, with the additional capability to close the table writer every time
 * batchSize records are accumulated. This results in batchSize or fewer records being written to
 * the database in each transaction.
 *
 * <p>This sink is useful in large environments where an execution might take a long time, causing
 * timeouts.</p>
 */
public class DslBatchExecuteRecordSink extends DslRecordSink {
    private final int batchSize;
    private int recordBatchCount;
    // This is volatile since recorder writer is not supposed to be synchronized, but also
    // multithreaded. This means that multiple threads might access/write into the recordWriter
    // and volatile will ensure all threads have the updated value of this variable.
    private volatile RecordWriter recordWriter;

    /**
     * Create a new instance of DslBatchExecuteRecordSink.
     *
     * @param dsl            {@link DSLContext} instance for DB connection
     * @param table          target table for updates
     * @param config         writer config
     * @param pool           thread pool
     * @param batchSize      the batchSize to execute the records. Every time the records accepted
     *                       reaches batch size, the recordWriter will be closed and re-opened. If
     *                       zero or negative batch size is given, it will run without batch
     *                       execution.
     */
    public DslBatchExecuteRecordSink(DSLContext dsl, Table table, WriterConfig config,
            ExecutorService pool, int batchSize) {
        super(dsl, table, config, pool);
        this.recordBatchCount = 0;
        this.batchSize = batchSize;
    }

    @Override
    public void accept(Record record) throws SQLException, InterruptedException {
        try {
            synchronized (this) {
                if (record != null && recordBatchCount >= batchSize && batchSize > 0) {
                    // due to the condition that recordBatchCount > which is only possible if
                    // recorderWriter is initialised, we don't need to do null check.
                    recordWriter.close();
                    recordBatchCount = 0;
                    this.recordWriter = createRecordWriter();
                }
            }

            if (record == null) {
                synchronized (this) {
                    if (recordWriter != null) {
                        recordWriter.close();
                    } else {
                        // make sure pre- and post- hooks are executed, since they may be
                        // important (e.g. a post- hook that removes stale records following
                        // an upsert of current records)
                        createRecordWriter().close();
                    }
                    recordBatchCount = 0;
                }
            } else {
                if (recordWriter == null) {
                    synchronized (this) {
                        if (recordWriter == null) {
                            this.recordWriter = createRecordWriter();
                        }
                    }
                } else if (recordWriter.isClosed()) {
                    throw new SQLException("Attempt to write to closed record writer");
                }

                synchronized (this) {
                    recordWriter.write(record);
                    recordBatchCount++;
                }
            }
        } catch (IOException e) {
            throw new SQLException("Failed to create record writer for table: " + getWriteTableName(), e);
        }
    }
}
