package com.vmturbo.extractor.models;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink that can send records prepared for a given table to the database.
 * PostgreSQL-specific.
 *
 * <p>An instance of this (or some other) sink is attached to a table in order to prepare a
 * streaming insert operation. While the attachment is in place, records are opened against the
 * table, populated with the data, and then closed. Closing a record causes it to be sent to the
 * attached sink, and detaching the sink from the table causes the streaming insert to be
 * completed.</p>
 *
 * <p>In this sink implementation, these operations manifest as follows:</p>
 *
 * <dl>
 *     <dt>Attach to table</dt>
 *     <dd>Nothing is done by the sink immediately upon attachment; stream setup awaits the first
 *     record, so that no setup takes place if the stream turns out to be empty.</dd>
 *     <dt>Record posted</dt>
 *     <dd>When the first record is posted ot the sink, a COPY TO operation is started in a
 *     parallel thread, and a pipe is established so that data written to the pipe will be sent
 *     to the database as part of the copy stream. That first record and every subsequent record
 *     posted from that point forward is converted to CSV and written to the pipe.</dd>
 *     <dt>Detach from table</dt>
 *     <dd>The write end of the pipe is closed, resulting in the completion of the COPY operation.
 *     The sink awaits completion and reports results.</dd>
 * </dl>
 */
@NotThreadSafe
public class DslRecordSink extends AbstractRecordSink {
    protected final Table table;
    private final ExecutorService pool;
    private final WriterConfig config;
    private volatile RecordWriter recordWriter;

    /**
     * Create a new record sink.
     *
     * @param dsl    jOOQ DSL Context that can be used for the COPY operation
     * @param table  table we're attached to, target of the record insertions
     * @param config writer config
     * @param pool   thread pool
     */
    public DslRecordSink(DSLContext dsl, Table table, WriterConfig config, ExecutorService pool) {
        super(dsl);
        this.table = table;
        this.config = config;
        this.pool = pool;
    }

    /**
     * Get the columns belonging to our attached table.
     *
     * @return table columns
     */
    protected Collection<Column<?>> getRecordColumns() {
        return table.getColumns();
    }

    /**
     * Add a record to the stream, or signal that the sink is being detached.
     *
     * <p>The table will send a null record to signal detachment. All non-null records are conveyed
     * to the COPY operation stream.</p>
     *
     * @param record record to be inserted, or null to signal end of record stream
     */
    @Override
    public void accept(final Record record) throws SQLException, InterruptedException {
        try {
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
                recordWriter.write(record);
            }
        } catch (IOException e) {
            throw new SQLException("Failed to create record writer for table: " + getWriteTableName(), e);
        }
    }

    RecordWriter createRecordWriter() throws IOException {
        return new RecordWriter(getRecordColumns(), pool);
    }

    /**
     * Class that performs database writes.
     *
     * <p>We use the high-speed Postgres COPY statement, sending records in CSV format.</p>
     */
    class RecordWriter {
        private final PipedOutputStream outputStream;
        private final Collection<Column<?>> columns;
        private final Future<InsertResults> future;
        private int recordSentCount = 0;
        private int recordWriteFailureCount = 0;
        private boolean closed = false;

        RecordWriter(Collection<Column<?>> columns, ExecutorService pool) throws IOException {
            this.columns = columns;
            final PipedInputStream inputStream = new PipedInputStream();
            this.outputStream = new PipedOutputStream(inputStream);
            this.future = pool.submit(() -> {
                final String colNames = columns.stream()
                        .map(c -> "\"" + c.getName() + "\"")
                        .collect(Collectors.joining(","));
                final String stmt = String.format("COPY \"%s\" (%s) FROM STDIN WITH CSV",
                        getWriteTableName(), colNames);
                try {
                    return writeData(stmt, dsl, inputStream);
                } catch (DataAccessException e) {
                    // Note: jOOQ will convert any checked exceptions to a DataAccessException as
                    // part of its transaction management; see: DSLContext::transaction.
                    // Close reader side stream so the writer side got terminated with IOException
                    // rather than waiting forever
                    inputStream.close();
                    // rethrow to terminate the reader thread
                    // Note: this exception will ultimately be converted to an ExecutionException
                    // and be caught by the RecordWriter::close method.
                    throw e;
                }
            });
        }

        /**
         * This method set up the COPY operation in a parallel thread.
         *
         * @param copySql SQL for copy operation
         * @param dsl     {@link DSLContext} for DB access
         * @param inputStream  a {@link PipedInputStream} from which data for the operation can be read
         * @return a {@link Future} that will report results when we're finished
         * @throws DataAccessException database issue when executing the transactional logic
         */
        private InsertResults writeData(String copySql, DSLContext dsl, PipedInputStream inputStream) throws DataAccessException {
            long start = System.nanoTime();
            final AtomicLong recordCount = new AtomicLong(0L);
            dsl.transaction(trans -> DSL.using(trans).connection(transConn -> {
                runHook(transConn, getPreCopyHookSql(transConn), "pre-copy");
                try {
                    // execute the copy operation, with data coming from our reader
                    final CopyManager copier = new CopyManager(transConn.unwrap(PgConnection.class));
                    recordCount.set(copier.copyIn(copySql, inputStream));
                } catch (Exception e) {
                    logger.error("Failed performing copy to table {}", getWriteTableName(), e);
                    // rethrow to rollback transaction
                    throw new DataAccessException(e.getMessage(), e);
                }
                try {
                    runHook(transConn, getPostCopyHookSql(transConn), "post-copy");
                } catch (SQLException e) {
                    // rolling back, so nothing written
                    recordCount.set(0L);
                    // rethrow to rollback transaction
                    throw e;
                }
            }));
            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            return new InsertResults(recordCount.get(), elapsed);
        }

        /**
         * Attempt to write the given record to db.
         *
         * @param record the record to write
         */
        public void write(final Record record) {
            if (++recordSentCount % 1000 == 0) {
                logger.debug("Tried to write {} records to {}", recordSentCount, getWriteTableName());
            }
            try {
                final String csv = record.toCSVRow(columns);
                outputStream.write(csv.getBytes(UTF_8));
                outputStream.write(System.lineSeparator().getBytes(UTF_8));
            } catch (IOException e) {
                // failed to write record
                // report write failure when table detaches
                recordWriteFailureCount++;
            }
        }

        /**
         * Called when the sink is detached from its table, to finish up the COPY operation and
         * report its results.
         *
         * @throws RecordWriteException if an error occurs when writing records
         */
        @VisibleForTesting
        void close() throws RecordWriteException {
            try {
                if (outputStream != null) {
                    // this will cause the COPY operation to hit EOF on its reader and complete its
                    // operation with the DB
                    outputStream.close();
                }
                // await completion and report results
                if (future != null) {
                    InsertResults result = future.get(config.insertTimeoutSeconds(), TimeUnit.SECONDS);
                    logger.info("Wrote {} records to table {}", result.getRecordCount(), getWriteTableName());
                }
            } catch (TimeoutException | InterruptedException | ExecutionException | IOException e) {
                logger.error("Failed to complete writing to table {}", getWriteTableName(), e);
                throw new RecordWriteException(e.getMessage(), e);
            }

            if (recordWriteFailureCount > 0) {
                logger.error("Failed to write {} records to table {}", recordWriteFailureCount, getWriteTableName());
            }

            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    protected String getWriteTableName() {
        return table.getName();
    }

    /**
     * Class for results of a COPY operation.
     */
    static class InsertResults {

        private final long recordCount;
        private final long msec;

        InsertResults(long recordCount, long msec) {
            this.recordCount = recordCount;
            this.msec = msec;
        }

        public long getMsec() {
            return msec;
        }

        public long getRecordCount() {
            return recordCount;
        }
    }
}
