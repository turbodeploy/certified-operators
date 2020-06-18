package com.vmturbo.extractor.models;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Record sink that can send records prepared for a given table to the database.
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
public class DslRecordSink implements Consumer<Record> {
    protected static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;
    private final ExecutorService pool;
    protected final Table table;
    private final WriterConfig config;
    private RecordWriter recordWriter;

    /**
     * Create a new record sink.
     *
     * @param dsl    jOOQ DSL Context that can be used for the COPY operation
     * @param table  table we're attached to, target of the record insertions
     * @param config writer config
     * @param pool   thread pool
     */
    public DslRecordSink(DSLContext dsl, Table table, WriterConfig config, ExecutorService pool) {
        this.dsl = dsl;
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
    public void accept(final Record record) {
        if (record == null) {
            if (recordWriter != null) {
                recordWriter.close(false);
            }
        } else {
            try {
                if (recordWriter == null) {
                    this.recordWriter = createRecordWriter();
                } else if (recordWriter.isClosed()) {
                    throw new IllegalStateException("Attempt to write to closed record writer");
                }
                recordWriter.write(record);
            } catch (IOException e) {
                logger.error("Failed to write record to table {}", getWriteTableName(), e);
            }
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
        private PrintWriter writer;
        private final Collection<Column<?>> columns;
        private final Connection conn;
        private Future<InsertResults> future;
        private int recordSentCount = 0;
        private boolean closed = false;

        RecordWriter(Collection<Column<?>> columns, ExecutorService pool) throws IOException {
            this.columns = columns;
            this.conn = getTransactionalConnection();
            if (conn != null && runPreCopyHook(conn)) {
                final PipedReader reader = new PipedReader();
                this.writer = new PrintWriter(new PipedWriter(reader));
                this.future = pool.submit(() -> writeData(String.format("COPY \"%s\" FROM STDIN WITH CSV", getWriteTableName()), conn, reader));
            } else {
                if (future != null) {
                    future.cancel(true);
                }
                close(true);
            }
        }

        private Connection getTransactionalConnection() {
            try {
                final Connection conn = dsl.configuration().connectionProvider().acquire();
                conn.setAutoCommit(false);
                return conn;
            } catch (DataAccessException | SQLException e) {
                logger.error("Failed to obtain a connection for sink operation", e);
                return null;
            }
        }

        private boolean runPreCopyHook(Connection conn) {
            try {
                preCopyHook(conn);
                return true;
            } catch (SQLException e) {
                logger.error("Failed to execute pre-copy hook; will not write any records", e);
                return false;
            }
        }

        private void runPostCopyHook(Connection conn) {
            try {
                postCopyHook(conn);
            } catch (SQLException e) {
                logger.error("Failed to execute postCopy hook; rolling back all work", e);
            }
        }

        /**
         * This method set up the COPY operation in a parallel thread.
         *
         * @param copySql SQL for copy operation
         * @param conn    connection to use
         * @param reader  a {@link Reader} from which data for the operation can be read
         * @return a {@link Future} that will report results when we're finished
         */
        private InsertResults writeData(String copySql, Connection conn, Reader reader) {
            long start = System.nanoTime();
            long recordCount;
            try {
                // execute the copy operation, with data coming from our reader
                recordCount = new CopyManager(conn.unwrap(PgConnection.class))
                        .copyIn(copySql, reader);
            } catch (Exception e) {
                logger.error("Failed performing copy to table {}", getWriteTableName(), e);
                recordCount = 0L;
            }
            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            return new InsertResults(recordCount, elapsed);
        }

        public void write(final Record record) {
            final String csv = record.toCSVRow(columns);
            writer.println(csv);
            if (++recordSentCount % 1000 == 0) {
                logger.debug("Wrote {} records to {}", recordSentCount, getWriteTableName());
            }
        }

        /**
         * Called when the sink is detached from its table, to finish up the COPY operation and
         * report its results.
         *
         * @param failed true if the operation is known to have failed
         */
        @VisibleForTesting
        void close(boolean failed) {
            try {
                if (writer != null) {
                    // this will cause the COPY operation to hit EOF on its reader and complete its
                    // operation with the DB
                    writer.close();
                }
                // await completion and report results
                if (future != null) {
                    InsertResults result = future.get(config.insertTimeoutSeconds(), TimeUnit.SECONDS);
                    logger.info("Wrote {} records to table {}", result.getRecordCount(), getWriteTableName());
                    runPostCopyHook(conn);
                }
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                logger.error("Failed to complete writing to table {}", getWriteTableName(), e);
            } finally {
                if (conn != null) {
                    try {
                        if (failed) {
                            conn.rollback();
                        } else {
                            conn.commit();
                        }
                    } catch (SQLException e) {
                        logger.error("Failed to rollback failed transaction", e);
                    }
                    dsl.configuration().connectionProvider().release(conn);
                }
            }
            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /**
     * Perform sink-specific setup operations before beginning the COPY operation.
     *
     * @param conn database connection on which COPY will execute
     * @throws SQLException if there's a problem with setup
     */
    protected void preCopyHook(final Connection conn) throws SQLException {
    }

    /**
     * Perform sink-specific operations after all the data has been sent for the COPY operation.
     *
     * @param conn database connection on which COPY operation executed (still open)
     * @throws SQLException if there's a problem
     */
    protected void postCopyHook(final Connection conn) throws SQLException {
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
