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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
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
    protected final Model model;
    private final WriterConfig config;
    private PrintWriter writer;
    private Future<InsertResults> future;
    private int recordSentCount = 0;

    /**
     * Create a new record sink.
     *
     * @param dsl    jOOQ DSL Context that can be used for the COPY operation
     * @param table  table we're attached to, target of the record insertions
     * @param model  model the table belongs to
     * @param config writer config
     * @param pool   thread pool
     */
    public DslRecordSink(DSLContext dsl, Table table, Model model, WriterConfig config,
            ExecutorService pool) {
        this.dsl = dsl;
        this.table = table;
        this.model = model;
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
            close();
        } else {
            final String csv = record.toCSVRow(getRecordColumns());
            try {
                if (writer == null) {
                    setupWriter();
                }
                writer.println(csv);
                if (++recordSentCount % 1000 == 0) {
                    logger.debug("Wrote {} records to {}", recordSentCount, getWriteTableName());
                }
            } catch (IOException e) {
                logger.error("Failed to write record to table {}", getWriteTableName(), e);
            }
        }
    }

    /**
     * Executed when the first record is posted to the sink, establishes a COPY operation in a
     * parallel thread and a {@link Future} that we can use later to await completion and obtain
     * results.
     *
     * @throws IOException if there's a problem setting things up
     */
    private void setupWriter() throws IOException {
        PipedReader reader = new PipedReader();
        this.writer = new PrintWriter(new PipedWriter(reader));
        this.future = pool.submit(() -> writeData(reader));
    }

    /**
     * This method set up the COPY operation in a parallel thread.
     *
     * @param reader a {@link Reader} from which data for the operation can be read
     * @return a {@link Future} that will report results when we're finished, in the form of a # of
     * records copied to db, and duration in milliseconds.
     */
    private InsertResults writeData(Reader reader) {
        long start = System.nanoTime();
        long recordCount = dsl.transactionResult(configuration -> {
            Connection conn = null;
            try {
                conn = configuration.connectionProvider().acquire();
                // perform sink-specific setup, like creating a temp table as a direct recipient
                // of copied records
                preCopyHook(conn);
                // execute the copy operation, with data coming from our reader
                final String copySql = String.format("COPY \"%s\" FROM STDIN WITH CSV", getWriteTableName());
                long count = new CopyManager(conn.unwrap(PgConnection.class))
                        .copyIn(copySql, reader);
                // perform sink-specific completion, like merging temp table data into the target
                // table
                postCopyHook(conn);
                return count;
            } catch (Exception e) {
                logger.error("Failed performing copy to table {}", getWriteTableName(), e);
                return 0L;
            } finally {
                if (conn != null) {
                    configuration.connectionProvider().release(conn);
                }
                reader.close();
            }
        });
        final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        return new InsertResults(recordCount, elapsed);
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
     * Called when the sink is detached from its table, to finish up the COPY operation and
     * report its results.
     */
    private void close() {
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
            }
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            logger.error("Failed to complete writing to table {}", getWriteTableName(), e);
        }
    }

    /**
     * Class for results of a COPY operation.
     */
    private static class InsertResults {

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
