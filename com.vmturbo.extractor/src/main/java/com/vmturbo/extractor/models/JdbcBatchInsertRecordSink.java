package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.jooq.DSLContext;

import com.vmturbo.extractor.models.Table.Record;

/**
 * Inserter of Records in batches (jdbc bulk insert).
 */
public class JdbcBatchInsertRecordSink extends AbstractRecordSink {
    private static final String COMMA = ",";
    private final List<Record> batch;
    private final int batchSize;
    private final Connection conn;
    private boolean first = true;
    private Table original;
    private Table table;
    private String insert = "";

    /**
     * Construct the instance.
     *
     * @param dsl jooq context
     * @param batchSize batch size
     * @param conn db connection
     */
    public JdbcBatchInsertRecordSink(DSLContext dsl, int batchSize, Connection conn) {
        super(dsl);
        this.batch = new ObjectArrayList<>(batchSize);
        this.batchSize = batchSize;
        this.conn = conn;
    }

    @Override
    public synchronized void accept(Record record) throws SQLException, InterruptedException {
        if (record != null) {
            if (table == null) {
                table = getWriteTable(record.getTable());
                original = record.getTable();
                insert = buildInsert(table);
            } else if (!original.equals(record.getTable())) {
                throw new SQLException("Sink is already configured to write in table " + table
                                + " and cannot accept rows for " + record.getTable());
            }
            if (batch.size() < batchSize) {
                batch.add(record);
                return;
            }
        }
        // finished (null record) or assembled a batch
        // do not run hooks if there were no records supplied at all
        if (table != null) {
            if (first) {
                runHook(conn, getPreCopyHookSql(conn), "pre-insert");
            }
            first = false;
            if (batch.size() > 0) {
                Collection<Column<?>> columns = table.getColumns();
                try (PreparedStatement stmt = conn.prepareStatement(insert)) {
                    for (Record writingRecord : batch) {
                        Iterator<Column<?>> itc = columns.iterator();
                        for (int index = 1; itc.hasNext(); ++index) {
                            setValue(stmt, index, itc.next(), writingRecord);
                        }
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }
                logger.trace("Wrote a batch of size {} in {}", batch.size(), table.getName());
                batch.clear();
            }
            if (record == null) {
                runHook(conn, getPostCopyHookSql(conn), "post-insert");
            }
        }
    }

    private void setValue(PreparedStatement stmt, int index, Column<?> col, Record writingRecord)
                    throws SQLException {
        Object value = writingRecord.get(col);
        // mapping of handwritten orm ColType value to jdbc value
        // statically defining this will require functional interfaces declared
        // for each PreparedStatement setter
        // we support only small subset of extractor orm column types here
        // unfortunately it's weakly typed
        // TODO we should consider not writing orms
        // we will never achieve acceptable level of functionality implemented
        // reuse jooq definitions instead of Table/Column/ColType even if jooq is not used for persisting
        switch (col.getColType()) {
            case DOUBLE:
                if (value instanceof Number) {
                    stmt.setDouble(index, ((Number)value).doubleValue());
                } else {
                    stmt.setNull(index, Types.DOUBLE);
                }
                break;
            case LONG:
                if (value instanceof Number) {
                    stmt.setLong(index, ((Number)value).longValue());
                } else {
                    stmt.setNull(index, Types.BIGINT);
                }
                break;
            case INT:
                if (value instanceof Number) {
                    stmt.setInt(index, ((Number)value).intValue());
                } else {
                    stmt.setNull(index, Types.INTEGER);
                }
                break;
            case STRING:
                if (value instanceof String) {
                    stmt.setString(index, (String)value);
                } else {
                    stmt.setNull(index, Types.VARCHAR);
                }
                break;
            default:
                logger.warn("Unsupported column type " + col.getColType());
                break;
        }
    }

    private static String buildInsert(Table writeTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(writeTable.getName());
        sb.append(" (");
        // assume ascii column names
        sb.append(writeTable.getColumns().stream().map(Column::getName)
                        .collect(Collectors.joining(COMMA)));
        sb.append(") VALUES (");
        sb.append(writeTable.getColumns().stream().map(col -> "?")
                        .collect(Collectors.joining(COMMA)));
        sb.append(")");
        return sb.toString();
    }

    /**
     * Get the table to which data should be written.
     *
     * @param original configured table
     * @return destination table
     */
    protected Table getWriteTable(Table original) {
        return original;
    }
}
