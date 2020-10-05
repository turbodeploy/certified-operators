package com.vmturbo.cost.component;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * Interface for table stores that can be dumped into file and restored from file dump.
 *
 * @param <T> the type of context object.
 * @param <S> the type of table record.
 */
public interface TableDiagsRestorable<T, S extends Record> extends DiagsRestorable<T> {

    /**
     * Logger.
     */
    Logger logger = LogManager.getLogger();

    /**
     * Get DSL Context.
     *
     * @return DSL Context
     */
    DSLContext getDSLContext();

    /**
     * Get store table.
     *
     * @return store table
     */
    TableImpl<S> getTable();

    @Override
    default void restoreDiags(@Nonnull List<String> collectedDiags, T context) {
        final Field<?>[] fields = getTable().fields();
        final ObjectMapper mapper = new ObjectMapper();
        getDSLContext().transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            for (String line : collectedDiags) {
                S rec = jsonToRecord(line, fields, mapper);
                transaction.insertInto(getTable()).set(rec).onDuplicateKeyIgnore().execute();
            }
        });
    }

    @Override
    default void collectDiags(@Nonnull final DiagnosticsAppender appender) {
        getDSLContext().transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            Stream<S> records = transaction.selectFrom(getTable()).stream();
            records.forEach(s -> {
                try {
                    appender.appendString(s.formatJSON());
                } catch (DiagnosticsException e) {
                    logger.error("Exception encountered while dumping {}", getTable().getName(), e);
                }
            });
        });
    }

    /**
     * Convert JOOQ Json string into record.
     * Cannot use built-in JOOQ parser because it is using protobuf convertor for binary data
     * and cannot parse protobuf string.
     *
     * @param json JOOQ Json string
     * @param fields array of table record fields
     * @param mapper {@link ObjectMapper} instance
     * @return return record object
     * @throws JsonProcessingException in case of json parsing error
     * @throws ParseException in case of protobuf parsing error
     */
    default S jsonToRecord(String json, final Field<?>[] fields, final ObjectMapper mapper) throws JsonProcessingException, ParseException {
        List<Object> data = mapper.readValue(json, List.class);

        for (int i = 0; i < fields.length; i++) {
            //if field type is blob - that is protobuf and we should parse protobuf string
            if (fields[i].getDataType().getTypeName().equals(SQLDataType.BLOB.getTypeName())) {
                data.set(i,
                        TextFormat.parse((CharSequence)data.get(i), (Class)fields[i].getType()));
            }
        }
        final S rec = getTable().newRecord();
        rec.from(data, fields);
        return rec;
    }
}
