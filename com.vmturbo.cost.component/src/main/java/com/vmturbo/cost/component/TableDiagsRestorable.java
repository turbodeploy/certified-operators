package com.vmturbo.cost.component;

import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.sql.utils.jooq.JooqUtil;

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
     * The default batch size in restoring records.
     */
    int DEFAULT_RESTORE_BATCH_SIZE = 1000;

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

    /**
     * Get the size of batches of records to restore.
     * @return The batch of records to restore.
     */
    default int getBatchRestoreSize() {
        return DEFAULT_RESTORE_BATCH_SIZE;
    }

    @Override
    default void restoreDiags(@Nonnull List<String> collectedDiags, T context) {
        final Field<?>[] fields = getTable().fields();
        final ObjectMapper mapper = constructObjectMapper();
        getDSLContext().transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);

            logger.info("Disabling foreign key constraint checks while loading diags for '{}'", getTable());
            JooqUtil.disableForeignKeyConstraints(transaction);


            Iterables.partition(collectedDiags, getBatchRestoreSize()).forEach(batchLines -> {

                final List<S> batchRecords = batchLines.stream()
                        .map(line -> {
                            try {
                                return jsonToRecord(line, fields, mapper);
                            } catch (Exception e) {
                                logger.error("Error parsing line: {}", line, e);
                                return null;
                            }
                        }).filter(Objects::nonNull)
                        .collect(ImmutableList.toImmutableList());

                final Table<S> table = getTable();
                transaction.batch(
                        batchRecords.stream()
                                .map(record -> transaction.insertInto(table)
                                        .set(record)
                                        .onDuplicateKeyUpdate()
                                        .set(record))
                                .collect(ImmutableList.toImmutableList())).execute();
            });

            // If an exception occurs will loading the diags, constraint checks are disabled
            // only for the session. Re-enabling constraint checks is done for completeness,
            // but is not necessary.
            JooqUtil.enableForeignKeyConstraints(transaction);
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

        preProcessJsonData(data, fields);
        for (int i = 0; i < fields.length; i++) {
            //if field type is blob - that is protobuf and we should parse protobuf string
            final DataType<?> fieldDataType = fields[i].getDataType();
            if (fields[i].getDataType().getTypeName().equals(SQLDataType.BLOB.getTypeName())) {
                data.set(i,
                        TextFormat.parse((CharSequence)data.get(i), (Class)fields[i].getType()));
            } else if (Temporal.class.isAssignableFrom(fieldDataType.getType())) {
                // neither jackson nor jooq will implicitly convert a string timestamp to a date/time type.
                // This needs to be done explicitly prior to creating the record.
                data.set(i, mapper.convertValue(data.get(i), fieldDataType.getType()));
            }
        }
        final S rec = getTable().newRecord();
        rec.from(data, fields);
        return rec;
    }

    /**
     * Performs any optional pre-processing of data row read from diagnostic dump file, before it is
     * converted to a DB record.
     *
     * @param data Input data line (in JSON format) fields, could get updated.
     * @param fields DB fields for the table to which record is to be inserted.
     */
    default void preProcessJsonData(@Nonnull List<Object> data, @Nonnull final Field<?>[] fields) {
    }

    /**
     * Constructs the {@link ObjectMapper} instance used to deserialize records from a diag.
     * @return The {@link ObjectMapper} to use for record deserialization.
     */
    @Nonnull
    default ObjectMapper constructObjectMapper() {

        final JavaTimeModule module = new JavaTimeModule();
        final ObjectMapper objectMapper = Jackson2ObjectMapperBuilder.json()
                .modules(module)
                .build();

        return objectMapper;
    }
}
