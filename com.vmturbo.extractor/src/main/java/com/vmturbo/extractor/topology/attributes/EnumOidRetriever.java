package com.vmturbo.extractor.topology.attributes;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataTypeException;

import com.vmturbo.components.api.FormattedString;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Utility to retrieve enum OIDs to use for the historical attributes table.
 * We use OIDs from the database so that we don't need to do migrations when enum values or
 * ordering changes in the future.
 *
 * @param <T> The type of enum.
 */
interface EnumOidRetriever<T extends Enum<T>> {

    /**
     * Retrieve the OIDs for values of the enum.
     *
     * @param endpoint Database endpoint.
     * @return Map from enum value to the integer id for the enum.
     * @throws EnumOidRetrievalException If there is an error retrieving or interpreting the oids.
     */
    @Nonnull
    Map<T, Integer> getEnumOids(@Nonnull DbEndpoint endpoint) throws EnumOidRetrievalException;

    /**
     * Default implementation for Postgres.
     *
     * @param <T> The type of enum.
     */
    class PostgresEnumOidRetriever<T extends Enum<T>> implements EnumOidRetriever<T> {

        private static final Logger logger = LogManager.getLogger();

        /**
         * Query to get enum OIDs from the Postgres catalog.
         * We record enum OIDs as enum values in the attribute table, because that is the most
         * backwards-compatible way.
         */
        private static final String ENUM_OID_QUERY_TEMPLATE =
            "SELECT pg_catalog.pg_enum.oid, pg_catalog.pg_enum.enumlabel "
            + "FROM pg_catalog.pg_enum "
            + "INNER JOIN pg_catalog.pg_type "
            + "ON pg_catalog.pg_enum.enumtypid = pg_catalog.pg_type.oid "
            + "WHERE pg_catalog.pg_type.typname = '{}'";

        private final Class<T> clazz;
        private final String query;

        PostgresEnumOidRetriever(Class<T> clazz, String dbTypeName) {
            this.clazz = clazz;
            this.query = FormattedString.format(ENUM_OID_QUERY_TEMPLATE, dbTypeName);
        }

        @Override
        @Nonnull
        public Map<T, Integer> getEnumOids(@Nonnull DbEndpoint endpoint)
                throws EnumOidRetrievalException {
            try (DSLContext dslContext = endpoint.dslContext()) {
                // This gets the name of the enum ('entity_state'), not the name of the label.
                final Result<Record> result = dslContext.resultQuery(query).fetch();
                final Map<T, Integer> resultMap = new HashMap<>();
                for (Record record : result) {
                    try {
                        final Integer oid = record.get(0, Integer.class);
                        final T value = Enum.valueOf(clazz, record.get(1, String.class));
                        resultMap.put(value, oid);
                    } catch (IllegalArgumentException | DataTypeException e) {
                        throw new EnumOidRetrievalException(FormattedString.format("Failed to map record: {}", record), e);
                    }
                }
                return resultMap;
            } catch (UnsupportedDialectException | InterruptedException | SQLException e) {
                logger.error("Failed to initialize entity state attribute processor.", e);
                throw new EnumOidRetrievalException("Failed to initialize entity state attribute processor.", e);
            }
        }
    }

    /**
     * Exception thrown when failing to retrieve the enum oids.
     */
    class EnumOidRetrievalException extends Exception {
        private EnumOidRetrievalException(String format, Exception e) {
            super(format, e);
        }
    }
}
