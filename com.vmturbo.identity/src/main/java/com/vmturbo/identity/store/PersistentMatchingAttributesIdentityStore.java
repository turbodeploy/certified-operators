package com.vmturbo.identity.store;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.jooq.DSLContext;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.jooq.impl.UpdatableRecordImpl;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;

/**
 * Persistence for oids for matching attributes. Any spec that has an extractor that pulls out
 * MatchingAttributes to be used for OID generation can use this class to persist the OID mappings
 * in a db table. It is used for TargetSpec OIDs and topology data definition OIDs for example.
 *
 * @param <RecordTypeT> Subclass of UpdatableRecordImpl that represents the matching attributes
 * we are using to determine the OID.
 */
public class PersistentMatchingAttributesIdentityStore<RecordTypeT
        extends UpdatableRecordImpl<RecordTypeT>> implements PersistentIdentityStore<DSLContext> {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private static final String ERROR_PERSISTING_MSG =
            "Error persisting %s OID: %s, IdentityMatchingAttributes: %s";

    private final DSLContext dsl;

    private final TableImpl<RecordTypeT> databaseTable;

    private final TableField<RecordTypeT, Long> idField;

    private final TableField<RecordTypeT, String> matchingAttributesField;

    /**
     * File name to dump diagnostics.
     */
    private final String diagsFilename;

    /**
     * Description of the table used for logging errors.
     */
    private final String tableDescription;

    /**
     * Constructor.
     *
     * @param dsl DSLContext for the database backing this store.
     * @param dbTable DB table to use for persisting OIDs and matching attributes.
     * @param idField The field in the table that contains the OID.
     * @param matchingAttributesField The field in the table that contains the matching attributes.
     * @param diagsFilename The filename to store the diags in and read them from.
     * @param tableDescription A phrase for log messages that describes this store.
     */
    public PersistentMatchingAttributesIdentityStore(@Nonnull final DSLContext dsl,
            @Nonnull TableImpl<RecordTypeT> dbTable,
            @Nonnull TableField<RecordTypeT, Long> idField,
            TableField<RecordTypeT, String> matchingAttributesField,
            @Nonnull String diagsFilename,
            @Nonnull String tableDescription) {
        this.dsl = dsl;
        this.databaseTable = dbTable;
        this.idField = idField;
        this.matchingAttributesField = matchingAttributesField;
        this.diagsFilename = diagsFilename;
        this.tableDescription = tableDescription;
    }

    @Override
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings() throws IdentityStoreException  {
        return dsl.select(idField, matchingAttributesField)
                .from(databaseTable)
                .fetch()
                .stream()
                .collect(Collectors.toMap(record -> deserializeTargetSpecIdentityMatchingAttrs(
                        record.value2()),
                        record -> record.value1(),
                        (id1, id2) -> id1 <= id2 ? id1 : id2));
    }

    @Override
    public void saveOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrsToOidMap) throws IdentityStoreException {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                saveOidMappings(attrsToOidMap, transactionDsl);
            });
        } catch (DataAccessException ex) {
            throw new IdentityStoreException("There was error while saving OID mapping in the "
                + "database.", ex);
        }

    }

    private void saveOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrToOidMap,
                                 @Nonnull DSLContext context) {
        for (Map.Entry<IdentityMatchingAttributes, Long> mapEntry : attrToOidMap.entrySet()) {
            final IdentityMatchingAttributes attr = mapEntry.getKey();
            final long oid = mapEntry.getValue();
            final String serializedAttr = serializeTargetSpecIdentityMatchingAttrs(attr);
            try {
                context.insertInto(databaseTable)
                        .set(idField, oid)
                        .set(matchingAttributesField, serializedAttr)
                        .onDuplicateKeyIgnore()
                        .execute();
            } catch (DataAccessException e) {
                // capture the info about the workflow causing the error
                throw new DataAccessException(
                        String.format(ERROR_PERSISTING_MSG, tableDescription, oid,
                                serializedAttr), e);
            }
        }
    }

    @Override
    public void updateOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrToOidMap)
            throws IdentityStoreException {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            for (Map.Entry<IdentityMatchingAttributes, Long> mapEntry : attrToOidMap.entrySet()) {
                final IdentityMatchingAttributes attr = mapEntry.getKey();
                final long oid = mapEntry.getValue();
                final String serializedAttr = serializeTargetSpecIdentityMatchingAttrs(attr);
                try {
                    transactionDsl.update(databaseTable)
                            .set(matchingAttributesField, serializedAttr)
                            .where(idField.eq(oid))
                            .execute();
                } catch (DataAccessException e) {
                    // capture the info about the workflow causing the error
                    throw new IdentityStoreException(
                            String.format(ERROR_PERSISTING_MSG, tableDescription, oid,
                                    serializedAttr), e);
                }
            }
        });
    }

    @Override
    public void removeOidMappings(Set<Long> oidsToRemove) throws IdentityStoreException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.deleteFrom(databaseTable)
                        .where(idField.in(oidsToRemove))
                        .execute();
            });
        } catch (DataAccessException e) {
            throw new IdentityStoreException("Error deleting Oid Mappings", e);
        }
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        try {
            for (Entry<IdentityMatchingAttributes, Long> entry : fetchAllOidMappings().entrySet()) {
                final MatchingAttributesHeader
                        matchingAttributesHeader = new MatchingAttributesHeader(entry.getValue(),
                        serializeTargetSpecIdentityMatchingAttrs(entry.getKey()));
                appender.appendString(GSON.toJson(
                        matchingAttributesHeader, MatchingAttributesHeader.class));
            }
        } catch (IdentityStoreException e) {
            throw new DiagnosticsException(
                    "Retrieving target identifiers from database failed" + e);
        }
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags, @Nullable DSLContext context) throws DiagnosticsException {
        try {
            if (context == null) {
                context = dsl;
            }
            // Clear table first.
            removeAllOids(context);
            final Map<IdentityMatchingAttributes, Long> attrToOidMap = Maps.newHashMap();
            collectedDiags.forEach(diag -> {
                final MatchingAttributesHeader matchingAttributesHeader = GSON.fromJson(diag, MatchingAttributesHeader.class);
                final IdentityMatchingAttributes attrs = deserializeTargetSpecIdentityMatchingAttrs(
                    matchingAttributesHeader.getIdentityMatchingAttributes());
                attrToOidMap.put(attrs, matchingAttributesHeader.getId());
            });
            saveOidMappings(attrToOidMap, context);
        } catch (DataAccessException e) {
            throw new DiagnosticsException(String.format("Saving target identifiers to database "
                + "failed. %s", e));
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return diagsFilename;
    }

    /**
     * Method to clear the target spec oid table.
     *
     * @param context dsl context for accessing DB.
     */
    private void removeAllOids(@Nonnull DSLContext context) {
        context.delete(databaseTable).execute();
    }

    @Nonnull
    private String serializeTargetSpecIdentityMatchingAttrs(
            @Nonnull final IdentityMatchingAttributes identityMatchingAttributes) {
        return GSON.toJson(identityMatchingAttributes);
    }

    @Nonnull
    private IdentityMatchingAttributes deserializeTargetSpecIdentityMatchingAttrs(
            @Nonnull final String identityMatchingAttributesStr) {
        return GSON.fromJson(
                identityMatchingAttributesStr, SimpleMatchingAttributes.class);
    }

    /**
     * A convenience class used to write and read entries from the diags.
     */
    public static class MatchingAttributesHeader {

        private long id;
        private String identityMatchingAttributes;

        /**
         * Constructor for a MatchingAttributesHeader.
         *
         * @param id the OID corresponding to the matching attributes.
         * @param identityMatchingAttributes the matching attributes encapsulated in a single string.
         */
        public MatchingAttributesHeader(final long id,
                @Nonnull final String identityMatchingAttributes) {
            this.id = id;
            this.identityMatchingAttributes = identityMatchingAttributes;
        }

        /**
         * Get the oid corresponding to the matching attributes.
         *
         * @return long giving the id corresponding to the attributes.
         */
        public long getId() {
            return id;
        }

        /**
         * Get the string enscapsulating the matching attributes.
         *
         * @return String that gives the matching attributes in a single string.
         */
        public String getIdentityMatchingAttributes() {
            return identityMatchingAttributes;
        }
    }
}
