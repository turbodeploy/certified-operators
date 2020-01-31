package com.vmturbo.topology.processor.targets;

import static com.vmturbo.topology.processor.db.tables.TargetspecOid.TARGETSPEC_OID;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.persistence.Column;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;

/**
 * Persistence for oids for target spec, the corresponding target has the same oid with the target spec.
 **/
public class PersistentTargetSpecIdentityStore implements PersistentIdentityStore {

    /**
     * File name to dump diagnostics.
     */
    public static final String TARGET_IDENTIFIERS_DIAGS_FILE_NAME = "Target.identifiers";
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final DSLContext dsl;

    /**
     * Constructor with DSL context instance injected.
     *
     * @param dsl DSL context instance
     */
    public PersistentTargetSpecIdentityStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings() throws IdentityStoreException  {
        return dsl.select()
                .from(TARGETSPEC_OID)
                .fetchInto(TargetSpecHeader.class)
                .stream()
                .collect(Collectors.toMap(targetHeader -> deserializeTargetSpecIdentityMatchingAttrs(
                            targetHeader.getIdentityMatchingAttributes()),
                            TargetSpecHeader::getId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrToOidMap)
            throws IdentityStoreException {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            for (Map.Entry<IdentityMatchingAttributes, Long> mapEntry : attrToOidMap.entrySet()) {
                final IdentityMatchingAttributes attr = mapEntry.getKey();
                final long oid = mapEntry.getValue();
                final String serializedAttr = serializeTargetSpecIdentityMatchingAttrs(attr);
                try {
                    transactionDsl.insertInto(TARGETSPEC_OID)
                            .set(TARGETSPEC_OID.ID, oid)
                            .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, serializedAttr)
                            .onDuplicateKeyIgnore()
                            .execute();
                } catch (DataAccessException e) {
                    // capture the info about the workflow causing the error
                    throw new IdentityStoreException(
                            String.format("Error persisting target spec OID: %s, "
                                    + "IdentityMatchingAttributes: %s", oid, serializedAttr), e);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
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
                    transactionDsl.update(TARGETSPEC_OID)
                            .set(TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES, serializedAttr)
                            .where(TARGETSPEC_OID.ID.eq(oid))
                            .execute();
                } catch (DataAccessException e) {
                    // capture the info about the workflow causing the error
                    throw new IdentityStoreException(
                            String.format("Error persisting target spec OID: %s, "
                                    + "IdentityMatchingAttributes: %s", oid, serializedAttr), e);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeOidMappings(Set<Long> oidsToRemove) throws IdentityStoreException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.deleteFrom(TARGETSPEC_OID)
                        .where(TARGETSPEC_OID.ID.in(oidsToRemove))
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
                final TargetSpecHeader targetSpecHeader = new TargetSpecHeader(entry.getValue(),
                        serializeTargetSpecIdentityMatchingAttrs(entry.getKey()));
                appender.appendString(GSON.toJson(targetSpecHeader, TargetSpecHeader.class));
            }
        } catch (IdentityStoreException e) {
            throw new DiagnosticsException(
                    "Retrieving target identifiers from database failed. " + e);
        }
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        dsl.transaction(configuration -> {
            final Map<IdentityMatchingAttributes, Long> attrToOidMap = Maps.newHashMap();
            try {
                // Clear table first.
                removeAllOids();
                collectedDiags.forEach(diag -> {
                    final TargetSpecHeader targetSpecHeader = GSON.fromJson(diag, TargetSpecHeader.class);
                    final IdentityMatchingAttributes attrs = deserializeTargetSpecIdentityMatchingAttrs(
                            targetSpecHeader.getIdentityMatchingAttributes());
                    attrToOidMap.put(attrs, targetSpecHeader.getId());
                });
                saveOidMappings(attrToOidMap);
            } catch (IdentityStoreException e) {
                throw new DiagnosticsException(String.format("Saving target identifiers to database "
                        + "failed. %s", e));
            }
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return TARGET_IDENTIFIERS_DIAGS_FILE_NAME;
    }

    /**
     * Method to clear the target spec oid table.
     *
     * @throws IdentityStoreException if deleting all oid mappings failed.
     */
    private void removeAllOids() throws IdentityStoreException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.delete(TARGETSPEC_OID).execute();
            });
        } catch (DataAccessException e) {
            throw new IdentityStoreException("Error deleting all Oid Mappings", e);
        }
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
     * A DB Bean to represent the unique columns for a target spec item header.
     * This includes the unique id and the identity matching attributes.
     */
    public static class TargetSpecHeader {

        @Column(name = "ID")
        public long id;
        @Column(name = "IDENTITY_MATCHING_ATTRIBUTES")
        public String identityMatchingAttributes;

        public TargetSpecHeader(final long id, @Nonnull final String identityMatchingAttributes) {
            this.id = id;
            this.identityMatchingAttributes = identityMatchingAttributes;
        }

        public long getId() {
            return id;
        }

        public String getIdentityMatchingAttributes() {
            return identityMatchingAttributes;
        }
    }
}
