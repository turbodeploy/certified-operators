package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
 * Persistence for OIDs for Workflows.
 **/
public class PersistentWorkflowIdentityStore implements PersistentIdentityStore<Void> {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final DSLContext dsl;

    public PersistentWorkflowIdentityStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings()
            throws IdentityStoreException {
        try {
            return dsl.select()
                    .from(WORKFLOW_OID)
                    .fetchInto(WorkflowHeader.class)
                    .stream()
                    .collect(Collectors.toMap(WorkflowHeader::getMatchingAttributes,
                            WorkflowHeader::getId));
        } catch (DataAccessException e) {
            throw new IdentityStoreException("Error fetching all OID mappings", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrsToOidMap) {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            for (Map.Entry<IdentityMatchingAttributes, Long> mapEntry : attrsToOidMap.entrySet()) {
                final long oid = mapEntry.getValue();
                final IdentityMatchingAttributes attr = mapEntry.getKey();
                final String externalName = attr.getMatchingAttribute(WORKFLOW_NAME)
                        .getAttributeValue();
                final String targetIdString = attr.getMatchingAttribute(WORKFLOW_TARGET_ID)
                        .getAttributeValue();
                final Long targetId = Long.valueOf(targetIdString);
                try {
                    transactionDsl.insertInto(WORKFLOW_OID)
                            .set(WORKFLOW_OID.ID, oid)
                            .set(WORKFLOW_OID.EXTERNAL_NAME, externalName)
                            .set(WORKFLOW_OID.TARGET_ID, targetId)
                            .onDuplicateKeyIgnore()
                            .execute();
                } catch (DataAccessException e) {
                    // capture the info about the workflow causing the error
                    throw new IdentityStoreException(
                            String.format("Error persisting workflow OID: %s externalName %s targetId %s",
                                    oid, externalName, targetId), e);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOidMappings(Map<IdentityMatchingAttributes, Long> attrsToOidMap) throws IdentityStoreException {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            for (Map.Entry<IdentityMatchingAttributes, Long> mapEntry : attrsToOidMap.entrySet()) {
                final long oid = mapEntry.getValue();
                final IdentityMatchingAttributes attr = mapEntry.getKey();
                final String externalName = attr.getMatchingAttribute(WORKFLOW_NAME)
                        .getAttributeValue();
                final String targetIdString = attr.getMatchingAttribute(WORKFLOW_TARGET_ID)
                        .getAttributeValue();
                final Long targetId = Long.valueOf(targetIdString);
                try {
                    transactionDsl.update(WORKFLOW_OID)
                            .set(WORKFLOW_OID.EXTERNAL_NAME, externalName)
                            .set(WORKFLOW_OID.TARGET_ID, targetId)
                            .where(WORKFLOW_OID.ID.eq(oid))
                            .execute();
                } catch (DataAccessException e) {
                    // capture the info about the workflow causing the error
                    throw new IdentityStoreException(
                            String.format("Error persisting workflow OID: %s externalName %s targetId %s",
                                    oid, externalName, targetId), e);
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
                transactionDsl.deleteFrom(WORKFLOW_OID)
                        .where(WORKFLOW_OID.ID.in(oidsToRemove))
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
                final WorkflowHeader workflowHeader;
                try {
                    workflowHeader = new WorkflowHeader(entry.getKey(), entry.getValue());
                } catch (NumberFormatException | IdentityStoreException e) {
                    throw new DiagnosticsException("Failed to create workflow header for " + entry,
                            e);
                }
                final String string = GSON.toJson(workflowHeader, WorkflowHeader.class);
                appender.appendString(string);
            }
        } catch (IdentityStoreException e) {
            throw new DiagnosticsException(
                    String.format("Retrieving workflow identifiers from database " + "failed. %s",
                            e));
        }
    }

    @Override
    public void restoreDiags(List<String> collectedDiags, Void context) throws DiagnosticsException {
        dsl.transaction(configuration -> {
            final Map<IdentityMatchingAttributes, Long> attrsToOidMap = Maps.newHashMap();
            try {
                // Clear table first.
                removeAllOids();
                collectedDiags.forEach(diag -> {
                    final WorkflowHeader workflowHeader = GSON.fromJson(diag, WorkflowHeader.class);
                    final IdentityMatchingAttributes attrs = workflowHeader.getMatchingAttributes();
                    attrsToOidMap.put(attrs, workflowHeader.getId());
                });
                saveOidMappings(attrsToOidMap);
            } catch (IdentityStoreException e) {
                throw new DiagnosticsException(String.format("Saving workflow identifiers to database "
                        + "failed. %s", e));
            }
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "workflow-identities";
    }

    /**
     * Method to clear the workflow oid table.
     *
     * @throws IdentityStoreException if deleting all oid mappings failed.
     */
    private void removeAllOids() throws IdentityStoreException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.delete(WORKFLOW_OID).execute();
            });
        } catch (DataAccessException e) {
            throw new IdentityStoreException("Error deleting all OID Mappings", e);
        }
    }

    /**
     * A DB Bean to represent the unique columns for a workflow item header.
     * This includes the unique id, the target_id, and the external_name of the workflow.
     */
    private static class WorkflowHeader {
        @Column(name = "ID")
        public long id;
        @Column(name = "TARGET_ID")
        public long targetId;
        @Column(name = "EXTERNAL_NAME")
        public String externalName;

        /**
         * Constructor with identity attributes and workflow OID as parameters. Then extracting target id
         * and workflow name from the matching attributes.
         *
         * @param attrs Identity matching attributes of the workflow
         * @param id OID of the workflow
         * @throws NumberFormatException If parsing target if failed
         * @throws IdentityStoreException If getting matching attributes failed
         */
        public WorkflowHeader(@Nonnull final IdentityMatchingAttributes attrs, final long id)
                throws NumberFormatException, IdentityStoreException {
            this.id = id;
            this.targetId = Long.valueOf(Objects.requireNonNull(attrs.getMatchingAttribute(WORKFLOW_TARGET_ID)
                    .getAttributeValue()));
            this.externalName = Objects.requireNonNull(attrs.getMatchingAttribute(WORKFLOW_NAME)
                    .getAttributeValue());
        }

        /**
         * Since we have another constructor which passing different parameters, we need to define the
         * default constructor with all db column parameters for auto loading the column values in db table
         * into the bean.
         *
         * @param id
         * @param targetId
         * @param externalName
         */
        @SuppressWarnings("unused")
        public WorkflowHeader(final long id, final long targetId, @Nonnull final String externalName) {
            this.id = id;
            this.targetId = targetId;
            this.externalName = externalName;
        }

        public long getId() {
            return id;
        }

        @SuppressWarnings("unused")
        public long getTargetId() {
            return targetId;
        }

        @SuppressWarnings("unused")
        public String getExternalName() {
            return externalName;
        }

        /**
         * Construct a {@link SimpleMatchingAttributes} object representing this Workflow DB table row.
         *
         * @return a new {@link SimpleMatchingAttributes} initialized with the exernalName and
         * targetId from this row
         */
        public SimpleMatchingAttributes getMatchingAttributes(){
            return new SimpleMatchingAttributes.Builder()
                    .addAttribute(WORKFLOW_NAME,
                            externalName)
                    .addAttribute(WORKFLOW_TARGET_ID,
                            Long.toString(targetId))
                    .build();
        }
    }
}
