package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.persistence.Column;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.store.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;

/**
 * Persistence for OIDs for Workflows.
 **/
public class PersistentWorkflowIdentityStore implements PersistentIdentityStore<WorkflowInfo> {

    private final DSLContext dsl;

    public PersistentWorkflowIdentityStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings() {
        return dsl.select()
                .from(WORKFLOW_OID)
                .fetchInto(WorkflowHeader.class).stream()
                .collect(Collectors.toMap(WorkflowHeader::getMatchingAttributes,
                        WorkflowHeader::getId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveOidMappings(@Nonnull Map<WorkflowInfo, Long> itemToOidMap,
                                @Nonnull Map<WorkflowInfo, IdentityMatchingAttributes> itemToAttributesMap)
            throws IdentityStoreException {
        // run the update as a transaction; if there is an exception, the transaction will be rolled back
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                for (Map.Entry<WorkflowInfo, Long> mapEntry : itemToOidMap.entrySet()) {
                    final WorkflowInfo workflow = mapEntry.getKey();
                    final long oid = mapEntry.getValue();
                    final IdentityMatchingAttributes attr = itemToAttributesMap.get(workflow);
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
                        throw new DataAccessException(
                                String.format("error persisting workflow OID: %s externalName %s targetId %s",
                                        oid, externalName, targetId), e);
                    }
                }
            });
        } catch (DataAccessException e) {
            throw new IdentityStoreException(e.getMessage(), e);
        }
    }

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

        @SuppressWarnings("unused")
        public String getExternalName() {
            return externalName;
        }

        public long getId() {
            return id;
        }

        public long getTargetId() {
            return targetId;
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
