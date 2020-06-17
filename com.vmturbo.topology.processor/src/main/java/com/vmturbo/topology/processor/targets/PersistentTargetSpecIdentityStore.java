package com.vmturbo.topology.processor.targets;

import static com.vmturbo.topology.processor.db.tables.TargetspecOid.TARGETSPEC_OID;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.identity.store.PersistentMatchingAttributesIdentityStore;
import com.vmturbo.topology.processor.db.tables.records.TargetspecOidRecord;

/**
 * Persistence for oids for target spec, the corresponding target has the same oid with the target spec.
 **/
public class PersistentTargetSpecIdentityStore extends PersistentMatchingAttributesIdentityStore<TargetspecOidRecord> {

    /**
     * File name to dump diagnostics.
     */
    public static final String TARGET_IDENTIFIERS_DIAGS_FILE_NAME = "Target.identifiers";
    private static final String TARGET_SPEC_DESCRIPTION = "target spec";

    /**
     * Constructor with DSL context instance injected.
     *
     * @param dsl DSL context instance
     */
    public PersistentTargetSpecIdentityStore(@Nonnull final DSLContext dsl) {
        super(dsl, TARGETSPEC_OID, TARGETSPEC_OID.ID, TARGETSPEC_OID.IDENTITY_MATCHING_ATTRIBUTES,
                TARGET_IDENTIFIERS_DIAGS_FILE_NAME, TARGET_SPEC_DESCRIPTION);
    }
}
