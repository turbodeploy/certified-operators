package com.vmturbo.group.topologydatadefinition;

import static com.vmturbo.group.db.tables.TopologyDataDefinitionOid.TOPOLOGY_DATA_DEFINITION_OID;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.group.db.tables.records.TopologyDataDefinitionOidRecord;
import com.vmturbo.identity.store.PersistentMatchingAttributesIdentityStore;

/**
 * Identity Store for TopologyDataDefinition OIDs.
 */
public class PersistentTopologyDataDefinitionIdentityStore extends
        PersistentMatchingAttributesIdentityStore<TopologyDataDefinitionOidRecord> {

    /**
     * Filename for diags.
     */
    private static final String TOPOLOGY_DATA_DEF_IDENTIFIERS_DIAGS_FILE_NAME =
        "Topology.Data.Definitions.identifiers";

    private static final String TABLE_DESCRIPTION = "topology data definition";

    /**
     * Constructor with DSL context instance injected.
     *
     * @param dsl DSL context instance
     */
    public PersistentTopologyDataDefinitionIdentityStore(@Nonnull final DSLContext dsl) {
        super(dsl, TOPOLOGY_DATA_DEFINITION_OID, TOPOLOGY_DATA_DEFINITION_OID.ID,
                TOPOLOGY_DATA_DEFINITION_OID.IDENTITY_MATCHING_ATTRIBUTES,
                TOPOLOGY_DATA_DEF_IDENTIFIERS_DIAGS_FILE_NAME, TABLE_DESCRIPTION);
    }
}
