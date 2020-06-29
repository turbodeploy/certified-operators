package com.vmturbo.topology.processor.migration;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.platform.sdk.common.util.AccountDefEntryConstants;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Migrate all existing entries in the Targetspec_oid db table to use all lowercase for address
 * fields.  This way, users cannot create duplicate targets just by changing the case in a FQDN.
 */
public class V_01_01_02__Target_Oid_Fields_To_Lowercase extends HandleTargetOidChange {

    /**
     * Constructor for migration to change all addresses to lowercase in targetspec_oid table.
     *
     * @param probeStore remote probe store holding probe information
     * @param targetStore target store holding all targets
     * @param idStore identity store for getting and writing target Oids
     * @param targetDao the target store for deleting targets made redundant by the Oid generation
     *        changes
     */
    public V_01_01_02__Target_Oid_Fields_To_Lowercase(@Nonnull ProbeStore probeStore,
            @Nonnull TargetStore targetStore,
            @Nonnull IdentityStore<TargetSpec> idStore,
            @Nonnull TargetDao targetDao) {
        super(probeStore, targetStore, idStore, targetDao);
    }

    @Override
    protected Set<String> getImpactedAccountDefKeys() {
        return ImmutableSet.of(AccountDefEntryConstants.ADDRESS_FIELD,
                AccountDefEntryConstants.NAME_OR_ADDRESS_FIELD);
    }
}
