package com.vmturbo.topology.processor.migration;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.platform.sdk.common.util.AccountDefEntryConstants;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * We previously made a change that converted all address fields to IP addresses when generating
 * attribute lists for determining target OIDs.  This created problems for some probe types as IP
 * address is not consistent over time and this allowed the same target to be added twice.  We
 * reverted this change and now must update the identity store and the DB that backs it to reflect
 * the correct value for address fields.  We do this by generating a map of target OID to TargetSpec
 * and updating the identity store, causing the matching attributes for each OID to be updated
 * according to the new scheme.  For efficiency, we skip targets that come from probe types that
 * don't have "address" as a target identifying field.
 */
public class V_01_00_03__Target_Oid_Replace_Ip_With_Address extends HandleTargetOidChange {

    /**
     * Create the migration object.
     *
     * @param probeStore used to determine which probe types have "address" as target identifying
     *                   field.
     * @param targetStore to provide targets and target specs for updating identity store
     * @param idStore identity store to update by replacing ip address by the value the user put in
     *                the "address" field.
     * @param targetDao the target store for deleting targets made redundant by the Oid generation
     *        changes
     */
    public V_01_00_03__Target_Oid_Replace_Ip_With_Address(@Nonnull ProbeStore probeStore,
            @Nonnull TargetStore targetStore, @Nonnull IdentityStore<TargetSpec> idStore,
            @Nonnull TargetDao targetDao) {
        super(probeStore, targetStore, idStore, targetDao);
    }

    @Override
    protected Set<String> getImpactedAccountDefKeys() {
        return Collections.singleton(AccountDefEntryConstants.ADDRESS_FIELD);
    }
}
