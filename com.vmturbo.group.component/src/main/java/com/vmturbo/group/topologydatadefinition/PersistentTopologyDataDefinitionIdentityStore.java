package com.vmturbo.group.topologydatadefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;

/**
 * Identity Store for TopologyDataDefinition OIDs.
 * TODO add connection to DB to persist mapping of IdentityMatchingAttributes to OIDs.
 */
public class PersistentTopologyDataDefinitionIdentityStore implements PersistentIdentityStore {

    /**
     * Filename for diags.
     */
    public static final String TOPOLOGY_DATA_DEF_IDENTIFIERS_DIAGS_FILE_NAME =
        "Topology.Data.Definitions.identifiers";

    /**
     * Map that takes matching attributes and maps them to a unique OID.
     */
    private final Map<IdentityMatchingAttributes, Long> identityMap = new HashMap<>();

    @Nonnull
    @Override
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings()
            throws IdentityStoreException {
        return identityMap;
    }

    @Override
    public void saveOidMappings(@Nonnull final Map<IdentityMatchingAttributes, Long> map) throws IdentityStoreException {
        identityMap.putAll(map);
    }

    @Override
    public void updateOidMappings(@Nonnull final Map<IdentityMatchingAttributes, Long> map) throws IdentityStoreException {
        identityMap.putAll(map);
    }

    @Override
    public void removeOidMappings(final Set<Long> set) throws IdentityStoreException {
        Set<IdentityMatchingAttributes> keysToDelete =  identityMap.entrySet().stream()
            .filter(entry -> set.contains(entry.getValue()))
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
        keysToDelete.forEach(identityMap::remove);
    }

    /**
     * TODO implement restoration from Diags.
     *
     * @param collectedDiags previously collected diagnostics
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

    }

    /**
     * TODO implement saving to diags.
     *
     * @param appender an appender to put diagnostics to. String by string.
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {

    }

    @Nonnull
    @Override
    public String getFileName() {
        return TOPOLOGY_DATA_DEF_IDENTIFIERS_DIAGS_FILE_NAME;
    }
}
