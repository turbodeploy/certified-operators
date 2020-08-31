package com.vmturbo.identity.store;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;

/**
 * A persistent mapping from a MatchingAttribute (derived from a discovered item) to the
 * corresponding OID.
 * Intended to be used as a backing store for the IdentityStore class.
 *
 * @param <T> the type of context object.
 */
public interface PersistentIdentityStore<T> extends DiagsRestorable<T> {

    /**
     * Fetches all the mappings from ItemAttributes -> OID. This is used by IdentityStore
     * at initialization time.
     *
     * @return a Map from ItemAttributes to the corresponding OID
     */
    @Nonnull
    Map<IdentityMatchingAttributes, Long> fetchAllOidMappings() throws IdentityStoreException;

    /**
     * Persist new mappings from Item identifiers to the corresponding OID.
     *
     * @param attrsToOidMap a map for IdentityMatchingAttributes to OID for all keys in 'itemsToPersist'
     * @throws IdentityStoreException if there is an error saving these records
     */
    void saveOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrsToOidMap)
            throws IdentityStoreException;

    /**
     * Update the existing mappings from Item identifiers to the corresponding OID.
     *
     * @param attrsToOidMap a map of the OIDs of records to remove from the PersistentIdentityStore.
     * @throws IdentityStoreException if there is an error updating these records
     */
    void updateOidMappings(@Nonnull Map<IdentityMatchingAttributes, Long> attrsToOidMap)
            throws IdentityStoreException;

    /**
     * Remove the PersistentIdentityStore records for each of the OIDs in the given list.
     *
     * @param oidsToRemove a list of the OIDs of records to remove from the PersistentIdentityStore.
     * @throws IdentityStoreException if there is an error removing these records
     */
    void removeOidMappings(Set<Long> oidsToRemove) throws IdentityStoreException;
}