package com.vmturbo.identity.store;


import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.identity.attributes.IdentityMatchingAttributes;

/**
 * A persistent mapping from a MatchingAttribute (derived from a discovered item) to the
 * corresponding OID.
 * Intended to be used as a backing store for the IdentityStore class.
 */
public interface PersistentIdentityStore<ITEM_TYPE> {

    /**
     * Fetches all the mappings from ItemAttributes -> OID. This is used by IdentityStore
     * at initialization time.
     *
     * @return a Map from ItemAttributes to the corresponding OID
     */
    @Nonnull
    Map<IdentityMatchingAttributes, Long> fetchAllOidMappings();

    /**
     * Persist new mappings from Item to the corresponding OID. The caller provides two maps
     * used to perform the persistence operation.
     *
     * The first map, 'itemOidMap', holds the new OID to be persisted for each item.
     *
     * The second map, 'itemToAttributesMap' contains the IdentityMatchingAttributes for each item -
     * the attributes which make this item unique with respect to OID generation,
     * in case the PersistentIdentityStore wants to persist the IdentityMatchingAttributes as well.
     *
     * @param itemToOidMap a map for Item to OID for all keys in 'itemsToPersist'
     * @param itemToAttributesMap the map from Item to the identifying attributes used to differentiate
     *                            each item from all the others.
     */
    void saveOidMappings(@Nonnull Map<ITEM_TYPE, Long> itemToOidMap,
                         @Nonnull Map<ITEM_TYPE, IdentityMatchingAttributes> itemToAttributesMap)
            throws IdentityStoreException;

    /**
     * Remove the PersistentIdentityStore records for each of the OIDs in the given list.
     *
     * @param oidsToRemove a list of the OIDs of records to remove from the PersistentIdentityStore.
     * @throws IdentityStoreException if there is an error removing these records
     */
    void removeOidMappings(Set<Long> oidsToRemove) throws IdentityStoreException;
}