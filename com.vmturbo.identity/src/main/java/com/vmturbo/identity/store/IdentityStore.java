package com.vmturbo.identity.store;


import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.vmturbo.identity.attributes.IdentityMatchingAttributes;

/**
 * A store maintaining a mapping from discovered items of type ITEM_TYPE  to the corresponding OID.
 * Intended to be used as a backing store for the any application that needs to create and manage
 * OIDs for tracking a generic {@link ITEM_TYPE}.
 *
 * Implementors of this interface may provide a persistent store if needed.
 *
 */
public interface IdentityStore<ITEM_TYPE> {

    /**
     * Fetch the corresponding OIDs for the given items (of type T).
     * If the item has been assigned an OID before, return it. If the item does not have an OID
     * assigned already, a new OID will be generated and assigned.
     *
     * @param itemList the list of {@link IdentityMatchingAttributes} items to look up
     * @return a list of the OIDs for each item in the input list; in the same order
     */
    @Nonnull
    IdentityStoreUpdate<ITEM_TYPE> fetchOrAssignItemOids(@Nonnull List<ITEM_TYPE> itemList)
            throws IdentityStoreException;

    /**
     * Remove the workflow oid infos corresponding to the given OIDs.
     *
     * @param oidsToRemove the oids for the workflow infos to be removed
     * @throws IdentityStoreException if there's an error removing the workflow infos
     */
    void removeItemOids(@Nonnull List<Long> oidsToRemove) throws IdentityStoreException;


    /**
     * Look up the OIDs for items that match the given predicate based on the
     * {@link IdentityMatchingAttributes}.
     *
     * @param itemFilter a predicate matched against each IdentityMatchingAttributes in the store
     * @return a Set of OIDs for which the corresponding IdentityMatchingAttributes evaluates
     * to 'true' using the given 'itemFilter'
     */
    @Nonnull
    Set<Long> filterItemOids(@Nonnull Predicate<IdentityMatchingAttributes> itemFilter);
}
