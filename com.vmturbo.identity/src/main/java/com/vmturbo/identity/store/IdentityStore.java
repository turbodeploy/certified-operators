package com.vmturbo.identity.store;


import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.identity.attributes.IdentityMatchingAttributes;

/**
 * A store maintaining a mapping from a {@link IdentityMatchingAttributes} (derived from a
 * discovered item) to the corresponding OID. Intended to be used as a backing store for the
 * {@link IdentityLookup} class.
 *
 * Implementors of this interface may provide a persistent store if needed.
 */
public interface IdentityStore {

    /**
     * Fetch the corresponding OIDs for the given {@link IdentityMatchingAttributes}.
     * If the item has been assigned an OID before, return it. If the item does not have an OID
     * assigned already, a new OID will be generated and assigned.
     *
     * @param attrList the list of {@link IdentityMatchingAttributes} items to look up
     * @return a list of the OIDs for each item in the input list; in the same order
     */
    @Nonnull
    List<Long> fetchOrAssignOids(@Nonnull List<IdentityMatchingAttributes> attrList);
}
