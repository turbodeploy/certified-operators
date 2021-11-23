package com.vmturbo.identity.store;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;

/**
 * Returned by IdentityStore after fetchOrAssignItemOids(). This includes two maps from
 * ITEM_TYPE to Long, one representing the OID mapping for the ITEM_TYPE for which an OID was previously
 * assigned and the other representing the OID mapping for the ITEM_TYPE for which an OID was not
 * previously assigned and for which a new OID was generated.
 *
 * Note that a given ITEM_TYPE object will be the key in only one of the two update maps.
 **/
@Immutable
public class IdentityStoreUpdate<ITEM_TYPE> {

    /**
     * Returned map of ITEM -> old OID for all the 'old' items, i.e. the ITEMs for which there was
     * already an OID assigned.
     */
    private final ImmutableMap<ITEM_TYPE, Long> oldItems;

    /**
     * Returned map of ITEM -> new OID for all the 'new' items, i.e. the ITEMs for which there was
     * no previous OID assigned, and for which a new one was allocated.
     */
    private final ImmutableMap<ITEM_TYPE, Long> newItems;

    public IdentityStoreUpdate(@Nonnull Map<ITEM_TYPE, Long> oldItems,
                               @Nonnull Map<ITEM_TYPE, Long> newItems) {
        Objects.requireNonNull(newItems);
        this.oldItems = ImmutableMap.copyOf(Objects.requireNonNull(oldItems));
        this.newItems = ImmutableMap.copyOf(Objects.requireNonNull(newItems));
    }

    /**
     * Return a map of ITEM -> old OID for all the 'old' items, i.e. the ITEMs for which there was
     * already an OID assigned.
     *
     * @return the map from ITEM -> old OID for items previously assigned an OID
     */
    public Map<ITEM_TYPE, Long> getOldItems() {
        return oldItems;
    }

    public Map<ITEM_TYPE, Long> getNewItems() {
        return newItems;
    }
}
