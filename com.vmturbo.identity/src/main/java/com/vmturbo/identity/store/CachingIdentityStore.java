package com.vmturbo.identity.store;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;

/**
 * This class implements a write-through cache for {@link IdentityMatchingAttributes} to
 * OID mappings. The implementation of a {@link PersistentIdentityStore} is injected
 * to perform the persistence. We also use an in-memory map to avoid going to the persistent
 * store to answer every query.
 *
 * @param <ITEM_TYPE> the type of item for which OIDs will be generated and cached by this class.
 **/
public class CachingIdentityStore<ITEM_TYPE> implements IdentityStore<ITEM_TYPE> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * the persistent store for the ItemAttributes -> OID mapping
     */
    private final PersistentIdentityStore persistentStore;

    /**
     * an in-memory map from ItemAttributes to the corresponding OID
     */
    private final BiMap<IdentityMatchingAttributes, Long> oidMap = HashBiMap.create();

    /**
     * A utility to create an IdentityMatchingAttributes instance capturing the key
     * (identifying) attributes from a given item {@link ITEM_TYPE}
     */
    private final AttributeExtractor<ITEM_TYPE> attributeExtractor;

    /**
     * Has the in-memory-map been initialized yet?
     */
    private boolean initialized = false;

    /**
     * The CachingEntityStore depends on an instance of {#link PersistentIdentityStore}.
     *
     * @param persistentStore the persistent store for all the {@link IdentityMatchingAttributes}->
     *                        OIDmappings that we have ever seen
     * @param identityInitializer initialized instance of IdentityGenerator
     */
    public CachingIdentityStore(@Nonnull final AttributeExtractor<ITEM_TYPE> attributeExtractor,
                                @Nonnull final PersistentIdentityStore persistentStore,
                                @Nonnull final IdentityInitializer identityInitializer) {
        this.attributeExtractor = Objects.requireNonNull(attributeExtractor);
        this.persistentStore = Objects.requireNonNull(persistentStore);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    /**
     * To fetch the OID for each input {@link IdentityMatchingAttributes} we look in the in-memory
     * cache of previously seen IdentityMatchingAttributes. If found, we use the previously
     * generated/persisted OID. If not found, generate a new OID and persist it by calling the
     * 'persistentStore'.
     * Note that the in-memory 'oidMap' is lazy-initialized from the persistentStore on first
     * usage.
     *
     * @param itemList the list of {@link ITEM_TYPE}  items to look up
     * @return the list of OIDs corresponding to each of the input {@link IdentityMatchingAttributes};
     * note that some of the OIDs returned may have been newly generated and persisted for this request.
     */
    @Override
    @Nonnull
    public IdentityStoreUpdate<ITEM_TYPE> fetchOrAssignItemOids(@Nonnull final List<ITEM_TYPE> itemList)
            throws IdentityStoreException {
        synchronized (oidMap) {
            // Ensure the in-memory cache is initialized
            initializeFromPersistentStore();

            // two way map from ITEM_TYPE to OID containing new OIDs generated in this call
            final Map<ITEM_TYPE, IdentityMatchingAttributes> itemsToPersist = Maps.newHashMap();

            // a map of item to previously allocated IDs for items we've seen before
            final Map<ITEM_TYPE, Long> oldOids = Maps.newHashMap();

            // a map of item to newly allocated IDs if we haven't seen this item before
            final Map<ITEM_TYPE, Long> newlyAllocatedOids = Maps.newHashMap();

            // look up OIDs for all items in the input; either cached, allocated during this run,
            // or not seen yet and needing a new OID to be allocated
            // calculate the matching attributes for this item to use as a key to look up OIDs
            // attributes -> oid previously cached? if so, return the OID for the attributes
            itemList.stream().filter(Objects::nonNull).forEach(itemToLookUp -> {
                IdentityMatchingAttributes attributes =
                        attributeExtractor.extractAttributes(itemToLookUp);
                // has this item been mapped before?
                Long previousOid = oidMap.get(attributes);
                if (previousOid != null) {
                    // yes; add this to the list of oldOids
                    oldOids.put(itemToLookUp, previousOid);
                } else {
                    // not previously persisted; did we see it in this list of TIEM_TYPEs?
                    if (!newlyAllocatedOids.containsKey(itemToLookUp)) {
                        // no; remember to persist this item and attributes, and
                        // allocate a new OID to use for this item
                        itemsToPersist.put(itemToLookUp, attributes);
                        Long newOid =  IdentityGenerator.next();
                        newlyAllocatedOids.put(itemToLookUp, newOid);
                    }
                }
            });
            // were there any new mappings created?
            if (!itemsToPersist.isEmpty()) {
                // yes, new mappings created - extract only the newly allocated item->oid entries
                final Map<IdentityMatchingAttributes, Long> attrsToOidMap = newlyAllocatedOids.keySet().stream()
                        .filter(itemsToPersist::containsKey)
                        .collect(Collectors.toMap(itemsToPersist::get, newlyAllocatedOids::get));
                // save them to the persistent store
                persistentStore.saveOidMappings(attrsToOidMap);
                // once persisted, add to the in-memory cache
                attrsToOidMap.forEach((IdentityMatchingAttributes attrs, Long oid) ->
                    oidMap.put(attrs, oid));
            }
            // return the structure indicating what OIDs were seen before, and what OIDs were added
            return new IdentityStoreUpdate<>(oldOids, newlyAllocatedOids);
        }
    }

    /**
     * Remove records for the OIDs given from the in-memory OID store and the persistent store.
     *
     * @param oidsToRemove a list of OIDs to be removed
     * @throws IdentityStoreException if there's an error removing from the rows from the
     * {@link PersistentIdentityStore}
     */
    @Override
    public void removeItemOids(@Nonnull final Set<Long> oidsToRemove) throws IdentityStoreException {
        synchronized (oidMap) {
            initializeFromPersistentStore();
            persistentStore.removeOidMappings(oidsToRemove);
            // only remove from cache if the they are removed from db successfully
            oidsToRemove.forEach(oid -> oidMap.inverse().remove(oid));
        }
    }

    /**
     * Update the attributes associated with a particular OID. We need to support this to allow users to
     * make changes to user-controlled entities without changing the entity OID.
     *
     * @param itemMap the oid to identity matching attributes map
     * @throws IdentityStoreException if there's an error updating from the rows from the
     * {@link PersistentIdentityStore}
     * @throws IdentifierConflictException
     */
    @Override
    public void updateItemAttributes(@Nonnull final Map<Long, ITEM_TYPE> itemMap)
            throws IdentityStoreException, IdentifierConflictException {
        synchronized (oidMap) {
            initializeFromPersistentStore();
            final Map<IdentityMatchingAttributes, Long> attrsToOidMap = Maps.newHashMap();
            for (final Map.Entry<Long, ITEM_TYPE> entry : itemMap.entrySet()) {
                if (entry.getValue() == null) {
                    logger.error("Null item value for item OID {}.", entry.getKey());
                    continue;
                }
                IdentityMatchingAttributes attributes = attributeExtractor
                        .extractAttributes(entry.getValue());
                Long previousOid = oidMap.get(attributes);
                // If updated item identifiers are same with other existing items, terminate and throw
                // exception.
                if (previousOid != null && !previousOid.equals(entry.getKey())) {
                    throw new IdentifierConflictException(String.format("Updated item %d with identifier "
                            + "%s already exists.", previousOid, attributes));
                }
                attrsToOidMap.put(attributes, entry.getKey());
            }
            persistentStore.updateOidMappings(attrsToOidMap);
            // once persisted, update to the in-memory cache
            attrsToOidMap.forEach((IdentityMatchingAttributes attrs, Long oid) ->
                oidMap.inverse().put(oid, attrs));
        }
    }

    @Nonnull
    @Override
    public Set<Long> filterItemOids(@Nonnull final Predicate<IdentityMatchingAttributes> itemFilter)
            throws IdentityStoreException {
        synchronized (oidMap) {
            initializeFromPersistentStore();
            return oidMap.entrySet().stream()
                    .filter(oidMapEntry -> itemFilter.test(oidMapEntry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(ImmutableSet.toImmutableSet());
        }
    }

    /**
     * Ensure that the in-memory cache is initialized. Only fetch from the persistent store
     * once. Assumed to be called synchronized on 'oidMap'.
     */
    private synchronized void initializeFromPersistentStore() throws IdentityStoreException {
        if (!initialized) {
            // initialize the in-memory cache from the persistent store
            oidMap.putAll(persistentStore.fetchAllOidMappings());
            logger.info("CachingIdentityStore loaded, items: {}", oidMap.size());
            initialized = true;
        }
    }

}
