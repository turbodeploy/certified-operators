package com.vmturbo.topology.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;

/**
 * Dead-simple identity store for item matching attributes persistent tests. We skip the persistent store
 * implementaion and save the data only in memory here for easy testing.
 */
public class TestIdentityStore<ITEM_TYPE> implements IdentityStore<ITEM_TYPE>{

    private final AttributeExtractor<ITEM_TYPE> extractor;

    private final BiMap<IdentityMatchingAttributes, Long> oidMap;

    private long testId = 233L;

    public TestIdentityStore(@Nonnull final AttributeExtractor<ITEM_TYPE> attributeExtractor) {
        this.extractor = attributeExtractor;
        this.oidMap = HashBiMap.create();
    }

    @Override
    public IdentityStoreUpdate<ITEM_TYPE> fetchOrAssignItemOids(List<ITEM_TYPE> itemList)
            throws IdentityStoreException {
        final Map<ITEM_TYPE, Long> oldItems = new HashMap<>();
        final Map<ITEM_TYPE, Long> newItems = new HashMap<>();
        itemList.forEach(item -> {
            final IdentityMatchingAttributes attrs = extractor.extractAttributes(item);
            if (oidMap.get(attrs) != null) {
                oldItems.put(item, oidMap.get(attrs));
            } else {
                final long newId = nextId();
                oidMap.put(attrs, newId);
                newItems.put(item, newId);
            }
        });
        return new IdentityStoreUpdate<>(oldItems, newItems);
    }

    @Override
    public void removeItemOids(Set<Long> oidsToRemove) throws IdentityStoreException {
        oidsToRemove.forEach(id -> oidMap.inverse().remove(id));
    }

    @Override
    public void updateItemAttributes(Map<Long, ITEM_TYPE> itemMap)
            throws IdentityStoreException {
        itemMap.entrySet().forEach(entry -> {
            oidMap.inverse().put(entry.getKey(), extractor.extractAttributes(entry.getValue()));
        });
    }

    @Override
    public Set<Long> filterItemOids(Predicate<IdentityMatchingAttributes> itemFilter)
            throws IdentityStoreException {
        return oidMap.entrySet().stream()
                .filter(oidMapEntry -> itemFilter.test(oidMapEntry.getKey()))
                .map(Map.Entry::getValue)
                .collect(ImmutableSet.toImmutableSet());
    }

    private long nextId() {
        return testId ++;
    }
}
