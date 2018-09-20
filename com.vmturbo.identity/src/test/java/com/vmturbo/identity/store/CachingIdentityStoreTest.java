package com.vmturbo.identity.store;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;

@RunWith(MockitoJUnitRunner.class)
public class CachingIdentityStoreTest<ITEM_TYPE> {

    private final SimpleMatchingAttributes attr1 = SimpleMatchingAttributes.newBuilder()
            .addAttribute("id", "v1")
            .build();
    private final SimpleMatchingAttributes attr2 = SimpleMatchingAttributes.newBuilder()
            .addAttribute("id", "v2")
            .build();

    @Mock
    private ITEM_TYPE mockItem1;

    @Mock
    private ITEM_TYPE mockItem2;

    @Mock
    PersistentIdentityStore persistentStore;

    @Mock
    AttributeExtractor<ITEM_TYPE> attributeExtractor;

    @Mock
    IdentityInitializer identityInitializer;


    @BeforeClass
    public static void classSetup() {
        IdentityGenerator.initPrefix(1L);
    }

    long beginTestOid;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(attributeExtractor.extractAttributes(mockItem1)).thenReturn(attr1);
        when(attributeExtractor.extractAttributes(mockItem2)).thenReturn(attr2);
        beginTestOid = IdentityGenerator.next();
    }

    /**
     * Test fetching OIDs for two {@link IdentityMatchingAttributes} with no OIDs
     * preloaded from the persistent store. Expect that both IdentitMatchingAttributes, with
     * OIDs assigned, will be written.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testFetchOrAssignOids() throws Exception {
        // arrange
        List<ITEM_TYPE> itemsList = Lists.newArrayList(
                mockItem1,
                mockItem2
        );
        CachingIdentityStore<ITEM_TYPE> testCachingIdentityStore = new CachingIdentityStore(attributeExtractor,
                persistentStore, identityInitializer);
        // act
        IdentityStoreUpdate<ITEM_TYPE> itemOidMapResult = testCachingIdentityStore
                .fetchOrAssignItemOids(itemsList);
        // assert
        verify(persistentStore).fetchAllOidMappings();
        ArgumentCaptor<Map> attrsToOidMapCaptor = ArgumentCaptor.forClass(Map.class);
        // verify that there was one call to 'saveOidMappings()'
        verify(persistentStore).saveOidMappings(attrsToOidMapCaptor.capture());
        verifyNoMoreInteractions(persistentStore);
        // verify that the attrs ->
        final Map<IdentityMatchingAttributes, Long> attrsToOidMap = attrsToOidMapCaptor.getValue();
        assertThat(attrsToOidMap.keySet(), containsInAnyOrder(attr1, attr2));
        // check the result contains both items and that the OIDs were generated
        final Map<ITEM_TYPE, Long> newItemsMap = itemOidMapResult.getNewItems();
        assertThat(newItemsMap.size(), equalTo(2));
        assertThat(newItemsMap.keySet(), containsInAnyOrder(mockItem1, mockItem2));
        assertThat(newItemsMap.get(mockItem1), greaterThan(beginTestOid));
        assertThat(newItemsMap.get(mockItem2), greaterThan(newItemsMap.get(mockItem1)));
        assertTrue(itemOidMapResult.getOldItems().isEmpty());
    }

    /**
     * Simulate two OIDs pre-loaded from the persistent store; no new OID mapping will be saved
     * as a result of fetching the OIDs for the input.
     */
    @Test
    public void testPreloadedOids() throws Exception {
        // arrange
        PersistentIdentityStore persistentStore = Mockito.mock(PersistentIdentityStore.class);
        // in this case the OIDs were pre-generated, so we set up a baseline of '0'
        Map<IdentityMatchingAttributes, Long> preloadOidMappings =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(attr1, IdentityGenerator.next())
                        .put(attr2, IdentityGenerator.next())
                        .build();
        when(persistentStore.fetchAllOidMappings()).thenReturn(preloadOidMappings);
        List<ITEM_TYPE> testOidList = Lists.newArrayList(
                mockItem1,
                mockItem2
        );
        CachingIdentityStore<ITEM_TYPE> testCachingIdentityStore = new CachingIdentityStore(attributeExtractor,
                persistentStore, identityInitializer);
        // act
        IdentityStoreUpdate<ITEM_TYPE> itemOidMapResult = testCachingIdentityStore.fetchOrAssignItemOids(testOidList);
        // assert
        verify(persistentStore).fetchAllOidMappings();
        verifyNoMoreInteractions(persistentStore);
        // check the result contains both items and that the OIDs were generated
        final Map<ITEM_TYPE, Long> newItemsMap = itemOidMapResult.getNewItems();
        assertTrue(newItemsMap.isEmpty());
        final Map<ITEM_TYPE, Long> oldItemsMap = itemOidMapResult.getOldItems();
        assertThat(oldItemsMap.size(), equalTo(2));
        assertThat(oldItemsMap.keySet(), containsInAnyOrder(mockItem1, mockItem2));
        assertThat(oldItemsMap.get(mockItem1), greaterThan(beginTestOid));
        assertThat(oldItemsMap.get(mockItem2), greaterThan(oldItemsMap.get(mockItem1)));
    }

    /**
     * Simulate one OIDs pre-loaded from the persistent store; one new OID mapping will be saved
     * as a result of fetching the OIDs for the input.
     */
    @Test
    public void testOnePreloadedOid() throws Exception {
        // arrange
        PersistentIdentityStore persistentStore = Mockito.mock(PersistentIdentityStore.class);

        // simulate only attr1 preloaded
        Map<IdentityMatchingAttributes, Long> preloadOidMappings =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(attr1, IdentityGenerator.next())
                        .build();
        when(persistentStore.fetchAllOidMappings()).thenReturn(preloadOidMappings);
        List<ITEM_TYPE> itemsList = Lists.newArrayList(mockItem1, mockItem2);
        CachingIdentityStore<ITEM_TYPE> testCachingIdentityStore = new CachingIdentityStore(attributeExtractor,
                persistentStore, identityInitializer);
        // act
        IdentityStoreUpdate<ITEM_TYPE> itemOidMapResult = testCachingIdentityStore
                .fetchOrAssignItemOids(itemsList);
        // assert
        verify(persistentStore).fetchAllOidMappings();
        ArgumentCaptor<Map> attrsToOidMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(persistentStore).saveOidMappings(attrsToOidMapCaptor.capture());
        verifyNoMoreInteractions(persistentStore);
        // expect attr2 to have been saved in the persistent store
        assertThat(attrsToOidMapCaptor.getAllValues().size(), equalTo(1));
        Set<IdentityMatchingAttributes> matchingAttributesFound = attrsToOidMapCaptor.getValue().keySet();
        assertThat(matchingAttributesFound, contains(attr2));
        // check the that one item is in the old map, and one in the new map
        final Map<ITEM_TYPE, Long> newItemsMap = itemOidMapResult.getNewItems();
        assertThat(newItemsMap.size(), equalTo(1));
        final Map<ITEM_TYPE, Long> oldItemsMap = itemOidMapResult.getOldItems();
        assertThat(oldItemsMap.size(), equalTo(1));
    }

    /**
     * Test update two  {@link IdentityMatchingAttributes} with given generated cOIDs
     * preloaded from the persistent store. Expect that both IdentitMatchingAttributes, with
     * OIDs assigned, will be updated.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testUpdateItemAttributes() throws Exception {
        // arrange
        final long id1 = IdentityGenerator.next();
        final long id2 = IdentityGenerator.next();
        Map<Long, ITEM_TYPE> updateItems = ImmutableMap.of(id1, mockItem1, id2, mockItem2);
        CachingIdentityStore<ITEM_TYPE> testCachingIdentityStore = new CachingIdentityStore(attributeExtractor,
                persistentStore, identityInitializer);
        //act
        testCachingIdentityStore.updateItemAttributes(updateItems);
        // assert
        verify(persistentStore).fetchAllOidMappings();

        ArgumentCaptor<Map> attrsToOidMapCaptor = ArgumentCaptor.forClass(Map.class);
        // verify that there was one call to 'saveOidMappings()'
        verify(persistentStore).updateOidMappings(attrsToOidMapCaptor.capture());
        verifyNoMoreInteractions(persistentStore);
        // verify that the items ->
        final Map<IdentityMatchingAttributes, Long> itemsMap = attrsToOidMapCaptor.getValue();
        assertThat(itemsMap.keySet(), containsInAnyOrder(attr1, attr2));
        assertThat(itemsMap.values(), containsInAnyOrder(id1, id2));
    }
}