package com.vmturbo.topology.processor.identity.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityDescriptorMock;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.services.EntityProxyDescriptor;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

/**
 * The VMTIdentityServiceInMemoryUnderlyingStoreTest implements in-memory underlying store unit
 * tests.
 */
public class IdentityServiceInMemoryUnderlyingStoreTest {
    private IdentityServiceUnderlyingStore store;
    private IdentityDatabaseStore databaseStore = mock(IdentityDatabaseStore.class);
    long firstOID;
    long secondOID;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() throws Exception {
        firstOID = IdentityGenerator.next();
        secondOID = IdentityGenerator.next();
        store = new IdentityServiceInMemoryUnderlyingStore(databaseStore);
    }

    @Test
    public void testLookup() throws Exception {
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock
                                                        .composePropertySet(Arrays.asList("VM")));
        Assert.assertEquals(firstOID, oid);
    }

    @Test
    public void testAddEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        HashMap<?, ?> sizeoid2Dto = (HashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore).saveDescriptors(any());
    }

    @Test
    public void testUpdateEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        HashMap<?, ?> sizeoid2Dto = (HashMap<?, ?>)oid2Dto.get(store);
        final int iSizeIndex = sizeIndex.size();
        final int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        EntityDescriptor entityDescriptorUpdate =
                new EntityDescriptorMock(Arrays.asList("VM_Update"),
                                            new ArrayList<String>());
        Mockito.verify(databaseStore).saveDescriptors(any());
        store.updateEntry(firstOID, entityDescriptorUpdate,
                          mock(EntityMetadataDescriptor.class));
        // Size stays the same
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        // The value should be new
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock.composePropertySet(
                                                        Arrays.asList("VM_Update")));
        Assert.assertEquals(firstOID, oid);
        Mockito.verify(databaseStore, times(2))
                .saveDescriptors(any());
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateEntryError() throws Exception {
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        EntityDescriptor entityDescriptorUpdate =
                new EntityDescriptorMock(Arrays.asList("VM_Update_Fail"),
                                            new ArrayList<String>());
        store.updateEntry(secondOID, entityDescriptorUpdate,
                          mock(EntityMetadataDescriptor.class));
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        HashMap<?, ?> sizeoid2Dto = (HashMap<?, ?>)oid2Dto.get(store);
        // Size stays the same
        Assert.assertEquals(1, sizeIndex.size());
        Assert.assertEquals(1, sizeoid2Dto.size());
        // The value should be new
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock.composePropertySet(
                                                        Arrays.asList("VM_Update_Fail")));
        Assert.assertEquals(firstOID, oid);
    }

    @Test
    public void testQuery() throws Exception {
        store.addEntry(firstOID,
                       new EntityDescriptorMock(Arrays.asList("VM"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        store.addEntry(secondOID,
                       new EntityDescriptorMock(Arrays.asList("VM_1"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM_Heuristics", 1));
        Iterable<EntityProxyDescriptor> result =
                store.query(properties);
        int count = 0;
        for (Iterator<EntityProxyDescriptor> it = result.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testQuerySingleMatch() throws Exception {
        store.addEntry(firstOID,
                       new EntityDescriptorMock(Arrays.asList("VM"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        store.addEntry(secondOID,
                       new EntityDescriptorMock(Arrays.asList("VM_1"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM_1", 1));
        properties.add(new PropertyDescriptorImpl("VM_Heuristics", 1));
        Iterable<EntityProxyDescriptor> result =
                store.query(properties);
        int count = 0;
        for (Iterator<EntityProxyDescriptor> it = result.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testQueryNotFound() throws Exception {
        store.addEntry(firstOID,
                       new EntityDescriptorMock(Arrays.asList("VM"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        store.addEntry(secondOID,
                       new EntityDescriptorMock(Arrays.asList("VM_1"),
                                                   Arrays.asList("VM_Heuristics")),
                       mock(EntityMetadataDescriptor.class));
        List<PropertyDescriptor> properties = new ArrayList<>();
        // Wrong rank. Nothing will get found.
        properties.add(new PropertyDescriptorImpl("VM_Heuristics", 2));
        Iterable<EntityProxyDescriptor> result =
                store.query(properties);
        int count = 0;
        for (Iterator<EntityProxyDescriptor> it = result.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void testRemoveEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        HashMap<?, ?> sizeoid2Dto = (HashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        store.removeEntry(firstOID);
        Assert.assertEquals(0, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(0, sizeoid2Dto.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore).removeDescriptor(firstOID);
    }

    @Test
    public void testRemoveNonExistentEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        HashMap<?, ?> sizeoid2Dto = (HashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class));
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        store.removeEntry(secondOID);
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore, times(0)).removeDescriptor(secondOID);
    }

    @Test
    public void testUpsertEntries() throws Exception {
        EntryData data1 = mock(EntryData.class);
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        store.upsertEntries(ImmutableMap.of(firstOID, data1, secondOID, data2));

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        ArgumentCaptor<Collection> savedDescCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(databaseStore).saveDescriptors(savedDescCaptor.capture());
        assertEquals(2, savedDescCaptor.getValue().size());
    }

    @Test
    public void testUpsertFail() throws Exception {
        doThrow(IdentityDatabaseException.class).when(databaseStore).saveDescriptors(any());
        EntryData data = mock(EntryData.class);
        when(data.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        try {
            store.upsertEntries(ImmutableMap.of(firstOID, data));
            Assert.fail("Expected exception when saving to database fails.");
        } catch (IdentityServiceStoreOperationException e) {
            // Make sure the OID didn't get assigned.
            assertFalse(store.containsOID(firstOID));
        }
    }


    @Test
    public void testInitialLoad() throws Exception {
        final EntityInMemoryProxyDescriptor descriptor = mock(EntityInMemoryProxyDescriptor.class);
        when(descriptor.getKey()).thenReturn("key");
        when(descriptor.getOID()).thenReturn(7L);

        when(databaseStore.getDescriptors())
            .thenThrow(IdentityDatabaseException.class)
            .thenReturn(Collections.singleton(descriptor));

        final IdentityServiceInMemoryUnderlyingStore store =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 50, TimeUnit.MILLISECONDS);

        verify(databaseStore, timeout(60000).atLeast(2)).getDescriptors();
        assertTrue(store.containsOID(7L));
        store.checkInitialized();
    }

    @Test
    public void testBackupRestore() throws Exception {
        final EntryData data1 = mock(EntryData.class);
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        final EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        store.upsertEntries(ImmutableMap.of(firstOID, data1, secondOID, data2));

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        final StringWriter writer = new StringWriter();
        store.backup(writer);

        final IdentityServiceInMemoryUnderlyingStore newStore =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore);

        newStore.restore(new StringReader(writer.toString()));
        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));
    }
}
