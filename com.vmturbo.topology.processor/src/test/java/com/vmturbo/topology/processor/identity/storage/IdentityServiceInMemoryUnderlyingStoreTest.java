package com.vmturbo.topology.processor.identity.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityDescriptorMock;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.EntityDescriptorImpl;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
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
    long probeId = 111;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() throws Exception {
        firstOID = IdentityGenerator.next();
        secondOID = IdentityGenerator.next();
        store = new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10);
    }

    @Test
    public void testLookup() throws Exception {
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock
                                                        .composePropertySet(Arrays.asList("VM")));
        Assert.assertEquals(firstOID, oid);
    }

    /**
     * Tests, that to property descriptor lists, which are the reverse of one another are not
     * matched the with the same entity.
     *
     * @throws Exception on exception occur
     */
    @Test
    public void testLookupInverted() throws Exception {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(new PropertyDescriptorImpl("VM", 1));
        propertyDescriptors.add(new PropertyDescriptorImpl("PM", 2));
        final EntityDescriptor entityDescriptor =
                new EntityDescriptorImpl(propertyDescriptors, Collections.emptyList(),
                        Collections.emptyList());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                propertyDescriptors);
        Assert.assertEquals(firstOID, oid);
        final List<PropertyDescriptor> reverseDescriptors = Lists.reverse(propertyDescriptors);
        Assert.assertNotEquals(firstOID,
                store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                        reverseDescriptors));
    }

    @Test
    public void testAddEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        ConcurrentHashMap<?, ?> sizeoid2Dto = (ConcurrentHashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore).saveDescriptors(anyLong(), any());
    }

    @Test
    public void testUpdateEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        ConcurrentHashMap<?, ?> sizeoid2Dto = (ConcurrentHashMap<?, ?>)oid2Dto.get(store);
        final int iSizeIndex = sizeIndex.size();
        final int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        EntityDescriptor entityDescriptorUpdate =
                new EntityDescriptorMock(Arrays.asList("VM_Update"),
                                            new ArrayList<String>());
        Mockito.verify(databaseStore).saveDescriptors(anyLong(), any());
        store.updateEntry(firstOID, entityDescriptorUpdate,
                          mock(EntityMetadataDescriptor.class),
                          EntityType.VIRTUAL_MACHINE,
                          probeId);
        // Size stays the same
        Assert.assertEquals(1, sizeIndex.size() - iSizeIndex);
        Assert.assertEquals(1, sizeoid2Dto.size() - iSizeoid2Dto);
        // The value should be new
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock.composePropertySet(
                                                        Arrays.asList("VM_Update")));
        Assert.assertEquals(firstOID, oid);
        Mockito.verify(databaseStore, times(2))
                .saveDescriptors(anyLong(), any());
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateEntryError() throws Exception {
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                       mock(EntityMetadataDescriptor.class),
                       EntityType.VIRTUAL_MACHINE,
                       probeId);
        EntityDescriptor entityDescriptorUpdate =
                new EntityDescriptorMock(Arrays.asList("VM_Update_Fail"),
                                            new ArrayList<String>());
        store.updateEntry(secondOID, entityDescriptorUpdate,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        ConcurrentHashMap<?, ?> sizeoid2Dto = (ConcurrentHashMap<?, ?>)oid2Dto.get(store);
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
    public void testRemoveEntry() throws Exception {
        Field index = store.getClass().getDeclaredField("index_");
        Field oid2Dto = store.getClass().getDeclaredField("oid2Dto_");
        index.setAccessible(true);
        oid2Dto.setAccessible(true);
        HashMap<?, ?> sizeIndex = (HashMap<?, ?>)index.get(store);
        ConcurrentHashMap<?, ?> sizeoid2Dto = (ConcurrentHashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);
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
        ConcurrentHashMap<?, ?> sizeoid2Dto = (ConcurrentHashMap<?, ?>)oid2Dto.get(store);
        int iSizeIndex = sizeIndex.size();
        int iSizeoid2Dto = sizeoid2Dto.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);
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
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data1.getEntityDTO()).thenReturn(Optional.of(entityDTO));

        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));

        store.upsertEntries(ImmutableMap.of(firstOID, data1, secondOID, data2));

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        ArgumentCaptor<Collection> savedDescCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(databaseStore).saveDescriptors(anyLong(), savedDescCaptor.capture());
        assertEquals(2, savedDescCaptor.getValue().size());
    }

    @Test
    public void testUpsertFail() throws Exception {
        doThrow(IdentityDatabaseException.class).when(databaseStore).saveDescriptors(anyLong(), any());
        EntryData data = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data.getProbeId()).thenReturn(probeId);
        when(data.getEntityDTO()).thenReturn(Optional.of(entityDTO));
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
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 50,
                    TimeUnit.MILLISECONDS, 10);

        verify(databaseStore, timeout(60000).atLeast(2)).getDescriptors();
        assertTrue(store.containsOID(7L));
        store.checkInitialized();
    }

    @Test
    public void testBackupRestore() throws Exception {
        final EntryData data1 = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data1.getEntityDTO()).thenReturn(Optional.of(entityDTO));

        final EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));

        store.upsertEntries(ImmutableMap.of(firstOID, data1, secondOID, data2));

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        final StringWriter writer = new StringWriter();
        store.backup(writer);

        final IdentityServiceInMemoryUnderlyingStore newStore =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10);

        newStore.restore(new StringReader(writer.toString()));
        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));
    }
}
