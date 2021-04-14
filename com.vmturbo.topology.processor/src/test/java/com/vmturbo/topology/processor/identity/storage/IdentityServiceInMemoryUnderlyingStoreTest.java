package com.vmturbo.topology.processor.identity.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityDescriptorMock;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.EntityDescriptorImpl;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.IdentityCache;

/**
 * The VMTIdentityServiceInMemoryUnderlyingStoreTest implements in-memory underlying store unit
 * tests.
 */
public class IdentityServiceInMemoryUnderlyingStoreTest {
    private IdentityServiceUnderlyingStore store;
    private IdentityDatabaseStore databaseStore = mock(IdentityDatabaseStore.class);
    ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata = new ConcurrentHashMap<>();
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
        perProbeMetadata.put(probeId,
            new ServiceEntityIdentityMetadataStore(Collections.singletonList(EntityIdentityMetadata
                .newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build())));
        store = new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10,  perProbeMetadata);
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
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeCache = identityCache.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        Assert.assertEquals(1, identityCache.size() - iSizeCache);
        Mockito.verify(databaseStore).saveDescriptors(any());
    }

    @Test
    public void testUpdateEntry() throws Exception {
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        final int cacheSize = identityCache.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                        mock(EntityMetadataDescriptor.class),
                        EntityType.VIRTUAL_MACHINE,
                        probeId);
        EntityDescriptor entityDescriptorUpdate =
                new EntityDescriptorMock(Arrays.asList("VM_Update"),
                                            new ArrayList<String>());
        Mockito.verify(databaseStore).saveDescriptors(any());
        store.updateEntry(firstOID, entityDescriptorUpdate,
                          mock(EntityMetadataDescriptor.class),
                          EntityType.VIRTUAL_MACHINE,
                          probeId);
        // Size stays the same
        Assert.assertEquals(1, identityCache.size() - cacheSize);
        Assert.assertEquals(1, identityCache.size() - cacheSize);
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
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        // Size stays the same
        Assert.assertEquals(1, identityCache.size());
        // The value should be new
        long oid = store.lookupByIdentifyingSet(mock(EntityMetadataDescriptor.class),
                                                EntityDescriptorMock.composePropertySet(
                                                        Arrays.asList("VM_Update_Fail")));
        Assert.assertEquals(firstOID, oid);
    }

    @Test
    public void testRemoveEntry() throws Exception {
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeoid2Dto = identityCache.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);
        Assert.assertEquals(1, identityCache.size() - iSizeoid2Dto);
        store.removeEntry(firstOID);
        Assert.assertEquals(0, identityCache.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore).removeDescriptor(firstOID);
    }

    @Test
    public void testRemoveNonExistentEntry() throws Exception {
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeoid2Dto = identityCache.size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);
        Assert.assertEquals(1, identityCache.size() - iSizeoid2Dto);
        store.removeEntry(secondOID);
        Assert.assertEquals(1, identityCache.size() - iSizeoid2Dto);
        Mockito.verify(databaseStore, times(0)).removeDescriptor(secondOID);
    }

    /**
     * Test that shallowRemove works both when the OID is in the store and when it isn't. Also
     * test that it does not remove anything from the database.
     *
     * @throws IdentityServiceStoreOperationException when store throws it.
     * @throws IdentityUninitializedException when store throws it.
     * @throws IdentityDatabaseException when db throws it.
     */
    @Test
    public void testShallowRemove()
            throws IdentityServiceStoreOperationException, IdentityUninitializedException,
            IdentityDatabaseException {
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                new ArrayList<String>());
        store.addEntry(firstOID, entityDescriptor,
                mock(EntityMetadataDescriptor.class),
                EntityType.VIRTUAL_MACHINE,
                probeId);

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.shallowRemove(firstOID));
        assertFalse(store.containsOID(firstOID));
        Mockito.verify(databaseStore, times(0)).removeDescriptor(firstOID);
        assertFalse(((IdentityServiceInMemoryUnderlyingStore)store).shallowRemove(secondOID));
    }

    @Test
    public void testUpsertEntries() throws Exception {
        EntryData data1 = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data1.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data1.getProbeId()).thenReturn(probeId);
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getProbeId()).thenReturn(probeId);
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));

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
        final long entityId = 7L;
        final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata =
            new ConcurrentHashMap<>();
        final ServiceEntityIdentityMetadataStore serviceEntityIdentityMetadataStore =
            mock(ServiceEntityIdentityMetadataStore.class);
        final ServiceEntityIdentityMetadata entityMetadata =
            mock(ServiceEntityIdentityMetadata.class);

        when(serviceEntityIdentityMetadataStore.getMetadata(EntityType.VIRTUAL_MACHINE)).thenReturn(entityMetadata);
        perProbeMetadata.put(probeId, serviceEntityIdentityMetadataStore);
        Set<IdentityRecord> identityRecords = new HashSet<>();
        identityRecords.add(new IdentityRecord(EntityType.VIRTUAL_MACHINE,
            descriptor, probeId));
        when(descriptor.getKey()).thenReturn("key");
        when(descriptor.getOID()).thenReturn(entityId);
        doThrow(IdentityDatabaseException.class).doReturn(identityRecords).when(databaseStore).getDescriptors();

        final IdentityServiceInMemoryUnderlyingStore store =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10, 50,
                    TimeUnit.MILLISECONDS, perProbeMetadata, false);
        store.initialize();
        verify(databaseStore, timeout(60000).atLeast(2)).getDescriptors();
        store.waitForInitializedStore();
        assertTrue(store.containsOID(entityId));
        store.checkInitialized();
    }

    @Test
    public void testLoadNonVolatilePropertiesCache() throws Exception {
        final EntityInMemoryProxyDescriptor descriptor = mock(EntityInMemoryProxyDescriptor.class);
        final long probeId = 1L;
        final long entityId = 7L;
        final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata =
            new ConcurrentHashMap<>();
        final ServiceEntityIdentityMetadataStore serviceEntityIdentityMetadataStore =
            mock(ServiceEntityIdentityMetadataStore.class);
        final ServiceEntityIdentityMetadata entityMetadata =
            mock(ServiceEntityIdentityMetadata.class);
        final List<ServiceEntityProperty> nonVolatilePropertyMetaData = new ArrayList<>();
        nonVolatilePropertyMetaData.add(new ServiceEntityProperty("id", 1));
        nonVolatilePropertyMetaData.add(new ServiceEntityProperty("entity_type", 2));
        final List<PropertyDescriptor> nonVolatileProperties = new ArrayList<>();
        nonVolatileProperties.add(new PropertyDescriptorImpl("myOid", 1));
        nonVolatileProperties.add(new PropertyDescriptorImpl("VM", 2));

        final List<PropertyDescriptor> identifyingProperties =
            new ArrayList<>(nonVolatileProperties);
        identifyingProperties.add(new PropertyDescriptorImpl("localName", 3));

        when(descriptor.getIdentifyingProperties()).thenReturn(identifyingProperties);
        when(entityMetadata.getNonVolatileProperties()).thenReturn(nonVolatilePropertyMetaData);
        when(serviceEntityIdentityMetadataStore.getMetadata(EntityType.VIRTUAL_MACHINE)).thenReturn(entityMetadata);
        when(descriptor.containsAll(nonVolatileProperties)).thenReturn(true);
        perProbeMetadata.put(probeId, serviceEntityIdentityMetadataStore);
        Set<IdentityRecord> identityRecords = new HashSet<>();
        identityRecords.add(new IdentityRecord(EntityType.VIRTUAL_MACHINE,
            descriptor, probeId));
        when(descriptor.getKey()).thenReturn("key");
        when(descriptor.getOID()).thenReturn(entityId);
        doReturn(identityRecords).when(databaseStore).getDescriptors();

        final IdentityServiceInMemoryUnderlyingStore store =
            new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10,  50,
                TimeUnit.MILLISECONDS, perProbeMetadata, true);
        store.initialize();
        verify(databaseStore, timeout(60000).only()).getDescriptors();
        store.checkInitialized();
        assertTrue(store.containsOID(entityId));

        List<EntityInMemoryProxyDescriptor> matchingEntities =
            store.getDtosByNonVolatileProperties(nonVolatileProperties);
        Assert.assertEquals(1, matchingEntities.size());
        Assert.assertEquals(descriptor, matchingEntities.get(0));

        final List<PropertyDescriptor> nonMatchingVolatileProperties = new ArrayList<>();
        nonMatchingVolatileProperties.add(new PropertyDescriptorImpl("myOid", 1));
        nonMatchingVolatileProperties.add(new PropertyDescriptorImpl("Storage", 2));
        matchingEntities =
            store.getDtosByNonVolatileProperties(nonMatchingVolatileProperties);
        Assert.assertEquals(0, matchingEntities.size());

    }

    /**
     * Tests restoring the data in the store.
     */
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
        when(data1.getProbeId()).thenReturn(probeId);

        final EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data2.getProbeId()).thenReturn(probeId);

        store.upsertEntries(ImmutableMap.of(firstOID, data1, secondOID, data2));

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        final StringWriter writer = new StringWriter();
        store.backup(writer);

        final IdentityServiceInMemoryUnderlyingStore newStore =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10, perProbeMetadata);

        newStore.restore(new StringReader(writer.toString()), null);
        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));
    }
}
