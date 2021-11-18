package com.vmturbo.topology.processor.identity.storage;

import static junitparams.JUnitParamsRunner.$;
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

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

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
import com.vmturbo.topology.processor.identity.cache.IdentityCache;
import com.vmturbo.topology.processor.identity.extractor.EntityDescriptorImpl;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.IdentityRecordsOperation;

/**
 * The VMTIdentityServiceInMemoryUnderlyingStoreTest implements in-memory underlying store unit
 * tests.
 */
@RunWith(JUnitParamsRunner.class)
public class IdentityServiceInMemoryUnderlyingStoreTest {
    private IdentityDatabaseStore databaseStore = mock(IdentityDatabaseStore.class);
    ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata = new ConcurrentHashMap<>();
    long firstOID;
    long secondOID;
    long probeId = 111;
    EntityDTO vmDTO;

    Object[] generateTestData() {
        final StoreParameters optimizedIdentityCacheParameters = new StoreParameters(true, false, false);
        final StoreParameters IdentityCacheParameters = new StoreParameters(false, true, false);
        final StoreParameters EntityDescriptorsParameters = new StoreParameters(false, false, true);

        return $(
                $(optimizedIdentityCacheParameters),
                $(IdentityCacheParameters),
                $(EntityDescriptorsParameters));
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() throws Exception {
        firstOID = IdentityGenerator.next();
        secondOID = IdentityGenerator.next();
        vmDTO = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).setId("123").build();
        perProbeMetadata.put(probeId,
            new ServiceEntityIdentityMetadataStore(Collections.singletonList(EntityIdentityMetadata
                .newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build())));
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testLookup(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>(), new ArrayList<String>());
        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }
        long oid = store.lookupByIdentifyingSet(entityDescriptor.getNonVolatileProperties(mock(EntityMetadataDescriptor.class)),
                                                entityDescriptor.getVolatileProperties(mock(EntityMetadataDescriptor.class)));
        Assert.assertEquals(firstOID, oid);
    }

    /**
     * Tests, that to property descriptor lists, which are the reverse of one another are not
     * matched the with the same entity.
     *
     * @throws Exception on exception occur
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testLookupInverted(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        final List<PropertyDescriptor> nonVolatileProperties = new ArrayList<>();
        nonVolatileProperties.add(new PropertyDescriptorImpl("VM", 1));
        nonVolatileProperties.add(new PropertyDescriptorImpl("Tag", 1));
        final EntityDescriptor entityDescriptor =
                new EntityDescriptorImpl(nonVolatileProperties, Collections.emptyList(),
                        Collections.emptyList());

        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }

        long oid = store.lookupByIdentifyingSet(entityDescriptor.getNonVolatileProperties(mock(EntityMetadataDescriptor.class)),
                entityDescriptor.getVolatileProperties(mock(EntityMetadataDescriptor.class)));
        Assert.assertEquals(firstOID, oid);
        final List<PropertyDescriptor> reverseDescriptors = Lists.reverse(nonVolatileProperties);
        Assert.assertNotEquals(firstOID,
                store.lookupByIdentifyingSet(reverseDescriptors,
                        entityDescriptor.getVolatileProperties(mock(EntityMetadataDescriptor.class))));
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testAddEntry(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeCache = identityCache.getOids().size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>(), new ArrayList<String>());


        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }

        Assert.assertEquals(1, identityCache.getOids().size() - iSizeCache);
        Mockito.verify(databaseStore).saveDescriptors(any());
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testRemoveEntry(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeoid2Dto = identityCache.getOids().size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>(), new ArrayList<String>());
        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }
        Assert.assertEquals(1, identityCache.getOids().size() - iSizeoid2Dto);
        store.removeEntry(firstOID);
        Assert.assertEquals(0, identityCache.getOids().size() - iSizeoid2Dto);
        Mockito.verify(databaseStore).removeDescriptor(firstOID);
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testRemoveNonExistentEntry(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        Field identityCacheClass = store.getClass().getDeclaredField("identityCache");
        identityCacheClass.setAccessible(true);
        IdentityCache identityCache = (IdentityCache)identityCacheClass.get(store);
        int iSizeoid2Dto = identityCache.getOids().size();

        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                                                                           new ArrayList<String>(), new ArrayList<String>());
        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }
        Assert.assertEquals(1, identityCache.getOids().size() - iSizeoid2Dto);
        store.removeEntry(secondOID);
        Assert.assertEquals(1, identityCache.getOids().size() - iSizeoid2Dto);
        Mockito.verify(databaseStore, times(0)).removeDescriptor(secondOID);
    }

    /**
     * Test that removing oids works both when the OID is in the store and when it isn't. Also
     * test that it does not remove anything from the database.
     *
     * @throws IdentityServiceStoreOperationException when store throws it.
     * @throws IdentityUninitializedException when store throws it.
     * @throws IdentityDatabaseException when db throws it.
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testRemove(StoreParameters storeParameters)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException,
            IdentityDatabaseException {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        EntityDescriptor entityDescriptor = new EntityDescriptorMock(Arrays.asList("VM"),
                new ArrayList<String>(), new ArrayList<String>());
        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID,
                    new EntryData(entityDescriptor, mock(EntityMetadataDescriptor.class), probeId,
                            vmDTO));
        }

        assertTrue(store.containsOID(firstOID));
        assertEquals(1, store.bulkRemove(new HashSet<>(Collections.singletonList(firstOID))));
        assertFalse(store.containsOID(firstOID));
        Mockito.verify(databaseStore, times(0)).removeDescriptor(firstOID);
        assertEquals(0, store.bulkRemove(new HashSet<>(Collections.singletonList(secondOID))));
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testUpsertEntries(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        EntryData data1 = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data1.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data1.getProbeId()).thenReturn(probeId);
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        new ArrayList<String>(),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        new ArrayList<String>(),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getProbeId()).thenReturn(probeId);
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));

        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID, data1);
            transaction.addEntry(secondOID, data2);
        }

        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        ArgumentCaptor<Collection> savedDescCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(databaseStore).saveDescriptors(savedDescCaptor.capture());
        assertEquals(2, savedDescCaptor.getValue().size());
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testUpsertFail(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        doThrow(IdentityDatabaseException.class).when(databaseStore).saveDescriptors(any());
        EntryData data = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        new ArrayList<String>(),
                        Arrays.asList("VM_Heuristics")));
        when(data.getProbeId()).thenReturn(probeId);
        when(data.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));

        try {
            try (IdentityRecordsOperation transaction = store.createTransaction()) {
                transaction.addEntry(firstOID, data);
            }
            Assert.fail("Expected exception when saving to database fails.");
        } catch (IdentityServiceStoreOperationException e) {
            // Make sure the OID didn't get assigned.
            assertFalse(store.containsOID(firstOID));
        }
    }


    @Test
    @Parameters(method = "generateTestData")
    public void testInitialLoad(StoreParameters storeParameters) throws Exception {
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
                    TimeUnit.MILLISECONDS, perProbeMetadata, storeParameters.isUseIdentityRecordsCache(), storeParameters.isUseOptimizedIdentityRecordsCache());
        store.initialize();
        verify(databaseStore, timeout(60000).atLeast(2)).getDescriptors();
        store.waitForInitializedStore();
        assertTrue(store.containsOID(entityId));
        store.checkInitialized();
    }

    @Test
    @Parameters(method = "generateTestData")
    public void testLoadNonVolatilePropertiesCache(StoreParameters storeParameters) throws Exception {
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
        when(entityMetadata.getNonVolatilePropertyRanks()).thenReturn(Arrays.asList(1, 2));
        when(entityMetadata.getHeuristicPropertyRanks()).thenReturn(Collections.singletonList(3));

        perProbeMetadata.put(probeId, serviceEntityIdentityMetadataStore);
        Set<IdentityRecord> identityRecords = new HashSet<>();
        identityRecords.add(new IdentityRecord(EntityType.VIRTUAL_MACHINE,
            descriptor, probeId));
        when(descriptor.getKey()).thenReturn("key");
        when(descriptor.getOID()).thenReturn(entityId);
        doReturn(identityRecords).when(databaseStore).getDescriptors();
        final IdentityServiceInMemoryUnderlyingStore store =
                new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10,  50,
                        TimeUnit.MILLISECONDS, perProbeMetadata, storeParameters.isUseIdentityRecordsCache(), storeParameters.isUseOptimizedIdentityRecordsCache());
        store.initialize();
        verify(databaseStore, timeout(60000).only()).getDescriptors();
        store.checkInitialized();
        assertTrue(store.containsOID(entityId));

        List<EntityInMemoryProxyDescriptor> matchingEntities = store.getDtosByNonVolatileProperties(nonVolatileProperties);
        Assert.assertEquals(1, matchingEntities.size());
        Assert.assertEquals(descriptor, matchingEntities.get(0));

        final List<PropertyDescriptor> nonMatchingVolatileProperties = new ArrayList<>();
        nonMatchingVolatileProperties.add(new PropertyDescriptorImpl("myOid", 1));
        nonMatchingVolatileProperties.add(new PropertyDescriptorImpl("Storage", 2));
        matchingEntities = store.getDtosByNonVolatileProperties(nonMatchingVolatileProperties);
        Assert.assertEquals(0, matchingEntities.size());
    }

    /**
     * Tests restoring the data in the store.
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testBackupRestore(StoreParameters storeParameters) throws Exception {
        final IdentityServiceUnderlyingStore store = initializeDefaultIdentityStore(storeParameters);
        final EntryData data1 = mock(EntryData.class);
        EntityDTO entityDTO =
                EntityDTO.newBuilder().setId("999").setEntityType(EntityType.VIRTUAL_MACHINE).build();
        when(data1.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"), Collections.emptyList(),
                        Arrays.asList("VM_Heuristics")));
        when(data1.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data1.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data1.getProbeId()).thenReturn(probeId);

        final EntryData data2 = mock(EntryData.class);
        when(data2.getDescriptor()).thenReturn(
                new EntityDescriptorMock(Arrays.asList("VM"),
                        Collections.emptyList(),
                        Arrays.asList("VM_Heuristics")));
        when(data2.getMetadata()).thenReturn(mock(EntityMetadataDescriptor.class));
        when(data2.getEntityDTO()).thenReturn(Optional.of(entityDTO));
        when(data2.getProbeId()).thenReturn(probeId);

        try (IdentityRecordsOperation transaction = store.createTransaction()) {
            transaction.addEntry(firstOID, data1);
            transaction.addEntry(secondOID, data2);
        }
        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));

        final StringWriter writer = new StringWriter();
        store.backup(writer);

        final IdentityServiceInMemoryUnderlyingStore newStore = new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10, perProbeMetadata,
                storeParameters.isUseIdentityRecordsCache(), storeParameters.isUseOptimizedIdentityRecordsCache());
        if (storeParameters.isUseIdentityRecordsCache() || storeParameters.isUseOptimizedIdentityRecordsCache()) {
            newStore.restore(new StringReader(writer.toString()), null);
        } else {
            newStore.restoreOldDiags(new StringReader(writer.toString()));
        }
        assertTrue(store.containsOID(firstOID));
        assertTrue(store.containsOID(secondOID));
    }

    private IdentityServiceUnderlyingStore initializeDefaultIdentityStore(StoreParameters storeParameters) {
        return new IdentityServiceInMemoryUnderlyingStore(databaseStore, 10,  perProbeMetadata, storeParameters.isUseIdentityRecordsCache(),
                storeParameters.isUseOptimizedIdentityRecordsCache());
    }

    private static class StoreParameters {
        final boolean useOptimizedIdentityRecordsCache;
        final boolean useIdentityRecordsCache;
        final boolean useEntityDescriptorsCache;

        public StoreParameters(boolean useOptimizedIdentityRecordsCache,
                boolean useIdentityRecordsCache, boolean useEntityDescriptorsCache) {
            this.useOptimizedIdentityRecordsCache = useOptimizedIdentityRecordsCache;
            this.useIdentityRecordsCache = useIdentityRecordsCache;
            this.useEntityDescriptorsCache = useEntityDescriptorsCache;
        }

        public boolean isUseOptimizedIdentityRecordsCache() {
            return useOptimizedIdentityRecordsCache;
        }

        public boolean isUseIdentityRecordsCache() {
            return useIdentityRecordsCache;
        }
    }
}
