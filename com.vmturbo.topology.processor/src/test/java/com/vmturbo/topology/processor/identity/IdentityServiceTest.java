/**
 * IdentityServiceTest.java Copyright (c) VMTurbo 2015
 */
package com.vmturbo.topology.processor.identity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;

/**
 * The IdentityServiceTest implements Identity Service tests.
 */
public class IdentityServiceTest {

    private IdentityService idSvc;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() throws Exception {
        idSvc = new IdentityService(
                new IdentityServiceInMemoryUnderlyingStore(
                        Mockito.mock(IdentityDatabaseStore.class)),
                    new HeuristicsMatcher());
    }

    @Test
    public void testEmpty() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneThereIdentical() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                   new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneThereIdenticalWithRemoval() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        idSvc.removeEntity(oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                   new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertNotEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneThereNonIdentical() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(
                                               Collections.singletonList("VM_Different"),
                                               new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertNotEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneCheckPresentByOID() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        Assert.assertTrue(idSvc.containsOID(oid));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckNotPresentByOID() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        Assert.assertFalse(idSvc.containsOID(oid + 1));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckPresentByIdentifyingProperties() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM", 1));
        Assert.assertTrue(idSvc.containsWithIdentifyingProperties(
                Mockito.mock(EntityMetadataDescriptor.class), properties));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckNotPresentByIdentifyingProperties() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM_Not", 1));
        Assert.assertFalse(idSvc.containsWithIdentifyingProperties(
                Mockito.mock(EntityMetadataDescriptor.class), properties));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckNotPresentByIdentifyingPropertiesDiffRank() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class));
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM", 2));
        Assert.assertFalse(idSvc.containsWithIdentifyingProperties(
                Mockito.mock(EntityMetadataDescriptor.class), properties));
        idSvc.removeEntity(oid);
    }
}
