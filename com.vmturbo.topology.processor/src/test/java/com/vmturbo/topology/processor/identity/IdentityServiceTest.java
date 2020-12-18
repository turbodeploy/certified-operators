/**
 * IdentityServiceTest.java Copyright (c) VMTurbo 2015
 */
package com.vmturbo.topology.processor.identity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.extractor.EntityDescriptorImpl;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;

/**
 * The IdentityServiceTest implements Identity Service tests.
 */
public class IdentityServiceTest {

    private IdentityService idSvc;

    private long probeId = 111;

    private CommonDTO.EntityDTO entityDTO =
            EntityDTO.newBuilder()
                    .setId("999")
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setUp() throws Exception {
        idSvc = new IdentityService(
                new IdentityServiceInMemoryUnderlyingStore(
                        Mockito.mock(IdentityDatabaseStore.class), 10),
                    new HeuristicsMatcher());

    }

    @Test
    public void testEmpty() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class), entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneThereIdentical() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class), entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                   new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class), entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testDifferentOrder() throws Exception {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(new PropertyDescriptorImpl("VM", 1));
        descriptors.add(new PropertyDescriptorImpl("PM", 1));
        long oid = idSvc.getEntityOID(new EntityDescriptorImpl(descriptors, Collections.emptyList(),
                Collections.emptyList()), Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        final List<PropertyDescriptor> reverseDescriptors = Lists.reverse(descriptors);
        long oid1 = idSvc.getEntityOID(
                new EntityDescriptorImpl(reverseDescriptors, Collections.emptyList(),
                        Collections.emptyList()), Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertNotEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneThereIdenticalWithRemoval() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class), entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        idSvc.removeEntity(oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                   new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class), entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertNotEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneThereNonIdentical() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        long oid1 = idSvc.getEntityOID(new EntityDescriptorMock(
                                               Collections.singletonList("VM_Different"),
                                               new ArrayList<String>()),
                                       Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid1);
        Assert.assertNotEquals(oid, oid1);
        idSvc.removeEntity(oid);
        idSvc.removeEntity(oid1);
    }

    @Test
    public void testOneCheckPresentByOID() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        Assert.assertTrue(idSvc.containsOID(oid));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckNotPresentByOID() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        Assert.assertFalse(idSvc.containsOID(oid + 1));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testOneCheckPresentByIdentifyingProperties() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(Collections.singletonList("VM"),
                                                                  new ArrayList<String>()),
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
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
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
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
                                      Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptorImpl("VM", 2));
        Assert.assertFalse(idSvc.containsWithIdentifyingProperties(
                Mockito.mock(EntityMetadataDescriptor.class), properties));
        idSvc.removeEntity(oid);
    }

    @Test
    public void testMatchByHeuristicProperties() throws Exception {
        long oid = idSvc.getEntityOID(new EntityDescriptorMock(
                        Collections.singletonList("VM"),
                        Collections.singletonList("Volatile1"),
                        Arrays.asList("Heuristic1", "Heuristic2")),
                Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertNotEquals(IdentityService.INVALID_OID, oid);

        // verify that a new entity descriptor w/same non-volatile id props + same heuristic props
        // BUT different volatile id props should still result in the same oid.
        long hopefullySameOid = idSvc.getEntityOID(new EntityDescriptorMock(
                        Collections.singletonList("VM"),
                        Collections.singletonList("Volatile2"),
                        Arrays.asList("Heuristic1", "Heuristic2")),
                Mockito.mock(EntityMetadataDescriptor.class),
                entityDTO, probeId);
        Assert.assertEquals("Match on heuristic properties should find existing oid",
                oid, hopefullySameOid);

        // verify that there are no invalid oid's in the store. (test for reopen on OM-31658)
        Assert.assertFalse("IdentityStore should not contain any invalid oids.",
                idSvc.containsOID(IdentityService.INVALID_OID));

        // clean up
        idSvc.removeEntity(oid);
        idSvc.removeEntity(hopefullySameOid);
    }

}
