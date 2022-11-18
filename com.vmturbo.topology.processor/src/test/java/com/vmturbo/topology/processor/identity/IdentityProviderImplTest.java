package com.vmturbo.topology.processor.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Test the identity provider API.
 */
public class IdentityProviderImplTest {

    private static final String SAME_ID = "same-id";
    /**
     * Expected exception rule.
     */
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private IdentityProvider identityProvider;

    private KeyValueStore keyValueStore;

    private ProbeInfo baseProbeInfo;

    private final ProbeInfoCompatibilityChecker compatibilityChecker = mock(ProbeInfoCompatibilityChecker.class);

    private final TopologyProcessorDBConfig dbConfig = mock(TopologyProcessorDBConfig.class);

    private final long assignedIdReloadReattemptIntervalSeconds = 0;

    private long probeId;

    private static final String ID_PROP = "id";
    private static final String ENTITY_ID = "123";

    /**
     * Initializes the tests.
     *
     * @throws Exception on exception occurred
     */
    @Before
    public void setup() throws Exception {
        keyValueStore = new MapKeyValueStore();
        when(dbConfig.dsl()).thenReturn(mock(DSLContext.class));
        identityProvider = new IdentityProviderImpl(
            keyValueStore,
            compatibilityChecker, 0L, mock(IdentityDatabaseStore.class),
            10,
            assignedIdReloadReattemptIntervalSeconds,
            mock(StaleOidManagerImpl.class), false);
        identityProvider.getStore().initialize();
        baseProbeInfo = Probes.defaultProbe;
        identityProvider.waitForInitializedStore();
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
            .setProbeType("test-probe-2")
            .setProbeCategory("test-category-2")
            .addEntityMetadata(
                EntityIdentityMetadata.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .addNonVolatileProperties(PropertyMetadata.newBuilder().setName(ID_PROP))
            )
            .build();
        probeId = identityProvider.getProbeId(probeInfo);
    }

    /**
     * Tests constructor initializes identity generator.
     */
    @Test
    public void testConstructorInitializesIdentityGenerator() {
        final long idGenPrefix = IdentityGenerator.MAXPREFIX - 1;
        identityProvider = new IdentityProviderImpl(keyValueStore, compatibilityChecker,
            idGenPrefix, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);

        assertEquals(idGenPrefix, IdentityGenerator.getPrefix());
    }

    /**
     * Test that target ID's are correctly assigned.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testGetTargetId() throws Exception {
        TargetSpec targetSpec = new TargetSpec(0L, Collections.emptyList(), Optional.empty(),
                "System");
        assertNotEquals(identityProvider.getTargetId(targetSpec.toDto()),
                identityProvider.getTargetId(targetSpec.toDto()));
    }

    /**
     * Test that probeId's are correctly assigned, with equality
     * based exclusively on probe type.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testGetProbeId() throws Exception {
        long probeId = identityProvider.getProbeId(baseProbeInfo);
        ProbeInfo eqProbe = ProbeInfo.newBuilder(baseProbeInfo)
                .setProbeCategory("otherCat").setUiProbeCategory("uiProbeCat").build();

        when(compatibilityChecker.areCompatible(baseProbeInfo, eqProbe)).thenReturn(true);
        assertEquals(probeId, identityProvider.getProbeId(eqProbe));
        // Should check compatibility.
        verify(compatibilityChecker).areCompatible(baseProbeInfo, eqProbe);

        ProbeInfo diffProbe = ProbeInfo.newBuilder(baseProbeInfo)
                .setProbeType("test2").build();
        assertNotEquals(probeId, identityProvider.getProbeId(diffProbe));
    }

    /**
     * Tests when probe ID is incompatible.
     *
     * @throws Exception on exceptions occurred
     */
    @Test(expected = IdentityProviderException.class)
    public void testGetProbeIdIncompatible() throws Exception {
        // pre-register the base probe
        identityProvider.getProbeId(baseProbeInfo);
        // copy the base probe and just change the category
        ProbeInfo incompatible = ProbeInfo.newBuilder(baseProbeInfo)
            .setProbeCategory("otherCat").setUiProbeCategory("uiProbeCat").build();
        // setup the "compatible false" result
        when(compatibilityChecker.areCompatible(baseProbeInfo, incompatible)).thenReturn(false);
        // act
        identityProvider.getProbeId(incompatible);
    }

    /**
     * Test that the same entity according to the metadata gets assigned
     * the same OID, and different entities get different OIDs.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testGetEntityId() throws Exception {
        EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("test")
                .build();
        final Map<Long, EntityDTO> ids = identityProvider.getIdsForEntities(probeId,
                Collections.singletonList(entity));
        assertEquals(1, ids.size());
        final long entityId = ids.keySet().iterator().next();
        // Entity with the same id, but different overall
        EntityDTO sameEntity = EntityDTO.newBuilder(entity)
                .setDisplayName("florence")
                .build();
        assertEquals(Long.valueOf(entityId), identityProvider.getIdsForEntities(probeId,
                Collections.singletonList(sameEntity)).keySet().iterator().next());
        EntityDTO diffEntity = EntityDTO.newBuilder(entity)
                .setId("test2")
                .build();
        assertNotEquals(Long.valueOf(entityId),
                identityProvider.getIdsForEntities(probeId,
                        Collections.singletonList(diffEntity)).keySet().iterator().next());
    }

    /**
     * Test that the same entity according to the input properties gets assigned
     * the same OID, and different entities get Optional Long OIDs.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testGetEntityIdFromProperties() throws Exception {
        EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("test")
                .setDisplayName("testName")
                .build();
        final Map<Long, EntityDTO> ids = identityProvider.getIdsForEntities(probeId,
                Collections.singletonList(entity));
        assertEquals(1, ids.size());
        final long entityId = ids.keySet().iterator().next();
        // Entity with the same id and same name
        Map<String, String> identifyingProperties = new HashMap<String, String>();
        identifyingProperties.put("id", "test");
        identifyingProperties.put("displayName", "testName");
        assertEquals(Optional.of(entityId),
                identityProvider.getOidFromProperties(identifyingProperties, probeId, entity.getEntityType()));
        identifyingProperties.clear();
        // Entity with the same id and different name
        identifyingProperties.put("id", "test");
        identifyingProperties.put("displayName", "newTestName");
        assertEquals(Optional.of(entityId),
                identityProvider.getOidFromProperties(identifyingProperties, probeId, entity.getEntityType()));
        identifyingProperties.clear();
        // Entity with different id and same name
        identifyingProperties.put("id", "newTest");
        identifyingProperties.put("displayName", "testName");
        Optional<Long>  retrievedOid = identityProvider.getOidFromProperties(identifyingProperties, probeId, entity.getEntityType());
        assertNotEquals(Optional.of(entityId), retrievedOid);
        assertEquals(Optional.empty(), retrievedOid);
    }

    /**
     * {@link IdentityProvider#getIdsFromIdentifyingPropertiesValues(long, List)}
     * should return a Map with an oid for a provided {@link EntityIdentifyingPropertyValues} instance. When called a
     * second time with the same input, the same oid must be returned.
     */
    @Test
    public void testEntityIdentifyingPropertyValuesDiscoveredReturnSameOid() throws IdentityServiceException {
        // given
        final EntityIdentifyingPropertyValues identifyingPropertyValues = createIdentifyingPropertyValues(
            EntityType.VIRTUAL_MACHINE, ID_PROP, ENTITY_ID);
        // when
        final Map<Long, EntityIdentifyingPropertyValues> ids = identityProvider.getIdsFromIdentifyingPropertiesValues(
            probeId, Collections.singletonList(identifyingPropertyValues));
        // then
        Assert.assertEquals(1, ids.size());
        final Map.Entry<Long, EntityIdentifyingPropertyValues> entry = ids.entrySet().iterator().next();
        Assert.assertEquals(identifyingPropertyValues, entry.getValue());
        final long oid = ids.keySet().iterator().next();

        // Test that IdentityProvider#getIdsFromIdentifyingPropertiesValues returns the same OID when presented with the
        // same instance of EntityIdentifyingPropertyValues
        final Map<Long, EntityIdentifyingPropertyValues> roundTwoIds = identityProvider
            .getIdsFromIdentifyingPropertiesValues(probeId, Collections.singletonList(identifyingPropertyValues));
        Assert.assertEquals(oid, (long)roundTwoIds.keySet().iterator().next());
    }

    /**
     * Test that when an {@link EntityDTO} is encountered with the same identifying properties as a
     * {@link EntityIdentifyingPropertyValues} instance, then both resolve to the same OID.
     */
    @Test
    public void testGetOidForEntityDTOAndIdentities() throws IdentityServiceException {
        // given
        final EntityDTO entityDTO = EntityDTO.newBuilder().setId(ENTITY_ID).setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();
        final Map<Long, EntityDTO> entityId = identityProvider.getIdsForEntities(probeId,
            Collections.singletonList(entityDTO));
        final EntityIdentifyingPropertyValues identifyingPropertyValues = createIdentifyingPropertyValues(
            EntityType.VIRTUAL_MACHINE, ID_PROP, ENTITY_ID);
        // when
        final Map<Long, EntityIdentifyingPropertyValues> identifyingPropertiesValuesId = identityProvider
            .getIdsFromIdentifyingPropertiesValues(probeId, Collections.singletonList(identifyingPropertyValues));
        // then
        Assert.assertEquals(entityId.keySet(), identifyingPropertiesValuesId.keySet());
    }

    /**
     * Test that when a property sent via {@link EntityIdentifyingPropertyValues} is not found in
     * {@link EntityIdentityMetadata}, an {@link IdentityServiceException} is thrown.
     *
     * @throws Exception if test encounters an error.
     */
    @Test
    public void testExceptionWithPropertyMismatch() throws Exception {
        exception.expect(IdentityServiceException.class);
        exception.expectMessage("Property id is not present in the input identifying properties");
        final EntityIdentifyingPropertyValues identifyingPropertyValues =
            createIdentifyingPropertyValues(EntityType.VIRTUAL_MACHINE, "displayName", "vm-1");
        identityProvider.getIdsFromIdentifyingPropertiesValues(probeId,
            Collections.singletonList(identifyingPropertyValues));
    }

    /**
     * Test that when {@link EntityIdentityMetadata} is not provided for a given {@link EntityType} for a probe,
     * {@link IdentityProvider#getIdsFromIdentifyingPropertiesValues(long, List)} throws an
     * {@link IdentityServiceException}.
     *
     * @throws Exception if test encounters an error.
     */
    @Test
    public void testExceptionNoIdentityMetadataRegistered() throws Exception {
        exception.expect(IdentityServiceException.class);
        exception.expectMessage("No Identity metadata registered for Entity type VIRTUAL_VOLUME");
        final EntityIdentifyingPropertyValues identifyingPropertyValues =
            createIdentifyingPropertyValues(EntityType.VIRTUAL_VOLUME, ID_PROP, ENTITY_ID);
        identityProvider.getIdsFromIdentifyingPropertiesValues(probeId,
            Collections.singletonList(identifyingPropertyValues));
    }

    /**
     * Test that if {@link EntityIdentifyingPropertyValues} contains an empty
     * {@link EntityIdentifyingPropertyValues#getIdentifyingPropertyValuesMap()}, then an
     * {@link IdentityServiceException} is encountered.
     *
     * @throws Exception if test encounters an error.
     */
    @Test
    public void testExceptionEmptyIdentityPropValuesMap() throws Exception {
        exception.expect(IdentityServiceException.class);
        exception.expectMessage("Property id is not present in the input identifying properties");
        final EntityIdentifyingPropertyValues identifyingPropertyValues = EntityIdentifyingPropertyValues.newBuilder()
            .setEntityId(ENTITY_ID).setEntityType(EntityType.VIRTUAL_MACHINE).build();
        identityProvider.getIdsFromIdentifyingPropertiesValues(probeId,
            Collections.singletonList(identifyingPropertyValues));
    }

    /**
     * Test that if {@link EntityIdentifyingPropertyValues} does not have {@link EntityType} set, then an
     * {@link IdentityServiceException} is encountered.
     *
     * @throws Exception if test encounters an error.
     */
    @Test
    public void testExceptionEntityTypeNotSet() throws Exception {
        exception.expect(IdentityServiceException.class);
        exception.expectMessage("Entity type not set for EntityIdentifyingPropertyValues");
        final EntityIdentifyingPropertyValues identifyingPropertyValues = EntityIdentifyingPropertyValues.newBuilder()
            .setEntityId(ENTITY_ID).build();
        identityProvider.getIdsFromIdentifyingPropertiesValues(probeId,
            Collections.singletonList(identifyingPropertyValues));
    }

    /**
     * Test that when 2 different probe types report the same identity metadata for an entity type, the OIDs returned by
     * {@link IdentityProvider#getIdsFromIdentifyingPropertiesValues(long, List)} called with the first probe id are the
     * same as the OIDs returned by {@link IdentityProvider#getIdsForEntities(long, List)} called with the second
     * probe id, when the actual identifying property values are the same.
     *
     * @throws Exception if test encounters an error.
     */
    @Test
    public void testOidRetainedAcrossDifferentProbes() throws Exception {
        // register a new probe
        final ProbeInfo probeInfo2 = ProbeInfo.newBuilder()
            .setProbeType("test-probe-3")
            .setProbeCategory("test-category-2")
            .addEntityMetadata(
                EntityIdentityMetadata.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .addNonVolatileProperties(PropertyMetadata.newBuilder().setName(ID_PROP))
            )
            .build();
        final long probeId2 = identityProvider.getProbeId(probeInfo2);
        Assert.assertNotEquals(probeId2, probeId);

        // retrieve OIDs using probe one
        final EntityDTO entityDTO = EntityDTO.newBuilder().setId(ENTITY_ID)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();
        final Map<Long, EntityDTO> probeOneIds = identityProvider.getIdsForEntities(probeId,
            Collections.singletonList(entityDTO));

        // retrieve OIDs using probe two
        final EntityIdentifyingPropertyValues identifyingPropertyValues = EntityIdentifyingPropertyValues.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .putIdentifyingPropertyValues(ID_PROP, ENTITY_ID)
            .setEntityId(ENTITY_ID).build();
        final Map<Long, EntityIdentifyingPropertyValues> probeTwoIds = identityProvider
            .getIdsFromIdentifyingPropertiesValues(probeId2, Collections.singletonList(identifyingPropertyValues));

        Assert.assertEquals(probeOneIds.keySet(), probeTwoIds.keySet());
    }

    private EntityIdentifyingPropertyValues createIdentifyingPropertyValues(final EntityType entityType,
                                                                            final String prop, final String val) {
        return EntityIdentifyingPropertyValues.newBuilder()
            .setEntityType(entityType)
            .putIdentifyingPropertyValues(prop, val)
            .build();
    }

    /**
     * Test that two entities with the same time and same identity-metadata but different
     * EntityTypes are different.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testEntityTypeDistinguishing() throws Exception {
        // arrange
        ProbeInfo probeInfo = ProbeInfo.newBuilder(baseProbeInfo)
                .addEntityMetadata(
                        EntityIdentityMetadata.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id")))
                .addEntityMetadata(
                        EntityIdentityMetadata.newBuilder()
                                .setEntityType(EntityType.APPLICATION_COMPONENT)
                                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id")))
                .build();
        long probeId = identityProvider.getProbeId(probeInfo);

        EntityDTO vmEntity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(SAME_ID)
                .build();
        EntityDTO appEntity = EntityDTO.newBuilder(vmEntity)
            .setEntityType(EntityType.APPLICATION_COMPONENT)
            .setDisplayName(SAME_ID)
            .build();
        // act - compute the IDs for the VM and for the APP Entities
        final Map<Long, EntityDTO> vmIdMap = identityProvider.getIdsForEntities(probeId,
                Collections.singletonList(vmEntity));
        final Map<Long, EntityDTO> appIdMap = identityProvider.getIdsForEntities(probeId,
            Collections.singletonList(appEntity));
        // Ensure that the IDs are different
        assertEquals(1, vmIdMap.size());
        final long vmId = vmIdMap.keySet().iterator().next();
        assertEquals(1, appIdMap.size());
        final long appId = appIdMap.keySet().iterator().next();
        assertNotEquals(vmId, appId);
    }

    /**
     * Test getting entity ID's when the probe discovering the
     * entity doesn't have any metadata for that probe type.
     *
     * @throws IdentityServiceException on errors assigning OIDs occur.
     * @throws IdentityProviderException on errors providing OIDs occur
     */
    @Test
    public void testGetEntityIdNoMetadata()
            throws IdentityServiceException, IdentityProviderException {
        long probeId = identityProvider.getProbeId(baseProbeInfo);

        EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("test")
                .build();

        exception.expect(IdentityServiceException.class);
        assertTrue(identityProvider.getIdsForEntities(probeId,
            Collections.singletonList(entity)).isEmpty());
    }

    /**
     * Test that the identity provider saves probe ID's into the backend {@link KeyValueStore}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProbeIdSave() throws Exception {
        final KeyValueStore mockKvStore = mock(KeyValueStore.class);

        final IdentityProvider newInstance = new IdentityProviderImpl( mockKvStore,
            compatibilityChecker, 0L, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);

        final long probeId = newInstance.getProbeId(baseProbeInfo);
        // Verify that the call to save the probeId happened.
        verify(mockKvStore, Mockito.times(1)).put(
                        Mockito.eq("id/probes/" + baseProbeInfo.getProbeType()),
                        Mockito.eq(Long.toString(probeId)));

        when(compatibilityChecker.areCompatible(baseProbeInfo, baseProbeInfo)).thenReturn(true);
        // Verify that getting the same ID again doesn't result in another call to the KV-store.
        assertEquals(probeId, newInstance.getProbeId(baseProbeInfo));
        verify(mockKvStore, Mockito.times(1)).put(
                        Mockito.eq("id/probes/" + baseProbeInfo.getProbeType()),
                        Mockito.eq(Long.toString(probeId)));
    }

    /**
     * Test that the identity provider reads probe ID's from the backend {@link KeyValueStore} at
     * initialization time.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProbeIdRestart() throws Exception {
        final long probeId = identityProvider.getProbeId(baseProbeInfo);

        // A different instance of the identity provider should assign
        // the same probe ID to the same probe.
        final IdentityProvider newInstance =
            new IdentityProviderImpl(keyValueStore, compatibilityChecker, 0L,
                mock(IdentityDatabaseStore.class), 10, assignedIdReloadReattemptIntervalSeconds,
                mock(StaleOidManagerImpl.class), false);
        assertEquals(probeId, newInstance.getProbeId(baseProbeInfo));
    }

    /**
     * Tests get entity id from identity service throwing exception.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEntityIdIdSvcOperationException() throws Exception {
        testGetEntityIdIdSvcException(new IdentityServiceException(""));
    }

    /**
     * Test that the identity provider behaves correctly when the underlying
     * identity service throws an exception.
     *
     * @param e The exception that the identity service should throw.
     * @throws Exception If any exception occurs.
     */
    private void testGetEntityIdIdSvcException(Exception e) throws Exception {
        IdentityService identityService = mock(IdentityService.class);
        when(identityService.getOidsForObjects(any()))
                .thenThrow(e);
        IdentityProvider provider = new IdentityProviderImpl(new MapKeyValueStore(),
            compatibilityChecker, 0L, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);

        ProbeInfo probeInfo = ProbeInfo.newBuilder(baseProbeInfo)
                .addEntityMetadata(
                        EntityIdentityMetadata.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id"))
                )
                .build();
        long probeId = provider.getProbeId(probeInfo);
        EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("test")
                .build();

        exception.expect(IdentityServiceException.class);
        provider.getIdsForEntities(probeId, Collections.singletonList(entity));
    }

    /**
     * Tests restoring with bad JSON format.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadJsonRestore1() {
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(new MapKeyValueStore(),
            compatibilityChecker, 0, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);
        providerImpl.restoreStringDiags(ImmutableList.of("blah", "", ""), null);
    }

    /**
     * Tests restoring with bad JSON format.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadJsonRestore2() {
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(new MapKeyValueStore(),
            compatibilityChecker, 0, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);
        providerImpl.restoreStringDiags(ImmutableList.of("{}", "blah", ""), null);
    }

    /**
     * Tests restoring with wrong number of lines.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongLinesRestore() {
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(new MapKeyValueStore(),
            compatibilityChecker, 0, mock(IdentityDatabaseStore.class), 10,
            assignedIdReloadReattemptIntervalSeconds, mock(StaleOidManagerImpl.class), false);
        providerImpl.restoreStringDiags(Collections.emptyList(), null);
    }

    /**
     * Tests restoring from backup.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testBackupRestore() throws Exception {
        final IdentityService identityService = mock(IdentityService.class);
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(identityService,
            new MapKeyValueStore(), compatibilityChecker, 0, mock(StaleOidManagerImpl.class));

        final ProbeInfo probeInfo = ProbeInfo.newBuilder(baseProbeInfo)
            .addEntityMetadata(
                EntityIdentityMetadata.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id"))
            )
            .build();
        final EntityDTO entity = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("test")
            .build();

        final long probeId = providerImpl.getProbeId(probeInfo);
        when(identityService.getOidsForObjects(any())).thenReturn(Collections.singletonList(7L));

        final Map<Long, EntityDTO> idMap =
            providerImpl.getIdsForEntities(probeId, Collections.singletonList(entity));
        assertEquals(1, idMap.size());
        assertEquals(entity, idMap.get(7L));

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        final ArgumentCaptor<String> diagsCaptor = ArgumentCaptor.forClass(String.class);
        // Collect the diags
        providerImpl.collectDiags(appender);
        verify(identityService).backup(any(Writer.class));
        verify(appender, Mockito.atLeastOnce()).appendString(diagsCaptor.capture());

        // Create a new provider, restore the diags, and make sure
        // the new providers behaves just like the old one.
        final IdentityProviderImpl newProvider = new IdentityProviderImpl(identityService,
            new MapKeyValueStore(), compatibilityChecker, 0, mock(StaleOidManagerImpl.class));
        newProvider.restoreStringDiags(diagsCaptor.getAllValues(), null);
        verify(identityService).restore(any(), any());
        // It should assign the same ID for the same probe type.
        assertEquals(probeId, newProvider.getProbeId(probeInfo));
        assertEquals(idMap, newProvider.getIdsForEntities(probeId,
            Collections.singletonList(entity)));
    }
}
