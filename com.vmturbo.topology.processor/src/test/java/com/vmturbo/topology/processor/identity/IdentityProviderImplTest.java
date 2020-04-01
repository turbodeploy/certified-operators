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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Test the identity provider API.
 */
public class IdentityProviderImplTest {

    private static final String SAME_ID = "same-id";
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private IdentityProvider identityProvider;

    private KeyValueStore keyValueStore;

    private ProbeInfo baseProbeInfo;

    private ProbeInfoCompatibilityChecker compatibilityChecker = mock(ProbeInfoCompatibilityChecker.class);

    @Before
    public void setup() throws Exception {
        keyValueStore = new MapKeyValueStore();
        identityProvider = new IdentityProviderImpl(
            new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                    mock(IdentityDatabaseStore.class)), new HeuristicsMatcher()),
            keyValueStore,
            compatibilityChecker, 0L);
        baseProbeInfo = Probes.defaultProbe;
    }

    @Test
    public void testConstructorInitializesIdentityGenerator() {
        final long idGenPrefix = IdentityGenerator.MAXPREFIX - 1;
        identityProvider = new IdentityProviderImpl(new IdentityService(
            new IdentityServiceInMemoryUnderlyingStore(mock(IdentityDatabaseStore.class)),
                new HeuristicsMatcher()),
            keyValueStore,
            compatibilityChecker,
            idGenPrefix
        );

        assertEquals(idGenPrefix, IdentityGenerator.getPrefix());
    }

    /**
     * Test that target ID's are correctly assigned.
     *
     * @throws Exception If any exception thrown.
     */
    @Test
    public void testGetTargetId() throws Exception {
        TargetSpec targetSpec = new TargetSpec(0L, Collections.emptyList());
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
        ProbeInfo probeInfo = ProbeInfo.newBuilder(baseProbeInfo)
                .addEntityMetadata(
                        EntityIdentityMetadata.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id"))
                )
                .build();
        long probeId = identityProvider.getProbeId(probeInfo);

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
                                .setEntityType(EntityType.APPLICATION)
                                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id")))
                .build();
        long probeId = identityProvider.getProbeId(probeInfo);

        EntityDTO vmEntity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(SAME_ID)
                .build();
        EntityDTO appEntity = EntityDTO.newBuilder(vmEntity)
            .setEntityType(EntityType.APPLICATION)
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
     */
    @Test
    public void testGetEntityIdNoMetadata() throws IdentityUninitializedException,
        IdentityMetadataMissingException, IdentityProviderException {
        long probeId = identityProvider.getProbeId(baseProbeInfo);

        EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("test")
                .build();

        exception.expect(IdentityMetadataMissingException.class);
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

        final IdentityProvider newInstance = new IdentityProviderImpl(
            mock(IdentityService.class), mockKvStore, compatibilityChecker, 0L);

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
            new IdentityProviderImpl(mock(IdentityService.class), keyValueStore, compatibilityChecker, 0L);
        assertEquals(probeId, newInstance.getProbeId(baseProbeInfo));
    }

    @Test
    public void testGetEntityIdIdSvcOperationException() throws Exception {
        testGetEntityIdIdSvcException(new IdentityServiceOperationException(""));
    }

    @Test
    public void testGetEntityIdIdSvcWrongSetException() throws Exception {
        testGetEntityIdIdSvcException(new IdentityWrongSetException(""));
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
        when(identityService.getEntityOIDs(any()))
                .thenThrow(e);
        IdentityProvider provider = new IdentityProviderImpl(identityService,
            new MapKeyValueStore(), compatibilityChecker, 0L);

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

        exception.expect(IdentityProviderException.class);
        provider.getIdsForEntities(probeId, Collections.singletonList(entity));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadJsonRestore1() {
        final IdentityService identityService = mock(IdentityService.class);
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(identityService,
                new MapKeyValueStore(), compatibilityChecker, 0);
        providerImpl.restoreDiags(ImmutableList.of("blah", "", ""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadJsonRestore2() {
        final IdentityService identityService = mock(IdentityService.class);
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(identityService,
                new MapKeyValueStore(), compatibilityChecker, 0);
        providerImpl.restoreDiags(ImmutableList.of("{}", "blah", ""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongLinesRestore() {
        final IdentityService identityService = mock(IdentityService.class);
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(identityService,
                new MapKeyValueStore(), compatibilityChecker, 0);
        providerImpl.restoreDiags(Collections.emptyList());
    }

    @Test
    public void testBackupRestore() throws Exception {
        final IdentityService identityService = mock(IdentityService.class);
        final IdentityProviderImpl providerImpl = new IdentityProviderImpl(identityService,
                new MapKeyValueStore(), compatibilityChecker, 0);

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
        when(identityService.getEntityOIDs(any())).thenReturn(Collections.singletonList(7L));

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
                new MapKeyValueStore(), compatibilityChecker, 0);
        newProvider.restoreDiags(diagsCaptor.getAllValues());
        verify(identityService).restore(any());
        // It should assign the same ID for the same probe type.
        assertEquals(probeId, newProvider.getProbeId(probeInfo));
        assertEquals(idMap, newProvider.getIdsForEntities(probeId,
                Collections.singletonList(entity)));
    }
}
