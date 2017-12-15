package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.matchesEntity;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Attempt to simulate a basic storage stitching operation.
 */
public class StitchingIntegrationTest {
    private final StitchingOperationLibrary stitchingOperationLibrary = new StitchingOperationLibrary();
    private final StitchingOperationStore stitchingOperationStore =
        new StitchingOperationStore(stitchingOperationLibrary);
    private final PreStitchingOperationLibrary preStitchingOperationLibrary =
        new PreStitchingOperationLibrary();

    private final long netAppProbeId = 1234L;
    private final long netAppTargetId = 1111L;
    private final long vcProbeId = 5678L;

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final EntityValidator entityValidator = Mockito.mock(EntityValidator.class);
    private EntityStore entityStore = new EntityStore(targetStore, identityProvider,
        entityValidator, Clock.systemUTC());

    @Test
    public void testVcAlone() throws Exception {
        final Map<Long, EntityDTO> hypervisorEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_data.json.zip", 1L);

        Mockito.doReturn(Optional.empty())
            .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary, probeStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = stitchingManager.stitch(entityStore);
        final TopologyGraph topoGraph = TopologyGraph.newGraph(stitchingContext.constructTopology());

        final TopologyGraph otherGraph = TopologyGraph.newGraph(entityStore.constructTopology());

        final Map<Long, TopologyEntityDTO> stitchedEntities = topoGraph.entities()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final Map<Long, TopologyEntityDTO> unstitchedEntities = otherGraph.entities()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        stitchedEntities.forEach((oid, stitched) -> {
            final TopologyEntityDTO unstitched = unstitchedEntities.get(oid);
            assertThat(stitched, matchesEntity(unstitched));
        });
    }

    @Test
    public void testNetappStitching() throws Exception {
        final Map<Long, EntityDTO> storageEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/netapp_data.json.zip", 1L);
        final Map<Long, EntityDTO> hypervisorEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_data.json.zip", storageEntities.size() + 1L);

        Mockito.doReturn(Optional.empty())
            .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        addEntities(storageEntities, netAppTargetId);
        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(netAppProbeId,
            Collections.singletonList(new StorageStitchingOperation()));
        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary, probeStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = stitchingManager.stitch(entityStore);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();

        // System should have found the following stitching points:
        // REMOVED                                RETAINED
        // ---------------------------------------------------------------
        // nfs:nfs                           with NETAPP90:NFS
        // svm1.test.com:ONTAP_SIM9_LUN1_vol with NetApp90:ISCSI-SVM1
        // svm2-test.com:ONTAP_SIM9_LUN2_vol with NetApp90:ISCSI-SVM2
        final List<Long> expectedRemoved = oidsFor(Stream.of("nfs:nfs",
                "svm1.test.com:ONTAP_SIM9_LUN1_vol",
                "svm2-test.com:ONTAP_SIM9_LUN2_vol"),
            storageEntities);

        final List<String> expectedRetainedDisplayNames = Arrays.asList(
            "NETAPP90:NFS",
            "NetApp90:ISCSI-SVM1",
            "NetApp90:ISCSI-SVM2"
        );
        final List<Long> expectedRetained = oidsFor(expectedRetainedDisplayNames.stream(), hypervisorEntities);

        expectedRemoved.forEach(oid -> assertNull(topology.get(oid)));
        expectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));

        // After stitching each of the hypervisor (retained) storages should all be connected to
        // a storage controller even though the hypervisor did not discover a storage controller.
        final List<StitchingEntity> hypervisorStorages = stitchingContext.getStitchingGraph().entities()
            .filter(entity -> expectedRetainedDisplayNames.contains(entity.getDisplayName()))
            .collect(Collectors.toList());
        assertEquals(3, hypervisorStorages.size());

        hypervisorStorages.forEach(storage -> {
            final Set<StitchingEntity> providerSubtree = new HashSet<>();
            StitchingTestUtils.visitNeighbors(storage, providerSubtree,
                Collections.emptySet(), StitchingEntity::getProviders);

            assertTrue(providerSubtree.stream()
                .filter(provider -> provider.getEntityType() == EntityType.STORAGE_CONTROLLER)
                .findAny()
                .isPresent());
        });
    }

    List<Long> oidsFor(@Nonnull final Stream<String> displayNames,
                       @Nonnull final Map<Long, EntityDTO> entitymap) {
        return displayNames
            .map(displayName -> entitymap.entrySet().stream()
                .filter(entityEntry -> entityEntry.getValue().getDisplayName().equals(displayName))
                .findFirst().get())
            .map(Entry::getKey)
            .collect(Collectors.toList());
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId)
        throws EntitiesValidationException, IdentityUninitializedException, IdentityMetadataMissingException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }
 }
