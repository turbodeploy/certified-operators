package com.vmturbo.topology.processor.stitching;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * Attempt to simulate a basic storage stitching operation.
 */
public class StitchingIntegrationTest {
    private final Gson gson = ComponentGsonFactory.createGson();

    private final StitchingOperationLibrary stitchingOperationLibrary = new StitchingOperationLibrary();
    private final StitchingOperationStore stitchingOperationStore =
        new StitchingOperationStore(stitchingOperationLibrary);

    private final long netAppProbeId = 1234L;
    private final long netAppTargetId = 1111L;
    private final long vcProbeId = 5678L;

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final EntityValidator entityValidator = Mockito.mock(EntityValidator.class);
    private EntityStore entityStore = new EntityStore(targetStore, identityProvider, entityValidator);

    @Test
    public void testVcAlone() throws Exception {
        final Map<Long, EntityDTO> hypervisorEntities =
            entitiesFromFile("protobuf/messages/vcenter_data.json.zip", 1L);

        Mockito.doReturn(Optional.empty())
            .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = stitchingManager.stitch(entityStore);
        final TopologyGraph topoGraph = new TopologyGraph(stitchingContext.constructTopology());

        final TopologyGraph otherGraph = new TopologyGraph(entityStore.constructTopology());

        final Map<Long, TopologyEntityDTO> stitchedEntities = topoGraph.vertices()
            .map(Vertex::getTopologyEntityDtoBuilder)
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final Map<Long, TopologyEntityDTO> unstitchedEntities = otherGraph.vertices()
            .map(Vertex::getTopologyEntityDtoBuilder)
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
            entitiesFromFile("protobuf/messages/netapp_data.json.zip", 1L);
        final Map<Long, EntityDTO> hypervisorEntities =
            entitiesFromFile("protobuf/messages/vcenter_data.json.zip", storageEntities.size() + 1L);

        Mockito.doReturn(Optional.empty())
            .when(entityValidator).validateEntityDTO(Mockito.anyLong(), Mockito.any());
        addEntities(storageEntities, netAppTargetId);
        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(netAppProbeId,
            Collections.singletonList(new StorageStitchingOperation()));
        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager =
                new StitchingManager(stitchingOperationStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = stitchingManager.stitch(entityStore);
        final Map<Long, TopologyEntityDTO.Builder> topology = stitchingContext.constructTopology();

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

        final List<Long> expectedRetained = oidsFor(Stream.of("NETAPP90:NFS",
            "NetApp90:ISCSI-SVM1",
            "NetApp90:ISCSI-SVM2"), hypervisorEntities);

        expectedRemoved.forEach(oid -> assertNull(topology.get(oid)));
        expectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));
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

    private Map<Long, EntityDTO> entitiesFromFile(@Nonnull final String fileName,
                                                  long startingOid) throws Exception {
        final URL url = this.getClass().getClassLoader().getResources(fileName).nextElement();
        ZipInputStream zis = new ZipInputStream(url.openStream());
        final ZipEntry zipEntry = zis.getNextEntry();
        final byte[] bytes= new byte[(int)zipEntry.getSize()];
        int read = 0;
        while ((read += zis.read(bytes, read, bytes.length - read)) < bytes.length) { }
        final String discovery = new String(bytes, "UTF-8");

        final List<EntityDTO> entities = Arrays.asList(gson.fromJson(discovery, EntityDTO[].class));
        AtomicLong nextOid = new AtomicLong(startingOid);
        return entities.stream()
            .collect(Collectors.toMap(entity -> nextOid.getAndIncrement(), Function.identity()));
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId)
        throws EntitiesValidationException, IdentityUninitializedException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     */
    public static Matcher<TopologyEntityDTO> matchesEntity(final TopologyEntityDTO secondEntity) {
        return new BaseMatcher<TopologyEntityDTO>() {
            @Override
            @SuppressWarnings("unchecked")
            public boolean matches(Object o) {
                final TopologyEntityDTO firstEntity = (TopologyEntityDTO) o;
                final TopologyEntityDTO firstEntityWithoutBought = firstEntity.toBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    .build();
                final TopologyEntityDTO secondEntityWithoutBought = secondEntity.toBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    .build();

                if (!firstEntityWithoutBought.equals(secondEntityWithoutBought)) {
                    return false;
                }

                // Compare ignoring order
                final Set<CommoditiesBoughtFromProvider> firstCommodities =
                    firstEntity.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.toSet());
                final Set<CommoditiesBoughtFromProvider> secondCommodities =
                    secondEntity.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.toSet());

                return firstCommodities.equals(secondCommodities);
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("Entities should match");
            }
        };
    }
 }
