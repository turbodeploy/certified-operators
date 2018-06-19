package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.matchesEntityIgnoringOrigin;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.SetCommodityMaxQuantityPostStitchingOperationConfig;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Attempt to simulate a basic storage stitching operation.
 */
public class StitchingIntegrationTest {

    private StatsHistoryServiceMole statsRpcSpy = spy(new StatsHistoryServiceMole());

    private final StitchingOperationLibrary stitchingOperationLibrary = new StitchingOperationLibrary();
    private final StitchingOperationStore stitchingOperationStore =
        new StitchingOperationStore(stitchingOperationLibrary);
    private final PreStitchingOperationLibrary preStitchingOperationLibrary =
        new PreStitchingOperationLibrary();
    private PostStitchingOperationLibrary postStitchingOperationLibrary;

    private final long netAppProbeId = 1234L;
    private final long netAppTargetId = 1111L;
    private final long vcProbeId = 5678L;

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private EntityStore entityStore = new EntityStore(targetStore, identityProvider,
        Clock.systemUTC());
    private final DiskCapacityCalculator diskCapacityCalculator =
        Mockito.mock(DiskCapacityCalculator.class);

    private final Clock clock = Mockito.mock(Clock.class);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsRpcSpy);

    @Before
    public void setup() {
        final StatsHistoryServiceBlockingStub statsServiceClient =
            StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        postStitchingOperationLibrary =
            new PostStitchingOperationLibrary(
                new SetCommodityMaxQuantityPostStitchingOperationConfig(
                    statsServiceClient, 30, 10),  //meaningless values
                diskCapacityCalculator, clock, 0);
        when(probeStore.getProbeIdForType(anyString())).thenReturn(Optional.<Long>empty());
    }

    @Test
    public void testVcAlone() throws Exception {
        final Map<Long, EntityDTO> hypervisorEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_data.json.zip", 1L);

        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        stitchingManager.stitch(stitchingContext, new StitchingJournal<>());
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
            assertThat(stitched, matchesEntityIgnoringOrigin(unstitched));
        });
    }

    @Test
    public void testNetappStitchingWithEmptyJournal() throws Exception {
        testNetappStitching(StitchingJournalFactory.emptyStitchingJournalFactory());
    }

    @Test
    public void testNetappStitchingWithRecordingJournal() throws Exception {
        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
            .configurableStitchingJournalFactory(Clock.systemUTC())
            .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
            .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
            .build());

        testNetappStitching(journalFactory);

        final String journalOutput = journalStringBuilder.toString();
        assertThat(journalOutput, containsString("Merging from STORAGE-31-svm1.test.com:ONTAP_SIM9_LUN1_vol onto"));
        assertThat(journalOutput, containsString("STORAGE-70-NetApp90:ISCSI-SVM1"));
        assertThat(journalOutput, containsString("Merging from DISK_ARRAY-78-DiskArray-NetApp90:ISCSI-SVM1 onto"));
        assertThat(journalOutput, containsString("DISK_ARRAY-34-dataontap-vsim-cm3:aggr2"));
    }

    private void testNetappStitching(@Nonnull final StitchingJournalFactory journalFactory) throws Exception {
        Objects.requireNonNull(journalFactory);

        final Map<Long, EntityDTO> storageEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/netapp_data.json.zip", 1L);
        final Map<Long, EntityDTO> hypervisorEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_data.json.zip", storageEntities.size() + 1L);

        addEntities(storageEntities, netAppTargetId);
        addEntities(hypervisorEntities, 2222L);

        stitchingOperationStore.setOperationsForProbe(netAppProbeId,
            Collections.singletonList(new StorageStitchingOperation()));
        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
            .thenReturn(Collections.singletonList(netAppTarget));

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
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
                .anyMatch(provider -> provider.getEntityType() == EntityType.STORAGE_CONTROLLER)
            );
        });

        final IStitchingJournal<TopologyEntity> postStitchingJournal = journal.childJournal(
            new TopologyEntitySemanticDiffer(journal.getJournalOptions().getVerbosity()));
        stitchingManager.postStitch(new GraphWithSettings(TopologyGraph.newGraph(topology),
            Collections.emptyMap(), Collections.emptyMap()), postStitchingJournal);
    }

    List<Long> oidsFor(@Nonnull final Stream<String> displayNames,
                       @Nonnull final Map<Long, EntityDTO> entityMap) {
        return displayNames
            .map(displayName -> entityMap.entrySet().stream()
                .filter(entityEntry -> entityEntry.getValue().getDisplayName().equals(displayName))
                .findFirst().get())
            .map(Entry::getKey)
            .collect(Collectors.toList());
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId)
        throws IdentityUninitializedException, IdentityMetadataMissingException, IdentityProviderException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }
 }
