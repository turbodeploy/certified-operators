package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.matchesEntityIgnoringOrigin;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.ListStringToListStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToListStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.ListStringToStringDataDrivenStitchingOperation;
import com.vmturbo.stitching.ListStringToStringStitchingMatchingMetaDataImpl;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.fabric.FabricChassisStitchingOperation;
import com.vmturbo.stitching.fabric.FabricPMStitchingOperation;
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
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
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
    private final long vcTargetId = 2222L;
    private final long ucsProbeId = 2468L;
    private final long ciscoVcenterProbeId = 2345L;
    private final long ucsTargetId = 2121L;
    private final long ciscoVcenterTargetId = 3131L;
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
        when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
        when(probeStore.getProbe(ucsProbeId)).thenReturn(Optional.empty());
        // return non-cloud probe types so it gets treated as normal probes
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
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

        final StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore, Collections.emptyMap());
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

    private static Collection<CommodityType> boughtDataFromDiskArrayToStorage =
            ImmutableList.of(CommodityType.STORAGE_AMOUNT,
                    CommodityType.STORAGE_PROVISIONED,
                    CommodityType.STORAGE_LATENCY,
                    CommodityType.STORAGE_ACCESS,
                    CommodityType.EXTENT);

    private static Collection<CommodityBoughtMetadata> storageBoughtCommodityData =
            ImmutableList.of(CommodityBoughtMetadata.newBuilder()
                    .addAllCommodityMetadata(boughtDataFromDiskArrayToStorage)
                    .setProviderType(EntityType.DISK_ARRAY)
                    .setReplacesProvider(EntityType.DISK_ARRAY).build(),
                    CommodityBoughtMetadata.newBuilder()
                            .addAllCommodityMetadata(boughtDataFromDiskArrayToStorage)
                            .setProviderType(EntityType.LOGICAL_POOL)
                            .setReplacesProvider(EntityType.DISK_ARRAY).build());

    private StitchingOperation getDataDrivenStorageStitchingOperation() {
        EntityField externalNames = EntityField.newBuilder().addMessagePath("storage_data")
                .setFieldName("externalName").build();
        MatchingData storageMatchingData = MatchingData.newBuilder()
                .setMatchingField(externalNames).build();
        MatchingMetadata storageMatchingMetadata = MatchingMetadata.newBuilder()
                .addMatchingData(storageMatchingData).setReturnType(ReturnType.LIST_STRING)
                .addExternalEntityMatchingProperty(storageMatchingData)
                .setExternalEntityReturnType(ReturnType.LIST_STRING).build();
        final MergedEntityMetadata storageMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(storageMatchingMetadata)
                        .addAllCommoditiesBought(storageBoughtCommodityData)
                        .build();
        return new ListStringToListStringDataDrivenStitchingOperation(
                new ListStringToListStringStitchingMatchingMetaDataImpl(EntityType.STORAGE,
                        storageMergeEntityMetadata), Sets.newHashSet(ProbeCategory.HYPERVISOR));
    }

    @Test
    public void testNetappStitchingWithDataDrivenStitchingOperation() throws Exception {
        testNetappStitchingWithRecordingJournal(getDataDrivenStorageStitchingOperation());
    }

    @Test
    public void testNetappStitchingWithEmptyJournal() throws Exception {
        testNetappStitching(StitchingJournalFactory.emptyStitchingJournalFactory(),
                new StorageStitchingOperation());
    }

    @Test
    public void testStandardNetappStitching() throws Exception {
        testNetappStitchingWithRecordingJournal(new StorageStitchingOperation());
    }

    private void testNetappStitchingWithRecordingJournal(
            StitchingOperation storageStitchingOperationToUse) throws Exception {
        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());

        testNetappStitching(journalFactory, storageStitchingOperationToUse);

        final String journalOutput = journalStringBuilder.toString();
        assertThat(journalOutput, containsString("Merging from STORAGE-31-svm1.test.com:ONTAP_SIM9_LUN1_vol onto"));
        assertThat(journalOutput, containsString("STORAGE-70-NetApp90:ISCSI-SVM1"));
        assertThat(journalOutput, containsString("Merging from DISK_ARRAY-78-DiskArray-NetApp90:ISCSI-SVM1 onto"));
        assertThat(journalOutput, containsString("DISK_ARRAY-34-dataontap-vsim-cm3:aggr2"));
    }

    private void testNetappStitching(@Nonnull final StitchingJournalFactory journalFactory,
                                     StitchingOperation storageStitchingOperationToTest) throws Exception {
        Objects.requireNonNull(journalFactory);

        final Map<Long, EntityDTO> storageEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/netapp_data.json.zip", 1L);
        final Map<Long, EntityDTO> hypervisorEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_data.json.zip", storageEntities.size() + 1L);

        addEntities(storageEntities, netAppTargetId);
        addEntities(hypervisorEntities, vcTargetId);

        stitchingOperationStore.setOperationsForProbe(netAppProbeId,
                Collections.singletonList(storageStitchingOperationToTest));
        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
        final Target netAppTarget = Mockito.mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        when(targetStore.getProbeTargets(netAppProbeId))
                .thenReturn(Collections.singletonList(netAppTarget));
        final Target vcTarget = Mockito.mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);

        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.STORAGE))
                .thenReturn(Collections.singletonList(netAppProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));

        final StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore, Collections.emptyMap());
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

    private static Collection<CommodityType> boughtDataFromChassisToPM =
            ImmutableList.of(CommodityType.SPACE,
                    CommodityType.POWER,
                    CommodityType.COOLING,
                    CommodityType.DATACENTER);

    private static Collection<CommodityBoughtMetadata> pmBoughtCommodityData =
            ImmutableList.of(CommodityBoughtMetadata.newBuilder()
                            .addAllCommodityMetadata(boughtDataFromChassisToPM)
                            .setProviderType(EntityType.CHASSIS)
                            .setReplacesProvider(EntityType.DATACENTER).build(),
                    CommodityBoughtMetadata.newBuilder()
                            .addAllCommodityMetadata(boughtDataFromChassisToPM)
                            .setProviderType(EntityType.DATACENTER)
                            .setReplacesProvider(EntityType.DATACENTER).build(),
                    CommodityBoughtMetadata.newBuilder()
                            .addAllCommodityMetadata(ImmutableList.of(CommodityType.NET_THROUGHPUT))
                            .setProviderType(EntityType.SWITCH).build(),
                    CommodityBoughtMetadata.newBuilder()
                            .addAllCommodityMetadata(ImmutableList.of(CommodityType.NET_THROUGHPUT))
                            .setProviderType(EntityType.IO_MODULE).build());

    @Test
    public void testUCSStitchingWithStandardOperations() throws Exception {
        testUCSStitching(ImmutableList.of(new FabricChassisStitchingOperation(),
                new FabricPMStitchingOperation()));
    }

    @Test
    public void testUCSStitchingWithGenericOperations() throws Exception {
        testUCSStitching(ImmutableList.of(getDataDrivenFabricStitchingOperation()));
    }

    private StitchingOperation getDataDrivenFabricStitchingOperation() {
        EntityPropertyName pmuuidProperty = EntityPropertyName.newBuilder()
                .setPropertyName("PM_UUID").build();
        EntityField idField = EntityField.newBuilder().setFieldName("id").build();
        MatchingData fabricMatchingData = MatchingData.newBuilder()
                .setMatchingProperty(pmuuidProperty).setDelimiter(",").build();
        MatchingData fabricExternalMatchingData = MatchingData.newBuilder()
                .setMatchingField(idField).build();
        MatchingMetadata fabricMatchingMetadata = MatchingMetadata.newBuilder()
                .addMatchingData(fabricMatchingData).setReturnType(ReturnType.LIST_STRING)
                .addExternalEntityMatchingProperty(fabricExternalMatchingData)
                .setExternalEntityReturnType(ReturnType.STRING).build();
        final MergedEntityMetadata fabricMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(fabricMatchingMetadata)
                        .addAllCommoditiesBought(pmBoughtCommodityData)
                        .build();
        return new ListStringToStringDataDrivenStitchingOperation(
                new ListStringToStringStitchingMatchingMetaDataImpl(EntityType.PHYSICAL_MACHINE,
                        fabricMergeEntityMetadata), Sets.newHashSet(ProbeCategory.HYPERVISOR));
    }


    private void testUCSStitching(List<StitchingOperation<?, ?>> fabricStitchingOperationsToTest)
            throws Exception {
        final Map<Long, EntityDTO> ucsEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/cisco-ucs_data.json.zip",
                        1L);
        final Map<Long, EntityDTO> hypervisorEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/cisco-vcenter_data.json.zip",
                        ucsEntities.size() + 1L);

        addEntities(ucsEntities, ucsTargetId);
        addEntities(hypervisorEntities, ciscoVcenterTargetId);

        stitchingOperationStore.setOperationsForProbe(ucsProbeId,
                fabricStitchingOperationsToTest);
        stitchingOperationStore.setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore);
        final Target ucsTarget = Mockito.mock(Target.class);
        when(ucsTarget.getId()).thenReturn(ucsTargetId);
        final Target ucsVcenterTarget = Mockito.mock(Target.class);
        when(ucsVcenterTarget.getId()).thenReturn(ciscoVcenterTargetId);

        when(targetStore.getProbeTargets(ucsProbeId))
                .thenReturn(Collections.singletonList(ucsTarget));

        when(targetStore.getProbeTargets(ciscoVcenterProbeId))
                .thenReturn(Collections.singletonList(ucsVcenterTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.FABRIC))
                .thenReturn(Collections.singletonList(ucsProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(ciscoVcenterProbeId));

        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore, Collections.emptyMap());
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());

        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();

        // These proxy PMs should have been replaced by real PMs from the hypervisor
        final List<Long> expectedRemoved = oidsFor(Stream.of(
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-2/blade-1",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3/blade-3",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3/blade-2",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3/blade-1"),
                ucsEntities);

        // These PMs from the hypervisor should have gotten their Datacenters replaced by a Chassis
        // from the proxy PMs and should have also inherited their Switch and IOModule providers
        final List<String> expectedRetainedDisplayNames = Arrays.asList(
                "m4-esx1.cdnivt.cisco.com",
                "m4-esx2.cdnivt.cisco.com",
                "m4-esx3.cdnivt.cisco.com",
                "m4-esx4.cdnivt.cisco.com"
        );
        final List<Long> expectedRetained = oidsFor(expectedRetainedDisplayNames.stream(), hypervisorEntities);

        // This DataCenter from the hypervisor should have been removed as a provider to the PMs that
        // had a Chassis provider from the fabric probe
        final List<Long> removedProvider = oidsFor(Arrays.asList("cc1HX01").stream(),
                hypervisorEntities);

        // These are the providers added to esx4
        final List<String> expectedCopiedProviderDisplayNames = Arrays.asList(
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-2/slot-1",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-2/slot-2",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-2"
        );

        // These are the providers added for esx1, esx2, and esx3
        final List<String> expectedCopiedProviderDisplayNames2 = Arrays.asList(
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3/slot-1",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3/slot-2",
                "cloud1ucs.cdnivt.cisco.com:sys/chassis-3"
        );

        final List<Long> expectedCopiedProviders =
                oidsFor(expectedCopiedProviderDisplayNames.stream(), ucsEntities);

        final List<Long> expectedCopiedProviders2 =
                oidsFor(expectedCopiedProviderDisplayNames2.stream(), ucsEntities);

        expectedRemoved.forEach(oid -> assertNull(topology.get(oid)));
        expectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));

        // After stitching each of the hypervisor (retained) storages should all be connected to
        // a storage controller even though the hypervisor did not discover a storage controller.
        final List<StitchingEntity> hypervisorPMs = stitchingContext.getStitchingGraph().entities()
                .filter(entity -> expectedRetainedDisplayNames.contains(entity.getDisplayName()))
                .collect(Collectors.toList());
        assertEquals(4, hypervisorPMs.size());

        // assert that none of the retained PMs has the DataCenter as a provider (it should have
        // been replaced by a Chassis)
        // Also assert that each provider copied over from UCS is there in the retained PMs
        hypervisorPMs.forEach(pm -> {
            final Set<StitchingEntity> providerSubtree = new HashSet<>();
            StitchingTestUtils.visitNeighbors(pm, providerSubtree,
                    Collections.emptySet(), StitchingEntity::getProviders);

            assertFalse(providerSubtree.stream()
                    .anyMatch(provider -> provider.getOid() == removedProvider.get(0)));
            List<Long> copiedProviderOids = expectedCopiedProviders2;
            // if pm is esx4 change the list of copied provider oids
            if (pm.getDisplayName().equals(expectedRetainedDisplayNames.get(3))) {
                copiedProviderOids = expectedCopiedProviders;
            }
            copiedProviderOids.forEach(copiedProv ->
                    assertTrue(providerSubtree.stream()
                            .anyMatch(provider -> provider.getOid() == copiedProv)));

        });

        final IStitchingJournal<TopologyEntity> postStitchingJournal = journal.childJournal(
                new TopologyEntitySemanticDiffer(journal.getJournalOptions().getVerbosity()));
        stitchingManager.postStitch(new GraphWithSettings(TopologyGraph.newGraph(topology),
                Collections.emptyMap(), Collections.emptyMap()), postStitchingJournal);
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId)
            throws IdentityUninitializedException, IdentityMetadataMissingException, IdentityProviderException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
                Mockito.eq(probeId),
                Mockito.eq(new ArrayList<>(entities.values()))))
                .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId,
                new ArrayList<>(entities.values()));
    }
 }
