package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.Matchers;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.stitching.storage.StorageStitchingOperation;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.StitchingTestUtils;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Attempt to simulate a basic storage stitching operation.
 */
public class StorageStitchingIntegrationTest extends StitchingIntegrationTest {

    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long netAppProbeId = 1234L;
    private final long netAppTargetId = 1235L;

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
                .addMatchingData(storageMatchingData)
                .addExternalEntityMatchingProperty(storageMatchingData)
                .build();
        final MergedEntityMetadata storageMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(storageMatchingMetadata)
                        .addAllCommoditiesBought(storageBoughtCommodityData)
                        .build();
        return new StringsToStringsDataDrivenStitchingOperation(
                new StringsToStringsStitchingMatchingMetaData(EntityType.STORAGE,
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

        setOperationsForProbe(netAppProbeId,
                Collections.singletonList(storageStitchingOperationToTest));
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
                cpuCapacityStore);
        final Target netAppTarget = mock(Target.class);
        when(netAppTarget.getId()).thenReturn(netAppTargetId);

        // Default targetStore to return empty
        when(targetStore.getTarget(Matchers.anyLong()))
            .thenReturn(Optional.empty());

        when(targetStore.getProbeTargets(netAppProbeId))
                .thenReturn(Collections.singletonList(netAppTarget));
        when(targetStore.getTarget(netAppTargetId))
            .thenReturn(Optional.of(netAppTarget));
        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);

        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));
        when(targetStore.getTarget(vcTargetId))
            .thenReturn(Optional.of(vcTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.STORAGE))
                .thenReturn(Collections.singletonList(netAppProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));

        addEntities(storageEntities, netAppTargetId);
        addEntities(hypervisorEntities, vcTargetId);

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
        stitchingManager.postStitch(new GraphWithSettings(TopologyEntityTopologyGraphCreator.newGraph(topology),
                Collections.emptyMap(), Collections.emptyMap()), postStitchingJournal,
                Collections.emptySet());
    }
 }
