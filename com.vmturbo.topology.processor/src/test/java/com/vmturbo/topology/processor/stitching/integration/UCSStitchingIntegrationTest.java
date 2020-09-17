package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.fabric.FabricChassisStitchingOperation;
import com.vmturbo.stitching.fabric.FabricPMStitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
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
 * Attempt to simulate UCS stitching operation.
 */
public class UCSStitchingIntegrationTest extends StitchingIntegrationTest {

    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long ucsProbeId = 2468L;
    private final long ucsTargetId = 2121L;

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
                .addMatchingData(fabricMatchingData)
                .addExternalEntityMatchingProperty(fabricExternalMatchingData)
                .build();
        final MergedEntityMetadata fabricMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(fabricMatchingMetadata)
                        .addAllCommoditiesBought(pmBoughtCommodityData)
                        .addAllCommoditiesSoldMetadata(pmSoldCommodityData)
                        .build();
        return new StringsToStringsDataDrivenStitchingOperation(
                new StringsToStringsStitchingMatchingMetaData(EntityType.PHYSICAL_MACHINE,
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
        final Map<String, EntityDTO> hypervisorHostsBeforeStitch = hypervisorEntities.values().stream()
            .filter(entityDTO -> entityDTO.getEntityType() == EntityType.PHYSICAL_MACHINE)
            .collect(Collectors.toMap(EntityDTO::getId, Function.identity()));

        addEntities(ucsEntities, ucsTargetId);
        addEntities(hypervisorEntities, vcTargetId);

        setOperationsForProbe(ucsProbeId, fabricStitchingOperationsToTest);
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
                cpuCapacityStore);
        final Target ucsTarget = mock(Target.class);
        when(ucsTarget.getId()).thenReturn(ucsTargetId);
        final Target ucsVcenterTarget = mock(Target.class);
        when(ucsVcenterTarget.getId()).thenReturn(vcTargetId);
        when(targetStore.getProbeTargets(ucsProbeId))
                .thenReturn(Collections.singletonList(ucsTarget));
        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(ucsVcenterTarget));
        when(probeStore.getProbe(ucsProbeId)).thenReturn(Optional.empty());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.FABRIC))
                .thenReturn(Collections.singletonList(ucsProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));

        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
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

            // check that the stitched host will only sell one CPU/MEM/IO_THROUGHPUT/NET_THROUGHPUT
            // commodity with values from VC probe
            Map<CommodityType, List<Builder>> hostSoldCommoditiesAfterStitching =
                pm.getCommoditiesSold().collect(Collectors.groupingBy(Builder::getCommodityType));
            Map<CommodityType, List<CommodityDTO>> vcHostSoldCommoditiesBeforeStitching =
                hypervisorHostsBeforeStitch.get(pm.getLocalId()).getCommoditiesSoldList().stream()
                    .collect(Collectors.groupingBy(CommodityDTO::getCommodityType));
            pmSoldCommodityData.forEach(metadata -> {
                CommodityType commodityType = metadata.getCommodityType();
                assertEquals(1, hostSoldCommoditiesAfterStitching.get(commodityType).size());
                assertEquals(vcHostSoldCommoditiesBeforeStitching.get(commodityType).get(0).getUsed(),
                    hostSoldCommoditiesAfterStitching.get(commodityType).get(0).getUsed(), 0.0);
                assertEquals(vcHostSoldCommoditiesBeforeStitching.get(commodityType).get(0).getCapacity(),
                    hostSoldCommoditiesAfterStitching.get(commodityType).get(0).getCapacity(), 0.0);
            });
        });

        final IStitchingJournal<TopologyEntity> postStitchingJournal = journal.childJournal(
                new TopologyEntitySemanticDiffer(journal.getJournalOptions().getVerbosity()));
        stitchingManager.postStitch(new GraphWithSettings(TopologyEntityTopologyGraphCreator.newGraph(topology),
                Collections.emptyMap(), Collections.emptyMap()), postStitchingJournal,
                Collections.emptySet());
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

    private static Collection<CommoditySoldMetadata> pmSoldCommodityData = ImmutableList.of(
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.CPU).setIgnoreIfPresent(true).build(),
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.MEM).setIgnoreIfPresent(true).build(),
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.IO_THROUGHPUT).setIgnoreIfPresent(true).build(),
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.NET_THROUGHPUT).setIgnoreIfPresent(true).build()
    );
 }
