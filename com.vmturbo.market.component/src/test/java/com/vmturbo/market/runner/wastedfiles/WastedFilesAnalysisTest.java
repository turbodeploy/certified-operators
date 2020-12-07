package com.vmturbo.market.runner.wastedfiles;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.market.runner.wastedfiles.WastedFilesAnalysisEngine;
import com.vmturbo.market.runner.wastedfiles.WastedFilesResults;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

public class WastedFilesAnalysisTest {
    private static final int STORAGE_AMOUNT_CAPACITY = 20;
    private long topologyContextId = 1111;
    private long topologyId = 2222;
    private TopologyType topologyType = TopologyType.REALTIME;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(topologyContextId)
        .setTopologyId(topologyId)
        .setTopologyType(topologyType)
        .build();


    private static final Instant START_INSTANT = Instant.EPOCH.plus(90, ChronoUnit.MINUTES);
    private static final Instant END_INSTANT = Instant.EPOCH.plus(100, ChronoUnit.MINUTES);

    private WastedFilesAnalysisEngine wastedFilesAnalysisEngine = new WastedFilesAnalysisEngine();

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
    }

    private static TopologyEntityDTO.Builder createOnPremEntity(long oid, EntityType entityType) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setDeletable(true)
                .build());
    }

    private static TopologyEntityDTO.Builder createCloudEntity(
            final long oid,
            @Nonnull final EntityType entityType,
            @Nullable final AttachmentState attachmentState) {
        return createCloudEntity(oid, entityType, attachmentState, true);
    }

    private static TopologyEntityDTO.Builder createCloudEntity(
            final long oid,
            @Nonnull final EntityType entityType,
            @Nullable final AttachmentState attachmentState,
            final boolean deletable) {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD);
        if (entityType == EntityType.VIRTUAL_VOLUME) {
            builder.setDisplayName("Vol-" + oid)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                    .setAttachmentState(attachmentState)))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
                    .setCapacity(STORAGE_AMOUNT_CAPACITY))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber()))
                    .setCapacity(10))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setDeletable(deletable));
        }
        return builder;
    }

    private static void connectEntities(TopologyEntityDTO.Builder from, TopologyEntityDTO.Builder to) {
        from.addConnectedEntityList(ConnectedEntity.newBuilder().setConnectedEntityId(to.getOid())
            .setConnectedEntityType(to.getEntityType()));
    }

    private static void createCommodityLink(
            @Nonnull final TopologyEntityDTO.Builder consumer,
            @Nonnull final TopologyEntityDTO.Builder producer) {
        consumer.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(producer.getOid())
                .setProviderEntityType(producer.getEntityType()));
    }

    private static void addFilesToOnpremVolume(TopologyEntityDTO.Builder volume, String[] filePath,
                                       long [] sizeKb) {
        VirtualVolumeInfo.Builder volumeInfo = VirtualVolumeInfo.newBuilder();
        for (int i = 0; i < filePath.length; i++) {
            volumeInfo.addFiles(VirtualVolumeFileDescriptor.newBuilder().setPath(filePath[i])
                .setSizeKb(sizeKb[i]));
        }
        volume.setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(volumeInfo));
    }

    private static Map<Long, TopologyEntityDTO> createTestOnPremTopology() {
        final long vmOid = 1l;
        final long storOid = 2l;
        final long wastedFileVolumeOid = 3l;
        final long connectedVolumeOid = 4l;
        final long stor2Oid = 5L;
        final long wastedFileVolume2Oid = 6L;
        final TopologyEntityDTO.Builder vm = createOnPremEntity(vmOid, EntityType.VIRTUAL_MACHINE);
        final TopologyEntityDTO.Builder storage = createOnPremEntity(storOid, EntityType.STORAGE);
        final TopologyEntityDTO.Builder wastedFileVolume = createOnPremEntity(wastedFileVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder connectedVolume = createOnPremEntity(connectedVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder storage2 = createOnPremEntity(stor2Oid, EntityType.STORAGE);
        final TopologyEntityDTO.Builder wastedFileVolume2 = createOnPremEntity(wastedFileVolume2Oid,
            EntityType.VIRTUAL_VOLUME);
        final String [] filePathsWasted = {"/foo/bar/file1", "/etc/turbo/file2.iso", "file3"};
        final String[] filePathsWasted2 = {"skippedWastedFile1", "skippedWastedFile2",
            "skippedWastedFile3"};
        final String [] filePathsUsed = {"/foo/bar/used1", "/etc/turbo/used2.iso", "used3"};
        final long [] wastedSizesKb = {900, 1100, 2400000};
        final long [] usedSizesKb = {800, 1200, 2500000};
        connectEntities(vm, connectedVolume);
        connectEntities(connectedVolume, storage);
        connectEntities(wastedFileVolume, storage);
        connectEntities(wastedFileVolume2, storage2);
        addFilesToOnpremVolume(wastedFileVolume, filePathsWasted, wastedSizesKb);
        addFilesToOnpremVolume(connectedVolume, filePathsUsed, usedSizesKb);
        return new ImmutableMap.Builder<Long, TopologyEntityDTO>()
            .put(vm.getOid(), vm.build())
            .put(storage.getOid(), storage.build())
            .put(storage2.getOid(), storage2.build())
            .put(wastedFileVolume.getOid(), wastedFileVolume.build())
            .put(wastedFileVolume2.getOid(), wastedFileVolume2.build())
            .put(connectedVolume.getOid(), connectedVolume.build())
            .build();
    }

    private static Map<Long, TopologyEntityDTO> createTestCloudTopology(final boolean includeNondeletable) {
        final long vmOid = 1L;
        final long wastedFileVolume1Oid = 2L;
        final long wastedFileVolume2Oid = 3L;
        final long connectedVolumeOid = 4L;
        final long unConnectedVolumeInUseOid = 5L;
        final long storageTierOid = 6L;
        final TopologyEntityDTO.Builder vm = createCloudEntity(vmOid, EntityType.VIRTUAL_MACHINE, null);
        final TopologyEntityDTO.Builder wastedFileVolume1 = createCloudEntity(wastedFileVolume1Oid,
            EntityType.VIRTUAL_VOLUME, AttachmentState.UNATTACHED);
        final TopologyEntityDTO.Builder wastedFileVolume2 = createCloudEntity(wastedFileVolume2Oid,
            EntityType.VIRTUAL_VOLUME, AttachmentState.UNATTACHED, !includeNondeletable);
        final TopologyEntityDTO.Builder connectedVolume = createCloudEntity(connectedVolumeOid,
            EntityType.VIRTUAL_VOLUME, AttachmentState.ATTACHED);
        final TopologyEntityDTO.Builder unConnectedVolumeInUse = createCloudEntity(unConnectedVolumeInUseOid,
                EntityType.VIRTUAL_VOLUME, AttachmentState.ATTACHED);
        final TopologyEntityDTO.Builder storageTier = createCloudEntity(storageTierOid,
                EntityType.STORAGE_TIER, null);

        createCommodityLink(vm, connectedVolume);
        createCommodityLink(connectedVolume, storageTier);
        createCommodityLink(wastedFileVolume1, storageTier);
        createCommodityLink(wastedFileVolume2, storageTier);
        createCommodityLink(unConnectedVolumeInUse, storageTier);

        return ImmutableMap.<Long, TopologyEntityDTO>builder()
                .put(vm.getOid(), vm.build())
                .put(storageTier.getOid(), storageTier.build())
                .put(wastedFileVolume1.getOid(), wastedFileVolume1.build())
                .put(wastedFileVolume2.getOid(), wastedFileVolume2.build())
                .put(connectedVolume.getOid(), connectedVolume.build())
                .put(unConnectedVolumeInUse.getOid(), unConnectedVolumeInUse.build())
                .build();
    }

    /**
     * Test wasted files analysis for On Prem.
     */
    @Test
    public void testOnPremWastedFilesAnalysis() {
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo, originalCloudTopology)).thenReturn(cloudCostCalculator);

        final WastedFilesResults analysis = wastedFilesAnalysisEngine.analyzeWastedFiles(topologyInfo,
            createTestOnPremTopology(), cloudCostCalculator, originalCloudTopology);

        // expect 2 actions since one file is too small to get an action
        assertEquals(2, analysis.getActions().size());
        // make sure actions have the correct files in them
        assertEquals(ImmutableSet.of("/etc/turbo/file2.iso", "file3"),
            analysis.getActions().stream()
                .map(Action::getInfo)
                .map(ActionInfo::getDelete)
                .map(Delete::getFilePath)
                .collect(Collectors.toSet()));
        // make sure action explanations have the right values
        assertEquals(ImmutableSet.of(1100l, 2400000l),
            analysis.getActions().stream()
                .map(Action::getExplanation)
                .map(Explanation::getDelete)
                .map(DeleteExplanation::getSizeKb)
                .collect(Collectors.toSet()));
        // make sure storage is the target of each action
        analysis.getActions().forEach(action -> {
            ActionEntity target = action.getInfo().getDelete().getTarget();
            assertEquals(EntityType.STORAGE_VALUE, target.getType());
            assertEquals(EnvironmentType.ON_PREM, target.getEnvironmentType());
            assertEquals(2L, target.getId());
        });

        // Verify total storage amount released for this oid
        assertTrue(analysis.getMbReleasedOnProvider(2L).isPresent());
        assertEquals((2400000L + 1100L) / Units.NUM_OF_KB_IN_MB,
            analysis.getMbReleasedOnProvider(2L).getAsLong());
    }

    /**
     * Test wasted files analysis for Cloud.
     */
    @Test
    public void testCloudWastedFilesAnalysis() {
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);

        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo, originalCloudTopology)).thenReturn(cloudCostCalculator);

        Map<Long, TopologyEntityDTO> cloudTopology = createTestCloudTopology(false);

        cloudTopology.values().stream().filter(dto -> dto.getEntityType() == EntityType.VIRTUAL_VOLUME.getNumber())
                .forEach(dto -> {
                    CostJournal<TopologyEntityDTO> costJournal = mock(CostJournal.class);
                    when(costJournal.getTotalHourlyCost()).thenReturn(trax(10d * dto.getOid()));
                    when(cloudCostCalculator.calculateCostForEntity(any(), eq(dto))).thenReturn(Optional.of(costJournal));
                });

        final WastedFilesResults analysis = wastedFilesAnalysisEngine.analyzeWastedFiles(topologyInfo,
            cloudTopology, cloudCostCalculator, originalCloudTopology);


        assertEquals("There should be two actions for cloud wasted storage", 2, analysis.getActions().size());

        assertEquals("Ensure Action has the virtual volume oid as the delete target",
            ImmutableSet.of(2L, 3L),
            analysis.getActions().stream()
                .map(Action::getInfo)
                .map(ActionInfo::getDelete)
                .map(Delete::getTarget)
                .map(ActionEntity::getId)
                .collect(Collectors.toSet()));

        assertEquals("Ensure Action has the storage tier oid as the source target",
            ImmutableSet.of(6L, 6L),
            analysis.getActions().stream()
                .map(Action::getInfo)
                .map(ActionInfo::getDelete)
                .map(Delete::getSource)
                .map(ActionEntity::getId)
                .collect(Collectors.toSet()));

        Map<Long, Double> costMap = ImmutableMap.<Long, Double>builder()
            .put(2L, 20d)
            .put(3L, 30d)
            .put(4L, 40d)
            .put(5L, 50d)
            .build();
        analysis.getActions().forEach(action ->
            assertEquals("Ensure action has the right savings",
                costMap.get(action.getInfo().getDelete().getTarget().getId()),
                Double.valueOf(action.getSavingsPerHour().getAmount()))
        );

        analysis.getActions().forEach(action -> {
            assertTrue(action.hasExplanation());
            assertTrue(action.getExplanation().hasDelete());
            assertTrue(action.getExplanation().getDelete().hasSizeKb());
            assertEquals(STORAGE_AMOUNT_CAPACITY, action.getExplanation().getDelete().getSizeKb(), .001);
        });

        // make sure storage tier is the target of each action
        analysis.getActions().forEach(action -> {
            assertEquals("Each file path are empty", "", action.getInfo().getDelete().getFilePath());
            assertTrue("Each action should be executable", action.getExecutable());

            ActionEntity target = action.getInfo().getDelete().getTarget();
            assertEquals(EntityType.VIRTUAL_VOLUME_VALUE, target.getType());
            assertEquals(EnvironmentType.CLOUD, target.getEnvironmentType());

            ActionEntity source = action.getInfo().getDelete().getSource();
            assertEquals(EntityType.STORAGE_TIER_VALUE, source.getType());
            assertEquals(6L, source.getId());
        });
    }

    /**
     * Test Cloud WastedFileAnalysis when there are unattached entity set to be non-deletable.
     */
    @Test
    public void testCloudWastedFilesAnalysisWithEntitySetToNonDeletable() {
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);

        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo, originalCloudTopology)).thenReturn(cloudCostCalculator);

        Map<Long, TopologyEntityDTO> cloudTopology = createTestCloudTopology(true);

        cloudTopology.values().stream().filter(dto -> dto.getEntityType() == EntityType.VIRTUAL_VOLUME.getNumber())
                .forEach(dto -> {
                    CostJournal<TopologyEntityDTO> costJournal = mock(CostJournal.class);
                    when(costJournal.getTotalHourlyCost()).thenReturn(trax(10d * dto.getOid()));
                    when(cloudCostCalculator.calculateCostForEntity(any(), eq(dto))).thenReturn(Optional.of(costJournal));
                });

        final WastedFilesResults analysis = wastedFilesAnalysisEngine.analyzeWastedFiles(topologyInfo,
            cloudTopology, cloudCostCalculator, originalCloudTopology);


        assertEquals("There should be one actions for cloud wasted storage", 1, analysis.getActions().size());

        assertEquals("Ensure Action has the virtual volume oid as the delete target",
            ImmutableSet.of(2L),
            analysis.getActions().stream()
                .map(Action::getInfo)
                .map(ActionInfo::getDelete)
                .map(Delete::getTarget)
                .map(ActionEntity::getId)
                .collect(Collectors.toSet()));

        assertEquals("Ensure Action has the storage tier oid as the source target",
            ImmutableSet.of(6L),
            analysis.getActions().stream()
                .map(Action::getInfo)
                .map(ActionInfo::getDelete)
                .map(Delete::getSource)
                .map(ActionEntity::getId)
                .collect(Collectors.toSet()));

        Map<Long, Double> costMap = ImmutableMap.<Long, Double>builder()
            .put(2L, 20d)
            .put(3L, 30d)
            .put(4L, 40d)
            .put(5L, 50d)
            .build();
        analysis.getActions().forEach(action ->
            assertEquals("Ensure action has the right savings",
                costMap.get(action.getInfo().getDelete().getTarget().getId()),
                Double.valueOf(action.getSavingsPerHour().getAmount()))
        );

        // make sure storage tier is the target of each action
        analysis.getActions().forEach(action -> {
            assertEquals("Each file path are empty", "", action.getInfo().getDelete().getFilePath());
            assertTrue("Each action should be executable", action.getExecutable());

            ActionEntity target = action.getInfo().getDelete().getTarget();
            assertEquals(EntityType.VIRTUAL_VOLUME_VALUE, target.getType());
            assertEquals(EnvironmentType.CLOUD, target.getEnvironmentType());

            ActionEntity source = action.getInfo().getDelete().getSource();
            assertEquals(EntityType.STORAGE_TIER_VALUE, source.getType());
            assertEquals(6L, source.getId());
        });
    }
}
