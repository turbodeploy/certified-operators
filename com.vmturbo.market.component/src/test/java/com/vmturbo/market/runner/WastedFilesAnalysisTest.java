package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

public class WastedFilesAnalysisTest {
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

    private final Clock mockClock = mock(Clock.class);

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0L);
        when(mockClock.instant())
            .thenReturn(START_INSTANT)
            .thenReturn(END_INSTANT);
    }

    private TopologyEntityDTO.Builder createOnPremEntity(long oid, EntityType entityType) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
            .setEnvironmentType(EnvironmentType.ON_PREM);
    }

    private TopologyEntityDTO.Builder createCloudEntity(long oid, EntityType entityType) {
        if (entityType == EntityType.VIRTUAL_VOLUME) {
            return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setDisplayName("Vol-"+oid)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(VirtualVolumeInfo.newBuilder()
                    .setStorageAccessCapacity(10f)
                    .setStorageAmountCapacity(20f).build()).build());
        } else {
            return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD);
        }
    }

    private void connectEntities(TopologyEntityDTO.Builder from, TopologyEntityDTO.Builder to) {
        from.addConnectedEntityList(ConnectedEntity.newBuilder().setConnectedEntityId(to.getOid())
            .setConnectedEntityType(to.getEntityType()));
    }

    private void addFilesToOnpremVolume(TopologyEntityDTO.Builder volume, String[] filePath,
                                       long [] sizeKb) {
        VirtualVolumeInfo.Builder volumeInfo = VirtualVolumeInfo.newBuilder();
        for (int i = 0; i < filePath.length; i++) {
            volumeInfo.addFiles(VirtualVolumeFileDescriptor.newBuilder().setPath(filePath[i])
                .setSizeKb(sizeKb[i]));
        }
        volume.setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(volumeInfo));
    }

    private Map<Long, TopologyEntityDTO> createTestOnPremTopology() {
        final long vmOid = 1l;
        final long storOid = 2l;
        final long wastedFileVolumeOid = 3l;
        final long connectedVolumeOid = 4l;
        final TopologyEntityDTO.Builder vm = createOnPremEntity(vmOid, EntityType.VIRTUAL_MACHINE);
        final TopologyEntityDTO.Builder storage = createOnPremEntity(storOid, EntityType.STORAGE);
        final TopologyEntityDTO.Builder wastedFileVolume = createOnPremEntity(wastedFileVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder connectedVolume = createOnPremEntity(connectedVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final String [] filePathsWasted = {"/foo/bar/file1", "/etc/turbo/file2.iso", "file3"};
        final String [] filePathsUsed = {"/foo/bar/used1", "/etc/turbo/used2.iso", "used3"};
        final long [] wastedSizesKb = {900, 1100, 2400000};
        final long [] usedSizesKb = {800, 1200, 2500000};
        connectEntities(vm, connectedVolume);
        connectEntities(connectedVolume, storage);
        connectEntities(wastedFileVolume, storage);
        addFilesToOnpremVolume(wastedFileVolume, filePathsWasted, wastedSizesKb);
        addFilesToOnpremVolume(connectedVolume, filePathsUsed, usedSizesKb);
        return ImmutableMap.of(
            vm.getOid(), vm.build(),
            storage.getOid(), storage.build(),
            wastedFileVolume.getOid(), wastedFileVolume.build(),
            connectedVolume.getOid(), connectedVolume.build());
    }

    private Map<Long, TopologyEntityDTO> createTestCloudTopology() {
        final long vmOid = 1L;
        final long wastedFileVolume1Oid = 2L;
        final long wastedFileVolume2Oid = 3L;
        final long connectedVolumeOid = 4L;
        final long storageTierOid = 5L;
        final TopologyEntityDTO.Builder vm = createCloudEntity(vmOid, EntityType.VIRTUAL_MACHINE);
        final TopologyEntityDTO.Builder wastedFileVolume1 = createCloudEntity(wastedFileVolume1Oid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder wastedFileVolume2 = createCloudEntity(wastedFileVolume2Oid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder connectedVolume = createCloudEntity(connectedVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder storageTier = createCloudEntity(storageTierOid, EntityType.STORAGE_TIER);

        connectEntities(vm, connectedVolume);
        connectEntities(connectedVolume, storageTier);
        connectEntities(wastedFileVolume1, storageTier);
        connectEntities(wastedFileVolume2, storageTier);

        return ImmutableMap.of(
            vm.getOid(), vm.build(),
            storageTier.getOid(), storageTier.build(),
            wastedFileVolume1.getOid(), wastedFileVolume1.build(),
            wastedFileVolume2.getOid(), wastedFileVolume2.build(),
            connectedVolume.getOid(), connectedVolume.build());
    }

    /**
     * Test the {@link Analysis} constructor.
     */
    @Test
    public void testOnPremWastedFilesAnalysis() {
        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo)).thenReturn(cloudCostCalculator);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);

        final WastedFilesAnalysis analysis = new WastedFilesAnalysis(topologyInfo,
            createTestOnPremTopology(), mockClock, cloudCostCalculator, originalCloudTopology);

        assertTrue(analysis.execute());
        assertFalse(analysis.execute());
        assertEquals(AnalysisState.SUCCEEDED, analysis.getState());

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
            assertEquals(2l, target.getId());
        });
    }

    @Test
    public void testCloudWastedFilesAnalysis() {
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);

        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo)).thenReturn(cloudCostCalculator);
        final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);

        Map<Long, TopologyEntityDTO> cloudTopology = createTestCloudTopology();
        final WastedFilesAnalysis analysis = new WastedFilesAnalysis(topologyInfo,
            cloudTopology, mockClock, cloudCostCalculator, originalCloudTopology);


        cloudTopology.values().stream().filter(dto -> dto.getEntityType() == EntityType.VIRTUAL_VOLUME.getNumber())
            .forEach(dto -> {
                CostJournal<TopologyEntityDTO> costJournal = mock(CostJournal.class);
                when(costJournal.getTotalHourlyCost()).thenReturn(10d * dto.getOid());
                when(cloudCostCalculator.calculateCostForEntity(any(), eq(dto))).thenReturn(Optional.of(costJournal));
            });

        assertTrue(analysis.execute());
        assertFalse(analysis.execute());
        assertEquals(AnalysisState.SUCCEEDED, analysis.getState());

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
            ImmutableSet.of(5L, 5L),
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
            .build();
        analysis.getActions().forEach(action ->
            assertEquals("Ensure action has the right savings",
                costMap.get(action.getInfo().getDelete().getTarget().getId()),
                Double.valueOf(action.getSavingsPerHour().getAmount()))
        );

        // make sure storage tier is the target of each action
        analysis.getActions().forEach(action -> {
            assertEquals("Each file path are empty", "", action.getInfo().getDelete().getFilePath());

            ActionEntity target = action.getInfo().getDelete().getTarget();
            assertEquals(EntityType.VIRTUAL_VOLUME_VALUE, target.getType());
            assertEquals(EnvironmentType.CLOUD, target.getEnvironmentType());

            ActionEntity source = action.getInfo().getDelete().getSource();
            assertEquals(EntityType.STORAGE_TIER_VALUE, source.getType());
            assertEquals(5L, source.getId());
        });
    }
}
