package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
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
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.market.runner.cost.MarketPriceTable;
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

    private TopologyEntityDTO.Builder createEntity(long oid, EntityType entityType) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
            .setEnvironmentType(EnvironmentType.ON_PREM);
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

    private Map<Long, TopologyEntityDTO> createTestTopology() {
        final long vmOid = 1l;
        final long storOid = 2l;
        final long wastedFileVolumeOid = 3l;
        final long connectedVolumeOid = 4l;
        final TopologyEntityDTO.Builder vm = createEntity(vmOid, EntityType.VIRTUAL_MACHINE);
        final TopologyEntityDTO.Builder storage = createEntity(storOid, EntityType.STORAGE);
        final TopologyEntityDTO.Builder wastedFileVolume = createEntity(wastedFileVolumeOid,
            EntityType.VIRTUAL_VOLUME);
        final TopologyEntityDTO.Builder connectedVolume = createEntity(connectedVolumeOid,
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

    /**
     * Test the {@link Analysis} constructor.
     */
    @Test
    public void testWastedFilesAnalysis() {
        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
        when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        when(cloudCostCalculatorFactory.newCalculator(topologyInfo)).thenReturn(cloudCostCalculator);
        final MarketPriceTable priceTable = mock(MarketPriceTable.class);

        final WastedFilesAnalysis analysis = new WastedFilesAnalysis(topologyInfo,
            createTestTopology(), mockClock, cloudCostCalculator, priceTable);

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
}
