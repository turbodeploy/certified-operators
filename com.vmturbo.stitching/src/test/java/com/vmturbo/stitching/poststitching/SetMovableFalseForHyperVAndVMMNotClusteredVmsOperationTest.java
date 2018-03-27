package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class SetMovableFalseForHyperVAndVMMNotClusteredVmsOperationTest {

    private final SetMovableFalseForHyperVAndVMMNotClusteredVmsOperation operation =
            new SetMovableFalseForHyperVAndVMMNotClusteredVmsOperation();

    private final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private UnitTestResultBuilder resultBuilder;

    // pm commodities sold
    private final CommoditySoldDTO pmCpuCommoditySold = makeCommoditySold(CommodityType.CPU);
    private final CommoditySoldDTO pmMemCommoditySold = makeCommoditySold(CommodityType.MEM);
    private final CommoditySoldDTO pmClusterCommoditySold = makeCommoditySold(CommodityType.CLUSTER);
    private final List<CommoditySoldDTO> pmCommoditySoldList = ImmutableList.of(
            pmCpuCommoditySold,
            pmMemCommoditySold,
            pmClusterCommoditySold);

    // host provider
    private final TopologyEntity.Builder hostProvider = makeTopologyEntityBuilder(
            EntityType.PHYSICAL_MACHINE_VALUE,
            pmCommoditySoldList,
            Collections.emptyList());

    // vm commodities sold
    private final CommoditySoldDTO vmVcpuCommoditySold = makeCommoditySold(CommodityType.VCPU);
    private final List<CommoditySoldDTO> vmCommoditySoldList = ImmutableList.of(vmVcpuCommoditySold);

    // vm commodities bought
    private final CommodityBoughtDTO cpuCommBought = makeCommodityBought(CommodityType.CPU);
    private final CommodityBoughtDTO memCommBought = makeCommodityBought(CommodityType.MEM);
    private final CommodityBoughtDTO clusterCommBought = makeCommodityBought(CommodityType.CLUSTER);

    private final List<CommodityBoughtDTO> vmWithClusterCommodityBoughtList = ImmutableList.of(
            cpuCommBought,
            memCommBought,
            clusterCommBought);
    private final CommoditiesBoughtFromProvider vmWithClusterCommoBoughtFromProvider =
            CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(vmWithClusterCommodityBoughtList)
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .build();

    private final List<CommodityBoughtDTO> vmWithoutClusterCommodityBoughtList = ImmutableList.of(
            cpuCommBought,
            memCommBought);
    private final CommoditiesBoughtFromProvider vmWithoutClusterCommoBoughtFromProvider =
            CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(vmWithoutClusterCommodityBoughtList)
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .build();

    // vm with cluster comm
    private final TopologyEntity vmWithClusterComm = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            vmCommoditySoldList,
            ImmutableSet.of(vmWithClusterCommoBoughtFromProvider),
            ImmutableList.of(hostProvider));

    // vm without cluster comm
    private final TopologyEntity vmWithoutClusterComm = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            vmCommoditySoldList,
            ImmutableSet.of(vmWithoutClusterCommoBoughtFromProvider),
            ImmutableList.of(hostProvider));

    // save if the entity is movable right after creation
    final boolean isMovableBefore = vmWithClusterComm.getTopologyEntityDtoBuilder()
            .getCommoditiesBoughtFromProvidersList().stream()
            .filter(commBoughtFromProv -> EntityType.PHYSICAL_MACHINE_VALUE == commBoughtFromProv.getProviderEntityType())
            // there should be only 1
            .allMatch(commBoughtFromProv -> commBoughtFromProv.getMovable() == true);


    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void testNoEntities() {

        operation.performOperation(Stream.empty(), settingsCollection, resultBuilder);

        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityWithClusterCommodity() {

        operation.performOperation(Stream.of(vmWithClusterComm), settingsCollection, resultBuilder);

        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityWithoutClusterCommodity() {

        operation.performOperation(Stream.of(vmWithoutClusterComm), settingsCollection, resultBuilder);

        assertEquals(1, resultBuilder.getChanges().size());

        // apply the changes
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        // check that the entity without the cluster comm is not movable
        final boolean isMovable = vmWithoutClusterComm.getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersList().stream()
                .filter(commBoughtFromProv -> EntityType.PHYSICAL_MACHINE_VALUE == commBoughtFromProv.getProviderEntityType())
                // there should be only 1
                .allMatch(commBoughtFromProv -> commBoughtFromProv.getMovable() == true);

        assertFalse(isMovable);

    }

    @Test
    public void testMixedEntities() {

        operation.performOperation(Stream.of(vmWithoutClusterComm, vmWithClusterComm), settingsCollection, resultBuilder);

        assertEquals(1, resultBuilder.getChanges().size());

        // apply the changes
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        // check that the entity without the cluster comm is not movable
        final boolean isVmWithoutClusterCommMovable = vmWithoutClusterComm.getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersList().stream()
                .filter(commBoughtFromProv -> EntityType.PHYSICAL_MACHINE_VALUE == commBoughtFromProv.getProviderEntityType())
                // there should be only 1
                .allMatch(commBoughtFromProv -> commBoughtFromProv.getMovable() == true);

        assertFalse(isVmWithoutClusterCommMovable);

        // check that the entity with cluster comm remains movable as before
        final boolean isMovableAfter = vmWithClusterComm.getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersList().stream()
                .filter(commBoughtFromProv -> EntityType.PHYSICAL_MACHINE_VALUE == commBoughtFromProv.getProviderEntityType())
                // there should be only 1
                .allMatch(commBoughtFromProv -> commBoughtFromProv.getMovable() == true);

        assertEquals(isMovableBefore, isMovableAfter);

    }

}