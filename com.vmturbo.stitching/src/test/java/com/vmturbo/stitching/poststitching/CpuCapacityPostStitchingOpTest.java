package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class CpuCapacityPostStitchingOpTest {

    private final CpuCapacityPostStitchingOperation operation = new CpuCapacityPostStitchingOperation();
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private final static String NUM_CPU_CORES = "common_dto.EntityDTO.PhysicalMachineData.numCpuCores";
    private final static String CPU_CORE_MHZ = "common_dto.EntityDTO.PhysicalMachineData.cpuCoreMhz";

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void testNoEntities() {

        final TopologicalChangelog result =
            operation.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());

    }

    @Test
    public void testNoCommodities() {

        final TopologyEntity entity = makeTopologyEntity(Collections.emptyList(), Collections.emptyMap());
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoCpuCommodity() {
        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.BALLOONING));
        final TopologyEntity entity = makeTopologyEntity(commodities, Collections.emptyMap());
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testCpuCommodityHasCapacity() {

        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU, 255));
        final TopologyEntity entity = makeTopologyEntity(commodities, Collections.emptyMap());
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksNumCores() {
        final Map<String, String> propsMap = ImmutableMap.of(CPU_CORE_MHZ, "25", "irrelevant", "123");

        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, propsMap);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksCpuMhz() {
        final Map<String, String> propsMap = ImmutableMap.of(NUM_CPU_CORES, "25", "irrelevant", "123");

        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, propsMap);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksCoresAndMhz() {
        final Map<String, String> propsMap = ImmutableMap.of("irrelevant", "123");

        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, propsMap);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testHappyPath() {
        final Map<String, String> propsMap = ImmutableMap.of("irrelevant", "123",
            NUM_CPU_CORES, "2", CPU_CORE_MHZ, "5");

        final List<CommoditySoldDTO> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, propsMap);
        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertEquals(entity.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            Collections.singletonList(makeCommoditySold(CommodityType.CPU, 10)));
    }
}
