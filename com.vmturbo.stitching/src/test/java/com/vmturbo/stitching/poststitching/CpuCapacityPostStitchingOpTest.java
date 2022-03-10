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

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class CpuCapacityPostStitchingOpTest {

    private final CpuCapacityPostStitchingOperation operation = new CpuCapacityPostStitchingOperation();
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private static final TypeSpecificInfoView PM_INFO = new TypeSpecificInfoImpl()
        .setPhysicalMachine(new PhysicalMachineInfoImpl()
            .setCpuCoreMhz(5)
            .setNumCpus(2));

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

        final TopologyEntity entity = makeTopologyEntity(Collections.emptyList(), PM_INFO);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoCpuCommodity() {
        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.BALLOONING));
        final TopologyEntity entity = makeTopologyEntity(commodities, PM_INFO);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testCpuCommodityHasCapacity() {

        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU, 255));
        final TopologyEntity entity = makeTopologyEntity(commodities, PM_INFO);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksNumCores() {
        final TypeSpecificInfoImpl noCpuCountBldr = PM_INFO.copy();
        noCpuCountBldr.getOrCreatePhysicalMachine().clearNumCpus();

        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, noCpuCountBldr);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksCpuMhz() {
        final TypeSpecificInfoImpl noCoreMhzBldr = PM_INFO.copy();
        noCoreMhzBldr.getOrCreatePhysicalMachine().clearCpuCoreMhz();

        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, noCoreMhzBldr);
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityLacksCoresAndMhz() {
        final Map<String, String> propsMap = ImmutableMap.of("irrelevant", "123");

        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, TypeSpecificInfoView.getDefaultInstance());
        operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testHappyPath() {
        final List<CommoditySoldView> commodities = Collections.singletonList(makeCommoditySold(CommodityType.CPU));
        final TopologyEntity entity = makeTopologyEntity(commodities, PM_INFO);
        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(entity), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertEquals(entity.getTopologyEntityImpl().getCommoditySoldListList(),
            Collections.singletonList(makeCommoditySold(CommodityType.CPU, 10)));
    }
}
