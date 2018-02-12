package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class IndependentStorageAccessPostStitchingOpTest {

    private final IndependentStorageAccessPostStitchingOperation op =
        new IndependentStorageAccessPostStitchingOperation();

    private float settingCapacity = 24;
    private final Setting iopsSetting = makeNumericSetting(settingCapacity);
    private final CommoditySoldDTO emptyCommodity = makeCommoditySold(CommodityType.STORAGE_ACCESS);

    private EntityChangesBuilder<TopologyEntity> resultBuilder;
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(EntitySettingSpecs.IOPSCapacity))).thenReturn(Optional.of(iopsSetting));

    }

    @Test
    public void testNoEntity() {
        op.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityIsIneligible() {

        final TopologyEntity.Builder ineligibleProvider1 =
            makeTopologyEntityBuilder(EntityType.LOGICAL_POOL.getNumber(), Collections.emptyList(),
                Collections.emptyList());

        final TopologyEntity te1 = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity), Collections.emptyList(),
            Collections.singletonList(ineligibleProvider1));

        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

        final TopologyEntity.Builder ineligibleProvider2 =
            makeTopologyEntityBuilder(EntityType.DISK_ARRAY.getNumber(), Collections.emptyList(),
                Collections.emptyList());

        final TopologyEntity te2 = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity), Collections.emptyList(),
            Collections.singletonList(ineligibleProvider2));

        op.performOperation(Stream.of(te2), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoCommodities() {
        final TopologyEntity te = makeTopologyEntity(EntityType.STORAGE.getNumber(), Collections.emptyList());
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testWrongTypeCommodities() {
        final CommoditySoldDTO wrongType = makeCommoditySold(CommodityType.CPU_ALLOCATION);
        final TopologyEntity te = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(wrongType));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testCommoditiesWithCapacities() {
        final CommoditySoldDTO withCapacity = makeCommoditySold(CommodityType.STORAGE_ACCESS, settingCapacity);
        final TopologyEntity te = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(withCapacity));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoIopsSetting() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(EntitySettingSpecs.IOPSCapacity))).thenReturn(Optional.empty());

        final TopologyEntity te = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testHappyPath() {
        final TopologyEntity te = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), settingCapacity, .1));

    }

    @Test
    public void testMultipleEntities() {
        final TopologyEntity te1 = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity));
        when(settingsMock.getEntitySetting(eq(te1),
            eq(EntitySettingSpecs.IOPSCapacity))).thenReturn(Optional.empty());

        final TopologyEntity te2 = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity));
        final Setting setting2 = makeNumericSetting(105);
        when(settingsMock.getEntitySetting(eq(te2),
            eq(EntitySettingSpecs.IOPSCapacity))).thenReturn(Optional.of(setting2));

        final TopologyEntity te3 = makeTopologyEntity(EntityType.STORAGE.getNumber(),
            Collections.singletonList(emptyCommodity));

        op.performOperation(Stream.of(te1, te2, te3), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 2);
        te2.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), 105, .1));
        te3.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), settingCapacity, .1));

    }
}
