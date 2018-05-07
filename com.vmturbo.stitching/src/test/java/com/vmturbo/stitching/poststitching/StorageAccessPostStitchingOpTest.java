package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.CommoditySoldBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.TopologyEntityBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class StorageAccessPostStitchingOpTest {

    private final CommoditySoldDTO emptyCommodity = CommoditySoldBuilder.newBuilder()
        .withType(CommodityType.STORAGE_ACCESS).build();

    private EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private final DiskCapacityCalculator diskCapacityCalculator =
        mock(DiskCapacityCalculator.class);
    private final StorageAccessCapacityPostStitchingOperation diskArrayOp =
        new StorageAccessCapacityPostStitchingOperation(EntityType.DISK_ARRAY, diskCapacityCalculator);

    private UnitTestResultBuilder resultBuilder;

    private final TopologyEntityBuilder baseTe = TopologyEntityBuilder.newBuilder()
        .withProperty("common_dto.EntityDTO.DiskArrayData.diskCounts", "asdf");

    private static final float GOOD_VALUE = 987;
    private static final float BAD_VALUE_1 = 456;
    private static final float BAD_VALUE_2 = 123;


    @Before
    public void setup() {

        when(settingsMock.getEntitySetting(any(), any())).thenReturn(Optional.empty());
        when(settingsMock.getEntityUserSetting(any(), any())).thenReturn(Optional.empty());
        when(diskCapacityCalculator.calculateCapacity(any())).thenReturn(0.0);

        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void noEntities() {
        diskArrayOp.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testUserSettingOverridesAll() {

        final TopologyEntity te = baseTe.withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                .withType(CommodityType.STORAGE_ACCESS).withCapacity(BAD_VALUE_1)).build();

        when(settingsMock.getEntityUserSetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(makeNumericSetting(GOOD_VALUE)));
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(makeNumericSetting(GOOD_VALUE)));
        when(diskCapacityCalculator.calculateCapacity(any())).thenReturn((double)BAD_VALUE_2);

        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(GOOD_VALUE, te.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 1e-5);
    }

    @Test
    public void testProbeOverridesCalculatedAndDefault() {

        final TopologyEntity te = baseTe
            .withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                .withType(CommodityType.STORAGE_ACCESS).withCapacity(GOOD_VALUE)).build();

        when(settingsMock.getEntityUserSetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.empty());
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(makeNumericSetting(BAD_VALUE_1)));
        when(diskCapacityCalculator.calculateCapacity(any())).thenReturn((double)BAD_VALUE_2);

        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertTrue(resultBuilder.getChanges().isEmpty());
        assertEquals(GOOD_VALUE, te.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 1e-5);
    }

    @Test
    public void testCalculateOverridesDefaultSetting() {

        final TopologyEntity te = baseTe
            .withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                .withType(CommodityType.STORAGE_ACCESS)).build();


        when(settingsMock.getEntitySetting(any(), any()))
            .thenReturn(Optional.of(makeNumericSetting(BAD_VALUE_1)));
        when(diskCapacityCalculator.calculateCapacity(any())).thenReturn((double)GOOD_VALUE);

        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(GOOD_VALUE, te.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 1e-5);
    }

    @Test
    public void testUseDefaultIopsSettingWhenNoOtherValue() {

        final Setting mainSetting = makeNumericSetting(GOOD_VALUE);
        when(settingsMock.getEntitySetting(any(), any())).thenReturn(Optional.of(mainSetting));

        final TopologyEntity te = baseTe.withCommoditiesSold(emptyCommodity).build();

        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(GOOD_VALUE, te.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 1e-5);

    }

    @Test
    public void noSettingsAtAll() {

        final TopologyEntity te =
            TopologyEntityBuilder.newBuilder().withCommoditiesSold(emptyCommodity).build();
        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

}
