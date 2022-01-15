package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.CommoditySoldBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.TopologyEntityBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for VolumeEntityAccessCapacityPostStitchingOperation.
 */
public class VolumeEntityAccessCapacityPostStitchingOperationTest {
    private static final double CAPACITY_PROVIDER = 250D;
    private static final double CAPACITY_ENTITY = 150D;
    private static final CommodityType COMMODITY_TYPE = CommodityType.STORAGE_ACCESS;
    private static final TopologyEntityBuilder PROVIDER =
                    TopologyEntityBuilder.newBuilder().withEntityType(EntityType.STORAGE_VALUE)
                                    .withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                                                    .withType(COMMODITY_TYPE)
                                                    .withCapacity(CAPACITY_PROVIDER));
    private static final float CAPACITY_SETTING = 157.0F;
    private static final double DELTA = 0.0001D;

    private static final Setting IOPS_SETTING = Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.IOPSCapacity.getSettingName())
                    .setNumericSettingValue(
                                    NumericSettingValue.newBuilder().setValue(CAPACITY_SETTING))
                    .build();

    private static final CommoditySoldDTO COMMODITY_NO_CAP = CommoditySoldBuilder.newBuilder()
                    .withType(CommodityType.STORAGE_ACCESS).build();
    private static final CommoditySoldDTO COMMODITY_CAP = CommoditySoldBuilder.newBuilder()
                    .withType(CommodityType.STORAGE_ACCESS).withCapacity(CAPACITY_ENTITY).build();

    private static final IStitchingJournal<TopologyEntity> stitchingJournal =
                    (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    /**
     * Test that capacity is taken from settings if provider and entity does not specify one.
     */
    @Test
    public void testNoProvidersSetFromSettings() {
        VolumeEntityAccessCapacityPostStitchingOperation op =
                        new VolumeEntityAccessCapacityPostStitchingOperation();
        EntityChangesBuilder<TopologyEntity> resultBuilder = new UnitTestResultBuilder();

        TopologyEntity volume = TopologyEntityBuilder.newBuilder()
                        .withEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .withCommoditiesSold(COMMODITY_NO_CAP).build();
        when(settingsMock.getEntitySetting(volume, EntitySettingSpecs.IOPSCapacity))
                        .thenReturn(Optional.of(IOPS_SETTING));
        op.performOperation(Stream.of(volume), settingsMock, resultBuilder);

        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(CAPACITY_SETTING,
                        volume.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(),
                        DELTA);
    }

    /**
     * Test that capacity from entity is not overwritten.
     */
    @Test
    public void testNoOverride() {
        VolumeEntityAccessCapacityPostStitchingOperation op =
                        new VolumeEntityAccessCapacityPostStitchingOperation();
        EntityChangesBuilder<TopologyEntity> resultBuilder = new UnitTestResultBuilder();

        TopologyEntity volume = TopologyEntityBuilder.newBuilder()
                        .withEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .withCommoditiesSold(COMMODITY_CAP).build();
        when(settingsMock.getEntitySetting(volume, EntitySettingSpecs.IOPSCapacity))
                        .thenReturn(Optional.of(IOPS_SETTING));
        op.performOperation(Stream.of(volume), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(0, resultBuilder.getChanges().size());
    }

}
