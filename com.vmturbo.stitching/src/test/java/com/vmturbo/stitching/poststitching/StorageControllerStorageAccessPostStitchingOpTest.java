package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.StorageControllerStorageAccessPostStitchingOperation;

public class StorageControllerStorageAccessPostStitchingOpTest {


    private final StorageControllerStorageAccessPostStitchingOperation op =
        new StorageControllerStorageAccessPostStitchingOperation();

    private static final String DISK_PROP_KEY = "common_dto.EntityDTO.DiskArrayData.diskCounts";
    private static final String DISK_7200_KEY = "NUM_7200_DISKS";
    private static final String DISK_10K_KEY = "NUM_10K_DISKS";



    private final CommoditySoldDTO emptyCommodity = makeCommoditySold(CommodityType.STORAGE_ACCESS);

    private EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);
    private UnitTestResultBuilder resultBuilder;

    @Before
    public void setup() {

        when(settingsMock.getEntitySetting(any(), any())).thenReturn(Optional.empty());

        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void noEntities() {
        op.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void noUpdateableCommodities() {

        final CommoditySoldDTO wrongTypeCommodity = makeCommoditySold(CommodityType.CPU_ALLOCATION);
        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(CommodityType.STORAGE_ACCESS, 49);
        final TopologyEntity te1 = makeTopologyEntity(Collections.emptyList());
        final TopologyEntity te2 = makeTopologyEntity(Collections.singletonList(wrongTypeCommodity));
        final TopologyEntity te3 = makeTopologyEntity(Collections.singletonList(commodityWithCapacity));

        op.performOperation(Stream.of(te1, te2, te3), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testOneDiskType() {
        final Setting setting7200 = makeNumericSetting(111);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity7200)))
            .thenReturn(Optional.of(setting7200));

        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(DISK_7200_KEY, 4));

        final TopologyEntity te = makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 444, .1));
    }

    @Test
    public void testMultipleDiskTypes() {
        final Setting setting7200 = makeNumericSetting(111);
        final Setting setting10k = makeNumericSetting(222);

        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity7200)))
            .thenReturn(Optional.of(setting7200));
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity10k)))
            .thenReturn(Optional.of(setting10k));


        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(DISK_7200_KEY, 4, DISK_10K_KEY, 2));

        final TopologyEntity te = makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 888, .1));
    }

    @Test
    public void testZeroDisksOfSomeType() {
        final Setting setting7200 = makeNumericSetting(111);
        final Setting setting10k = makeNumericSetting(222);

        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity7200)))
            .thenReturn(Optional.of(setting7200));
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity10k)))
            .thenReturn(Optional.of(setting10k));


        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(DISK_7200_KEY, 0, DISK_10K_KEY, 3));

        final TopologyEntity te =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 666, .1));
    }

    @Test
    public void testFlags() {
        final Setting setting7200 = makeNumericSetting(111);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity7200)))
            .thenReturn(Optional.of(setting7200));

        final ImmutableMap<String, String> propMap1 = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(true, true, DISK_7200_KEY, 4));
        final TopologyEntity te1 =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap1);

        final ImmutableMap<String, String> propMap2 = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(false, false, DISK_7200_KEY, 4));
        final TopologyEntity te2 =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap2);

        final ImmutableMap<String, String> propMap3 = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(false, true, DISK_7200_KEY, 4));
        final TopologyEntity te3 =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap3);

        op.performOperation(Stream.of(te1, te2, te3), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 3);
        te1.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 666, .1));
        te2.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 444, .1));
        te3.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 577.2, .1));
    }

    @Test
    public void testSettingButNoProperty() {
        final Setting setting7200 = makeNumericSetting(111);
        final Setting setting10k = makeNumericSetting(222);

        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity7200)))
            .thenReturn(Optional.of(setting7200));
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity10k)))
            .thenReturn(Optional.of(setting10k));

        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(false, false, DISK_10K_KEY, 3));
        final TopologyEntity te =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);

        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 666, .1));
    }

    @Test
    public void testPropertyButNoSetting() {
        final Setting setting10k = makeNumericSetting(222);

        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.DiskCapacity10k)))
            .thenReturn(Optional.of(setting10k));

        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            makePropertyString(DISK_7200_KEY, 4, DISK_10K_KEY, 3));
        final TopologyEntity te =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);

        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 666, .1));
    }

    @Test
    public void testUseMainIopsSettingWhenNoProperties() {

        final Setting decoyDiskSetting = makeNumericSetting(987);
        when(settingsMock.getEntitySetting(any(), any()))
            .thenReturn(Optional.of(decoyDiskSetting));

        final Setting mainSetting = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(mainSetting));

        final TopologyEntity te = makeTopologyEntity(Collections.singletonList(emptyCommodity));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 123, .1));
    }

    @Test
    public void testUseMainIopsSettingWhenMalformedProperties() {

        final Setting decoyDiskSetting = makeNumericSetting(987);
        when(settingsMock.getEntitySetting(any(), any()))
            .thenReturn(Optional.of(decoyDiskSetting));

        final Setting mainSetting = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(mainSetting));

        final ImmutableMap<String, String> propMap = ImmutableMap.of(DISK_PROP_KEY,
            "asdf123jkl;456, and something else, and then finally " + DISK_7200_KEY + ": 456");

        final TopologyEntity te =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 123, .1));
    }

    @Test
    public void noSettingsAtAll() {
        final ImmutableMap<String, String> propMap =
            ImmutableMap.of(DISK_PROP_KEY, makePropertyString(DISK_7200_KEY, 24));

        final TopologyEntity te =
            makeTopologyEntity(Collections.singletonList(emptyCommodity), propMap);
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }


    private String makePropertyString(@Nonnull final String diskTypeKey1, final int numDisks1,
                                      @Nonnull final String diskTypeKey2, final int numDisks2) {
        return makePropertyString(diskTypeKey1, numDisks1) +
            makePropertyString(diskTypeKey2, numDisks2);

    }

    private String makePropertyString(final boolean hybridFlag, final boolean flashFlag,
                                      @Nonnull final String diskTypeKey, final int numDisks) {
        return "hybrid: " + hybridFlag + "\nflashAvailable: " + flashFlag + "\n" +
            makePropertyString(diskTypeKey, numDisks);
    }

    private String makePropertyString(@Nonnull final String diskTypeKey, final int numDisks) {
        return "disks {\n  numDiskName: \"" + diskTypeKey + "\"\n  numDisks: " + numDisks + "\n}\n";
    }


}
