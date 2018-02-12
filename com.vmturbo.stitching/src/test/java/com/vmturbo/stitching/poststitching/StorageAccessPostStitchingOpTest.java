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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.DiskArrayStorageAccessPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.LogicalPoolStorageAccessPostStitchingOperation;

public class StorageAccessPostStitchingOpTest {
    /* this test focuses on functionality unique to Logical Pools and Disk Arrays, since the
       Storage Controller test is pretty thorough and doesn't really depart from the base class. */

    private final DiskArrayStorageAccessPostStitchingOperation diskArrayOp =
        new DiskArrayStorageAccessPostStitchingOperation();

    private final LogicalPoolStorageAccessPostStitchingOperation logicalPoolOp =
        new LogicalPoolStorageAccessPostStitchingOperation();

    private final CommoditySoldDTO emptyCommodity = makeCommoditySold(CommodityType.STORAGE_ACCESS);

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);
    private UnitTestResultBuilder resultBuilder;

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        when(settingsMock.getEntitySetting(any(), any())).thenReturn(Optional.empty());
    }

    @Test
    public void testLogicalPoolWithWrongProviderIsFilteredOut() {
        final Setting validIops = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(validIops));

        final TopologyEntity teNoProvider =
            makeTopologyEntity(Collections.singletonList(emptyCommodity));

        final TopologyEntity.Builder wrongProviderBuilder =
            makeTopologyEntityBuilder(EntityType.VIRTUAL_MACHINE.getValue(),
                Collections.emptyList(), Collections.emptyList());
        final TopologyEntity.Builder teWrongProviderBuilder =
            makeTopologyEntityBuilder(EntityType.LOGICAL_POOL.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        teWrongProviderBuilder.addProvider(wrongProviderBuilder);
        final TopologyEntity teWrongProvider = teWrongProviderBuilder.build();

        logicalPoolOp.performOperation(Stream.of(teNoProvider, teWrongProvider), settingsMock,
            resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testLogicalPoolWithRightProvider() {
        final Setting validIops = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(validIops));

        final TopologyEntity.Builder providerBuilder =
            makeTopologyEntityBuilder(EntityType.DISK_ARRAY.getValue(), Collections.emptyList(),
                Collections.emptyList());
        final TopologyEntity.Builder teBuilder =
            makeTopologyEntityBuilder(EntityType.LOGICAL_POOL.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        teBuilder.addProvider(providerBuilder);
        final TopologyEntity te = teBuilder.build();

        logicalPoolOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), 123, .1));
    }

    @Test
    public void testStoragePropagationLogicalPool() {

        final Setting validIops = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(validIops));

        final TopologyEntity.Builder providerBuilder =
            makeTopologyEntityBuilder(EntityType.DISK_ARRAY.getValue(), Collections.emptyList(),
                Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder1 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder2 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(makeCommoditySold(CommodityType.CPU_ALLOCATION)),
                Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder3 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(makeCommoditySold(CommodityType.STORAGE_ACCESS, 456)),
                Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder4 =
            makeTopologyEntityBuilder(EntityType.DISK_ARRAY.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        final TopologyEntity.Builder teBuilder =
            makeTopologyEntityBuilder(EntityType.LOGICAL_POOL.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        teBuilder.addProvider(providerBuilder);
        teBuilder.addConsumer(consumerBuilder1);
        teBuilder.addConsumer(consumerBuilder2);
        teBuilder.addConsumer(consumerBuilder3);
        teBuilder.addConsumer(consumerBuilder4);

        final TopologyEntity te = teBuilder.build();

        logicalPoolOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        final List<TopologyEntity> expectedUnchangedConsumers = Arrays.asList(consumerBuilder2.build(),
            consumerBuilder3.build(), consumerBuilder4.build());

        assertEquals(resultBuilder.getChanges().size(), 2);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), 123, .1));
        te.getConsumers().forEach(consumer ->
            assertTrue(expectedUnchangedConsumers.contains(consumer) ||
                consumer.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .allMatch(commodity -> commodity.getCapacity() == 123)));

    }

    @Test
    public void testStoragePropagationDiskArray() {

        final Setting validIops = makeNumericSetting(123);
        when(settingsMock.getEntitySetting(any(), eq(EntitySettingSpecs.IOPSCapacity)))
            .thenReturn(Optional.of(validIops));

        final TopologyEntity.Builder consumerBuilder1 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder2 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(makeCommoditySold(CommodityType.CPU_ALLOCATION)),
                Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder3 =
            makeTopologyEntityBuilder(EntityType.STORAGE.getValue(),
                Collections.singletonList(makeCommoditySold(CommodityType.STORAGE_ACCESS, 456)),
                Collections.emptyList());
        final TopologyEntity.Builder consumerBuilder4 =
            makeTopologyEntityBuilder(EntityType.LOGICAL_POOL.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        final TopologyEntity.Builder teBuilder =
            makeTopologyEntityBuilder(EntityType.DISK_ARRAY.getValue(),
                Collections.singletonList(emptyCommodity), Collections.emptyList());
        teBuilder.addConsumer(consumerBuilder1);
        teBuilder.addConsumer(consumerBuilder2);
        teBuilder.addConsumer(consumerBuilder3);
        teBuilder.addConsumer(consumerBuilder4);

        final TopologyEntity te = teBuilder.build();

        diskArrayOp.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(TopologicalChange::applyChange);

        final List<TopologyEntity> expectedUnchangedConsumers = Arrays.asList(consumerBuilder2.build(),
            consumerBuilder3.build(), consumerBuilder4.build());

        assertEquals(resultBuilder.getChanges().size(), 2);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), 123, .1));
        te.getConsumers().forEach(consumer ->
            assertTrue(expectedUnchangedConsumers.contains(consumer) ||
                consumer.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .allMatch(commodity -> commodity.getCapacity() == 123)));

    }

}
