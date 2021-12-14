package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
 * Unit tests for {@link StorageEntityIopsOrLatencyCapacityPostStitchingOp}.
 */
@RunWith(value = Parameterized.class)
public class StorageEntityIopsOrLatencyCapacityPostStitchingOpTest {

    /**
     * Test runs for both IOPS and Latency commodities.
     *
     * @return the parameters
     */
    @Parameters(name = "({0}, {1})")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][]{{CommodityType.STORAGE_ACCESS, EntitySettingSpecs.IOPSCapacity},
                        {CommodityType.STORAGE_LATENCY, EntitySettingSpecs.LatencyCapacity}});
    }

    private final EntitySettingSpecs setting;

    private final CommodityType commodityType;

    private final StorageEntityIopsOrLatencyCapacityPostStitchingOp op;

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private EntitySettingsCollection settingsMock;

    /**
     * Initializes the test specifying the commodity used for the operation.
     *
     * @param commodityType StorageAccess or StorageLatency
     * @param setting the relevant setting
     */
    public StorageEntityIopsOrLatencyCapacityPostStitchingOpTest(CommodityType commodityType,
            EntitySettingSpecs setting) {
        this.setting = setting;
        this.commodityType = commodityType;
        this.op = new StorageEntityIopsOrLatencyCapacityPostStitchingOp(commodityType, setting);
    }

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        settingsMock = mock(EntitySettingsCollection.class);
        resultBuilder = new UnitTestResultBuilder();
    }

    /**
     * Test if there is no entity.
     */
    @Test
    public void testNoEntity() {
        op.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    /**
     * Tests that if there is no capacity set either by a setting, or on the entity, or entity's
     * providers we get the default setting value.
     */
    @Test
    public void testNoSettingsNoEntityNoProvider() {
        // ARRANGE
        TopologyEntityBuilder diskArray =
                getEntitySellingCommodityWithCapacity(null).withEntityType(
                        EntityType.DISK_ARRAY_VALUE);
        TopologyEntityBuilder logicalPool =
                getEntitySellingCommodityWithCapacity(null).withEntityType(
                        EntityType.LOGICAL_POOL_VALUE);
        TopologyEntity storage = getEntitySellingCommodityWithCapacity(null)
                .withEntityType(EntityType.STORAGE_VALUE)
                .withProviders(diskArray, logicalPool)
                .build();
        setCommoditySettingForEntity(settingsMock, storage, null);

        // ACT
        op.performOperation(Stream.of(storage), settingsMock, resultBuilder);
        applyChanges(resultBuilder, stitchingJournal);

        // ASSERT
        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(setting.getSettingSpec().getNumericSettingValueType().getDefault(),
                getCommodityCapacity(storage), 0);
    }

    /**
     * Tests that if the capacity is only set on providers, the most immediate provider is picked.
     */
    @Test
    public void testNoSettingsNoEntity() {
        // ARRANGE
        TopologyEntityBuilder diskArray =
                getEntitySellingCommodityWithCapacity(300d).withEntityType(
                        EntityType.DISK_ARRAY_VALUE);
        TopologyEntityBuilder logicalPool =
                getEntitySellingCommodityWithCapacity(400d).withEntityType(
                        EntityType.LOGICAL_POOL_VALUE);
        TopologyEntity storage = getEntitySellingCommodityWithCapacity(null)
                .withEntityType(EntityType.STORAGE_VALUE)
                .withProviders(diskArray, logicalPool)
                .build();
        setCommoditySettingForEntity(settingsMock, storage, null);

        // ACT
        op.performOperation(Stream.of(storage), settingsMock, resultBuilder);
        applyChanges(resultBuilder, stitchingJournal);

        // ASSERT
        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals("storage uses capacity from immediate provider", 400d,
                getCommodityCapacity(storage), 0);
    }

    private void setCommoditySettingForEntity(EntitySettingsCollection settingsMock,
            TopologyEntity storage, Float capacity) {
        when(settingsMock.getEntityUserSetting(storage, setting)).thenReturn(Optional
                .ofNullable(capacity)
                .map(cap -> Setting
                        .newBuilder()
                        .setSettingSpecName(setting.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(cap))
                        .build()));
    }

    private void applyChanges(EntityChangesBuilder<TopologyEntity> resultBuilder,
            IStitchingJournal<TopologyEntity> stitchingJournal) {
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));
    }

    private double getCommodityCapacity(TopologyEntity entity) {
        List<CommoditySoldDTO> commodities = entity
                .getTopologyEntityDtoBuilder()
                .getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == commodityType.getNumber())
                .collect(Collectors.toList());

        Assert.assertEquals(1, commodities.size());

        return commodities.get(0).getCapacity();
    }

    /**
     * Tests that the setting will always be applied.
     */
    @Test
    public void testSettingCapacityAlwaysApplied() {
        // ARRANGE
        TopologyEntityBuilder diskArray =
                getEntitySellingCommodityWithCapacity(100d).withEntityType(
                        EntityType.DISK_ARRAY_VALUE);
        TopologyEntityBuilder logicalPool =
                getEntitySellingCommodityWithCapacity(900d).withEntityType(
                        EntityType.LOGICAL_POOL_VALUE);
        TopologyEntity storage = getEntitySellingCommodityWithCapacity(800d)
                .withEntityType(EntityType.STORAGE_VALUE)
                .withProviders(diskArray, logicalPool)
                .build();
        setCommoditySettingForEntity(settingsMock, storage, 5000f);

        // ACT
        op.performOperation(Stream.of(storage), settingsMock, resultBuilder);
        applyChanges(resultBuilder, stitchingJournal);

        // ASSERT
        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(5000f, getCommodityCapacity(storage), 0);
    }

    private TopologyEntityBuilder getStorageSellingNothing() {
        return TopologyEntityBuilder
                .newBuilder()
                .withEntityType(EntityType.STORAGE_VALUE)
                .withCommoditiesSold(CommoditySoldBuilder.newBuilder().withType(commodityType)
                        .build());
    }

    /**
     * Tests that we get the default IOPS setting value if capacity is set only on invalid
     * providers.
     */
    @Test
    public void testNoPreexistingNoGoodProvidersNoSettingSetDefault() {

        // ARRANGE:
        // SC (sells IOPS with capacity) <-- ineligible because only DAs and LPs are
        final TopologyEntityBuilder ineligibleProvider1 =
                getEntitySellingCommodityWithCapacity(10_000d).withEntityType(
                        EntityType.STORAGE_CONTROLLER_VALUE);

        // DA (sells IOPS undefined capacity) <-- ineligible because it does not have capacity
        final TopologyEntityBuilder ineligibleProvider2 =
                getEntitySellingCommodityWithCapacity(null).withEntityType(
                        EntityType.DISK_ARRAY_VALUE);

        // LP <-- ineligible because it does not sell IOPS
        final TopologyEntityBuilder ineligibleProvider3 =
                TopologyEntityBuilder.newBuilder().withEntityType(EntityType.LOGICAL_POOL_VALUE);

        // Storage <- SC, DA, LP
        final TopologyEntity storage = getStorageSellingNothing()
                .withProviders(ineligibleProvider1, ineligibleProvider2, ineligibleProvider3)
                .build();

        setCommoditySettingForEntity(settingsMock, storage, null);

        // ACT
        op.performOperation(Stream.of(storage), settingsMock, resultBuilder);
        applyChanges(resultBuilder, stitchingJournal);

        // ASSERT
        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(setting.getSettingSpec().getNumericSettingValueType().getDefault(),
                getCommodityCapacity(storage), 0);
    }

    private TopologyEntityBuilder getEntitySellingCommodityWithCapacity(Double capacity) {
        CommoditySoldBuilder commoditySoldBuilder =
                CommoditySoldBuilder.newBuilder().withType(commodityType);
        if (capacity != null) {
            commoditySoldBuilder.withCapacity(capacity);
        }
        return TopologyEntityBuilder.newBuilder().withCommoditiesSold(commoditySoldBuilder);
    }

    /**
     * Test provider capacity is not over-writing pre-existing capacity.
     */
    @Test
    public void testPreexistingCapacityWinsOverProvider() {

        // ARRANGE
        TopologyEntityBuilder logicalPool =
                getEntitySellingCommodityWithCapacity(400d).withEntityType(
                        EntityType.LOGICAL_POOL_VALUE);
        TopologyEntity storage = getEntitySellingCommodityWithCapacity(300d)
                .withEntityType(EntityType.STORAGE_VALUE)
                .withProviders(logicalPool)
                .build();
        setCommoditySettingForEntity(settingsMock, storage, null);

        // ACT
        op.performOperation(Stream.of(storage), settingsMock, resultBuilder);
        applyChanges(resultBuilder, stitchingJournal);

        // ASSERT
        assertEquals(0, resultBuilder.getChanges().size());
    }
}
