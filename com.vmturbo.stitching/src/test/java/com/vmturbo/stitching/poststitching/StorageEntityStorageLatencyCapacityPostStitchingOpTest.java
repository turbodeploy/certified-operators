package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
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
 * Unit tests for {@link StorageEntityCapacityPostStitchingOperation}.
 */
public class StorageEntityStorageLatencyCapacityPostStitchingOpTest {

    private static final double CAPACITY = 5;
    private static final CommodityType COMMODITY_TYPE = CommodityType.STORAGE_LATENCY;
    private static final TopologyEntityBuilder PROVIDER = TopologyEntityBuilder.newBuilder()
        .withEntityType(EntityType.DISK_ARRAY_VALUE)
        .withCommoditiesSold(
            CommoditySoldBuilder.newBuilder().withType(COMMODITY_TYPE).withCapacity(CAPACITY));

    private static final float STORAGE_LATENCY_SETTING_DEFAULT = 10.0f;

    private static final Setting STORAGE_LATENCY_SETTING = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.LatencyCapacity.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(STORAGE_LATENCY_SETTING_DEFAULT))
            .build();

    private final StorageEntityCapacityPostStitchingOperation op =
        new StorageEntityCapacityPostStitchingOperation(CommodityType.STORAGE_LATENCY,
                EntitySettingSpecs.LatencyCapacity);

    private final CommoditySoldDTO emptyCommodity = CommoditySoldBuilder.newBuilder()
        .withType(CommodityType.STORAGE_LATENCY).build();

    private EntityChangesBuilder<TopologyEntity> resultBuilder;
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    /**
     * Setup for tests
     */
    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    /**
     * Test if there is not entity
     */
    @Test
    public void testNoEntity() {
        op.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    /**
     * Test getting default latency setting with no providers
     */
    @Test
    public void testNoProvidersSetFromSettings() {
        final TopologyEntity te1 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_VALUE)
            .withCommoditiesSold(emptyCommodity)
            .build();

        when(settingsMock.getEntitySetting(te1, EntitySettingSpecs.LatencyCapacity))
                .thenReturn(Optional.of(STORAGE_LATENCY_SETTING));
        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);

        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(STORAGE_LATENCY_SETTING_DEFAULT,
                te1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 0);
    }

    /**
     * Test with no latency setting and no providers
     */
    @Test
    public void testNoProvidersOrSettings() {
        final TopologyEntity te1 = TopologyEntityBuilder.newBuilder()
                .withEntityType(EntityType.STORAGE_VALUE)
                .withCommoditiesSold(emptyCommodity)
                .build();

        when(settingsMock.getEntitySetting(te1, EntitySettingSpecs.LatencyCapacity))
            .thenReturn(Optional.empty());
        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().size() == 1);
    }

    /**
     * Test getting default latency setting if there are no valid providers
     */
    @Test
    public void testNoGoodProvidersSetFromSettings() {

        final TopologyEntityBuilder ineligibleProvider1 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_CONTROLLER_VALUE)
            .withCommoditiesSold(
                CommoditySoldBuilder.newBuilder()
                    .withType(CommodityType.STORAGE_ACCESS).withCapacity(CAPACITY)
            );

        final TopologyEntityBuilder ineligibleProvider2 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.DISK_ARRAY_VALUE).withCommoditiesSold(
                CommoditySoldBuilder.newBuilder()
                    .withType(CommodityType.STORAGE_LATENCY)
            );

        final TopologyEntityBuilder ineligibleProvider3 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.LOGICAL_POOL_VALUE);

        final TopologyEntity te1 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_VALUE)
            .withCommoditiesSold(emptyCommodity)
            .withProviders(ineligibleProvider1, ineligibleProvider2, ineligibleProvider3)
            .build();

        when(settingsMock.getEntitySetting(te1, EntitySettingSpecs.LatencyCapacity))
                .thenReturn(Optional.of(STORAGE_LATENCY_SETTING));
        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(STORAGE_LATENCY_SETTING_DEFAULT,
                te1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 0);
    }

    /**
     * Test settings getting over-ridden by provider capacity
     */
    @Test
    public void testOneGoodProviderOverridesSettingCapacity() {
        final TopologyEntity te1 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_VALUE)
            .withCommoditiesSold(emptyCommodity)
            .withProviders(PROVIDER).build();


        // Even though we get the setting, we should use the provider's capacity.
        when(settingsMock.getEntitySetting(te1, EntitySettingSpecs.LatencyCapacity))
                .thenReturn(Optional.of(STORAGE_LATENCY_SETTING));
        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(CAPACITY,
            te1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 0);
    }

    /**
     * Test with multiple valid providers
     */
    @Test
    public void testManyGoodProviders() {
        //if there are several viable providers, one is selected arbitrarily.

        final double arbitraryCapacity = 15;
        final List<Double> acceptableCapacities = Arrays.asList(arbitraryCapacity, CAPACITY);

        final TopologyEntityBuilder provider2 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.DISK_ARRAY_VALUE)
            .withCommoditiesSold(
                CommoditySoldBuilder.newBuilder()
                    .withType(CommodityType.STORAGE_LATENCY).withCapacity(arbitraryCapacity)
            );

        final TopologyEntity te1 = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_VALUE)
            .withCommoditiesSold(emptyCommodity)
            .withProviders(PROVIDER, provider2).build();

        op.performOperation(Stream.of(te1), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        final double resultCapacity =
            te1.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity();
        assertTrue(acceptableCapacities.contains(resultCapacity));
    }

    /**
     * Test provider capacity over-writing pre-existing capacity
     */
    @Test
    public void testProviderCapacityOverwritePreexistingCapacity() {

        final TopologyEntity te = TopologyEntityBuilder.newBuilder()
            .withEntityType(EntityType.STORAGE_VALUE)
            .withProviders(PROVIDER)
            .withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                .withCapacity(11).withType(CommodityType.STORAGE_LATENCY))
            .build();

        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(1, resultBuilder.getChanges().size());
        assertEquals(CAPACITY,
            te.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(), 0);
    }

    /**
     * Test pre-existing capacity not getting over-written by provider capacity or default setting
     */
    @Test
    public void testSettingCapacityNotOverwritePreexistingCapacity() {
        final float existingCapacity = 11;

        final TopologyEntity te = TopologyEntityBuilder.newBuilder()
                .withEntityType(EntityType.STORAGE_VALUE)
                .withCommoditiesSold(CommoditySoldBuilder.newBuilder()
                        .withCapacity(existingCapacity)
                        .withType(CommodityType.STORAGE_LATENCY))
                .build();

        when(settingsMock.getEntitySetting(te, EntitySettingSpecs.LatencyCapacity))
                .thenReturn(Optional.of(STORAGE_LATENCY_SETTING));
        op.performOperation(Stream.of(te), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(0, resultBuilder.getChanges().size());
    }
}
